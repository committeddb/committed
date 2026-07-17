package mysql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/transform"
)

// The CDC path decodes a binlog string/ENUM/SET value as the RAW bytes stored in
// the column's own character set (go-mysql's decodeString does no transcoding),
// whereas the snapshot path reads the same value already transcoded to UTF-8 (the
// driver connects with its default utf8mb4 session, so the server transcodes
// result strings). For any non-UTF-8 column charset the two paths therefore
// diverge — e.g. a latin1 "café" is snapshot "café" but CDC "caf�" (the raw
// 0xE9 is invalid UTF-8, so json.Marshal replaces it), which is silent corruption
// AND a composite-key mismatch (the CDC upsert can't find the snapshot's row).
//
// To make the CDC path agree with the (correct) snapshot path, committed
// transcodes each non-UTF-8 column value to UTF-8 using the column charset the
// binlog itself carries (binlog_row_metadata=FULL, Preflight-required; exposed as
// per-column collation ids via TableMapEvent.CollationMap / EnumSetCollationMap).
// The collation-id -> charset-name mapping is read once from the source's own
// information_schema.COLLATIONS (authoritative and version-accurate — the fork's
// stripped mysql package carries no collation table), then paired with the
// charset -> Go-decoder table below.

// charsetEncodings maps a MySQL character-set NAME to the Go decoder that turns
// that charset's bytes into UTF-8. utf8/utf8mb3/utf8mb4/ascii/binary are absent
// on purpose — they need no transcoding (already UTF-8, 7-bit, or opaque bytes)
// and are handled by passthroughCharsets. A charset that is in NEITHER table is
// treated as unsupported and errors loudly at decode rather than silently
// emitting U+FFFD (see resolveCharsetPlans).
//
// Note MySQL's "latin1" is Windows-1252, not ISO-8859-1 (it populates the
// 0x80–0x9F range), so it maps to charmap.Windows1252. Single-byte charmap
// decoders are total — every byte maps to a rune, so latin1/greek/cp125x are
// exact and never error or substitute. Multibyte charsets (gbk/big5/sjis/…) are
// best-effort: a well-formed sequence is exact, while a malformed one follows
// x/text (usually a U+FFFD substitution, occasionally a hard error); a hard error
// skips that row rather than emitting it (see (*MySQLEventHandler).rowEntity).
// The loud "never silently corrupt" guarantee is for an UNSUPPORTED charset,
// which errors the whole decode up front (see resolveCharsetPlans /
// unsupportedCharsetError) rather than guessing.
var charsetEncodings = map[string]encoding.Encoding{
	"latin1":   charmap.Windows1252,
	"latin2":   charmap.ISO8859_2,
	"latin5":   charmap.ISO8859_9,
	"latin7":   charmap.ISO8859_13,
	"cp1250":   charmap.Windows1250,
	"cp1251":   charmap.Windows1251,
	"cp1256":   charmap.Windows1256,
	"cp1257":   charmap.Windows1257,
	"cp850":    charmap.CodePage850,
	"cp852":    charmap.CodePage852,
	"cp866":    charmap.CodePage866,
	"greek":    charmap.ISO8859_7,
	"hebrew":   charmap.ISO8859_8,
	"koi8r":    charmap.KOI8R,
	"koi8u":    charmap.KOI8U,
	"macroman": charmap.Macintosh,
	"tis620":   charmap.Windows874,
	"cp932":    japanese.ShiftJIS,
	"sjis":     japanese.ShiftJIS,
	"ujis":     japanese.EUCJP,
	"eucjpms":  japanese.EUCJP,
	"euckr":    korean.EUCKR,
	"gbk":      simplifiedchinese.GBK,
	"gb2312":   simplifiedchinese.GBK,
	"gb18030":  simplifiedchinese.GB18030,
	"big5":     traditionalchinese.Big5,
}

// binaryCharset is the charset a BLOB/BINARY/VARBINARY column reports (MySQL's
// binary collation). It marks the column CatBinary (base64) rather than text.
const binaryCharset = "binary"

// passthroughCharsets need no transcoding: already UTF-8 (utf8/utf8mb3/utf8mb4),
// 7-bit (ascii), or opaque bytes (binary — a BLOB/VARBINARY column, which must
// NOT be reinterpreted as text). A character column reported under one of these
// is left byte-for-byte as go-mysql decoded it, matching the snapshot.
var passthroughCharsets = map[string]bool{
	"utf8mb4": true,
	"utf8mb3": true,
	"utf8":    true,
	"ascii":   true,
	"binary":  true,
}

// charsetPlan is the resolved transcoding decision for one MySQL collation id:
// enc == nil means "already UTF-8, pass through"; enc != nil means "transcode the
// bytes through enc"; supported == false means the column's charset has no Go
// decoder and any value under it must fail the decode rather than corrupt.
type charsetPlan struct {
	enc       encoding.Encoding // nil => passthrough (no transcoding needed)
	charset   string            // charset name, for error messages
	supported bool              // false => unsupported charset, error on use
}

// resolveCharsetPlans reads the source's collation catalog
// (information_schema.COLLATIONS: collation id -> charset name) and turns it into
// a collation-id -> charsetPlan map. It is read from the source itself so the
// mapping is exactly what that server version uses (collation ids are immutable
// in MySQL, so this is stable for the life of the stream). Read-only; the caller
// supplies a short-lived connection.
func resolveCharsetPlans(ctx context.Context, db *gosql.DB) (map[uint64]charsetPlan, error) {
	rows, err := db.QueryContext(ctx, "SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATIONS")
	if err != nil {
		return nil, fmt.Errorf("read source collation catalog: %w", err)
	}
	defer func() { _ = rows.Close() }()

	plans := make(map[uint64]charsetPlan)
	for rows.Next() {
		var id uint64
		var charset string
		if err := rows.Scan(&id, &charset); err != nil {
			return nil, fmt.Errorf("scan collation catalog row: %w", err)
		}
		plans[id] = planForCharset(strings.ToLower(charset))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate collation catalog: %w", err)
	}
	return plans, nil
}

// planForCharset classifies a charset name into a passthrough, a transcode, or an
// unsupported plan.
func planForCharset(charset string) charsetPlan {
	switch {
	case passthroughCharsets[charset]:
		return charsetPlan{charset: charset, supported: true}
	case charsetEncodings[charset] != nil:
		return charsetPlan{enc: charsetEncodings[charset], charset: charset, supported: true}
	default:
		return charsetPlan{charset: charset, supported: false}
	}
}

// planFor returns the transcoding plan for a column's collation id. A collation
// id of 0, or one absent from the catalog (a non-character column carries no
// collation), is a passthrough — there is nothing to transcode. A real character
// column's collation is always present (the catalog and the binlog come from the
// same server), so an unsupported charset surfaces as plan.supported == false.
func planFor(plans map[uint64]charsetPlan, collationID uint64) charsetPlan {
	if collationID == 0 {
		return charsetPlan{supported: true}
	}
	if p, ok := plans[collationID]; ok {
		return p
	}
	return charsetPlan{supported: true}
}

// transcodeToUTF8 converts b from enc's charset to UTF-8. A nil enc is a
// passthrough (b is returned unchanged). It is the single seam both the value
// path (character columns) and the label path (ENUM/SET members) go through.
func transcodeToUTF8(enc encoding.Encoding, b []byte) ([]byte, error) {
	if enc == nil {
		return b, nil
	}
	out, _, err := transform.Bytes(enc.NewDecoder(), b)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// transcodeCell transcodes one binlog character-column value through enc,
// preserving its Go form: go-mysql decodes a VARCHAR/CHAR to a string and a
// TEXT/BLOB to []byte, so both are handled and returned as the same type. ok is
// false when the value is neither (a NULL or a non-text value that carries no
// bytes to transcode), leaving the caller's original value untouched. enc must be
// non-nil (the caller checks col.transcode first).
func transcodeCell(enc encoding.Encoding, cell any) (any, bool, error) {
	switch v := cell.(type) {
	case string:
		out, err := transcodeToUTF8(enc, []byte(v))
		if err != nil {
			return nil, false, err
		}
		return string(out), true, nil
	case []byte:
		out, err := transcodeToUTF8(enc, v)
		if err != nil {
			return nil, false, err
		}
		return out, true, nil
	default:
		return nil, false, nil
	}
}
