package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"golang.org/x/text/encoding"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// columnInfo is committed's per-column metadata for the binlog decode path — the
// parts the positional row image does not carry (name, JSON category, the
// ENUM/SET member lists needed to resolve the binlog's numeric encoding to
// labels, and the transcoder for a non-UTF-8 character column). enumValues is
// non-nil only for an ENUM column, setValues only for a SET column.
type columnInfo struct {
	name       string           // lower-cased column name
	cat        sql.JSONCategory // how the column's value renders as JSON
	enumValues []string         // ENUM member labels in definition order; nil otherwise
	setValues  []string         // SET member labels in definition order; nil otherwise
	// transcode converts this character column's raw binlog bytes to UTF-8 so the
	// CDC render matches the snapshot's; nil when the column is already UTF-8/
	// ascii/binary (passthrough) or is not a character column. ENUM/SET labels are
	// transcoded eagerly into enumValues/setValues instead, so this stays nil for
	// them.
	transcode encoding.Encoding
}

// tableSchema is a table's columns in ordinal (binlog row image) order.
type tableSchema struct {
	cols []columnInfo
}

// columnsFromTableMap builds the decode metadata from the binlog TableMapEvent —
// the schema as it existed when the row image was WRITTEN — rather than from live
// information_schema. This is what keeps an old-image row (one still replaying
// while the source has already run an ALTER) joined to the columns it was written
// under, not the post-ALTER columns: reading live information_schema silently
// mis-assigns values across a schema change (ADD COLUMN ... AFTER, DROP, rename).
//
// It requires binlog_row_metadata=FULL (MySQL 8.0.1+): without it the event
// carries no column names or ENUM/SET labels, so the function errors and the
// caller surfaces "enable FULL" rather than decoding against a schema it can't
// see. Preflight enforces FULL, so this error is defence in depth.
//
// charsetPlans is the source's collation catalog (see resolveCharsetPlans): it
// resolves each character column's collation id to a UTF-8 transcoder so the CDC
// render matches the snapshot's for a non-utf8mb4 charset. A column under a
// charset committed has no decoder for errors here rather than emitting corrupt
// (U+FFFD) bytes. A nil map (tests that don't exercise charsets) leaves every
// column a passthrough.
func columnsFromTableMap(tm *replication.TableMapEvent, charsetPlans map[uint64]charsetPlan) (*tableSchema, error) {
	// len(ColumnType) == ColumnCount (go-mysql slices it to that width) and is an
	// int already — no uint64→int conversion for gosec to flag.
	n := len(tm.ColumnType)

	// Column names are populated iff binlog_row_metadata=FULL — that's the FULL
	// check (there is no dedicated flag). Under FULL there is exactly one name per
	// column; otherwise the slice is empty.
	names := tm.ColumnNameString()
	if len(names) != n {
		return nil, fmt.Errorf(
			"binlog table map for %s.%s carries %d column names for %d columns; set binlog_row_metadata=FULL",
			tm.Schema, tm.Table, len(names), n)
	}

	// go-mysql pre-aligns ENUM/SET labels to the GLOBAL column index (resolving
	// ENUM/SET that the wire disguises as MYSQL_TYPE_STRING), so committed needs
	// no enum-column counter. Both maps are nil when the table has no such column;
	// indexing a nil map is safe.
	enums := tm.EnumStrValueMap()
	sets := tm.SetStrValueMap()

	// Per-column collation ids (global column index -> collation id): CollationMap
	// covers character columns (CHAR/VARCHAR/TEXT/BLOB — BLOB reports the binary
	// collation and stays a passthrough), EnumSetCollationMap covers ENUM/SET. Nil
	// when the table has no such column; indexing a nil map is safe.
	charCollations := tm.CollationMap()
	enumSetCollations := tm.EnumSetCollationMap()

	cols := make([]columnInfo, n)
	for i := 0; i < n; i++ {
		ci := columnInfo{name: strings.ToLower(names[i])}

		// A character column's value is transcoded lazily per row (its bytes live
		// in the row image); an ENUM/SET column's labels are transcoded once here
		// (they live in the table map, not the row). A binary column (BLOB/BINARY/
		// VARBINARY) reports the binary collation, whose plan charset is "binary".
		plan := planFor(charsetPlans, charCollations[i])
		if !plan.supported {
			return nil, unsupportedCharsetError(tm, names[i], plan.charset)
		}
		ci.transcode = plan.enc

		// JSON is never disguised (always type 0xf5) and go-mysql has no exported
		// IsJSONColumn, so a raw byte check is correct here. IsNumericColumn
		// resolves the real type and matches mysqlCategoryForTypeName's numeric set
		// exactly. BLOB/BINARY/VARBINARY are binary (base64, matching the snapshot's
		// CatBinary); ENUM, SET, BIT, dates, text, geometry fall to text.
		switch {
		case tm.ColumnType[i] == mysql.MYSQL_TYPE_JSON:
			ci.cat = sql.CatJSON
		case tm.IsNumericColumn(i):
			ci.cat = sql.CatNumber
		case plan.charset == binaryCharset:
			ci.cat = sql.CatBinary
		default:
			ci.cat = sql.CatText
		}

		if labels, ok := enums[i]; ok {
			transcoded, err := transcodeLabels(tm, names[i], charsetPlans, enumSetCollations[i], labels)
			if err != nil {
				return nil, err
			}
			ci.enumValues = transcoded
		}
		if labels, ok := sets[i]; ok {
			transcoded, err := transcodeLabels(tm, names[i], charsetPlans, enumSetCollations[i], labels)
			if err != nil {
				return nil, err
			}
			ci.setValues = transcoded
		}
		cols[i] = ci
	}
	return &tableSchema{cols: cols}, nil
}

// transcodeLabels converts an ENUM/SET column's member labels from its binlog
// charset to UTF-8 so a non-ASCII label (e.g. a latin1 'café') matches the
// snapshot path. An unsupported charset errors; a decoder that rejects a label's
// bytes errors too (labels are schema, not a single skippable row).
func transcodeLabels(tm *replication.TableMapEvent, column string, charsetPlans map[uint64]charsetPlan, collationID uint64, labels []string) ([]string, error) {
	plan := planFor(charsetPlans, collationID)
	if !plan.supported {
		return nil, unsupportedCharsetError(tm, column, plan.charset)
	}
	if plan.enc == nil {
		return labels, nil // already UTF-8/ascii — nothing to do
	}
	out := make([]string, len(labels))
	for i, label := range labels {
		b, err := transcodeToUTF8(plan.enc, []byte(label))
		if err != nil {
			return nil, fmt.Errorf(
				"transcode ENUM/SET label %q of %s.%s from %s: %w",
				label, tm.Schema, tm.Table, plan.charset, err)
		}
		out[i] = string(b)
	}
	return out, nil
}

// unsupportedCharsetError reports a character column whose charset committed
// cannot transcode to UTF-8 — failing loudly instead of emitting silently
// corrupt (U+FFFD) data (the direction chosen for this guard).
func unsupportedCharsetError(tm *replication.TableMapEvent, column, charset string) error {
	return fmt.Errorf(
		"column %s.%s.%s uses character set %q, which committed cannot transcode to UTF-8: "+
			"snapshot and CDC would diverge and the CDC value would be corrupt. Convert the column to utf8mb4",
		tm.Schema, tm.Table, column, charset)
}
