package backup

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/committeddb/committed/internal/version"
)

// Extracted is one archive entry Extract kept, verified against the manifest.
type Extracted struct {
	Name string // archive entry name (data-dir-relative, forward-slash)
	Data []byte
}

// Extract streams a backup archive once and returns the entries keep selected,
// each verified against the trailing manifest (size and SHA-256) — the same
// integrity bar Restore holds every staged file to, so a bit-rotted or
// truncated archive entry is refused rather than handed to a caller that
// would write it into a node's logs. keep maps an entry name to a key; when
// several entries map to one key, the LAST one in archive order wins and the
// earlier ones are dropped as they are superseded, so a caller that wants
// "the greatest segment at or below N" holds one segment in memory, not the
// log's history. An archive with no manifest, an unsupported format, or a
// feature level above this binary's is refused whole.
func Extract(r io.Reader, keep func(name string) (key string, ok bool)) (map[string]Extracted, *Manifest, error) {
	tr := tar.NewReader(r)
	kept := map[string]Extracted{}
	var manifest *Manifest
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("extract: read archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if hdr.Name == ManifestName {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, nil, fmt.Errorf("extract: read manifest: %w", err)
			}
			m := &Manifest{}
			if err := json.Unmarshal(data, m); err != nil {
				return nil, nil, fmt.Errorf("extract: parse manifest: %w", err)
			}
			if m.FormatVersion != FormatVersion {
				return nil, nil, fmt.Errorf("extract: unsupported backup format version %d (this binary supports %d)", m.FormatVersion, FormatVersion)
			}
			if m.FeatureLevel > version.FeatureLevel {
				return nil, nil, fmt.Errorf("extract: backup was produced at feature level %d but this binary supports only %d", m.FeatureLevel, version.FeatureLevel)
			}
			manifest = m
			continue
		}
		key, ok := keep(hdr.Name)
		if !ok {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, nil, fmt.Errorf("extract: read %q: %w", hdr.Name, err)
		}
		kept[key] = Extracted{Name: hdr.Name, Data: data}
	}
	if manifest == nil {
		return nil, nil, fmt.Errorf("extract: archive has no %s — not a committed backup, or truncated before its manifest", ManifestName)
	}
	listed := make(map[string]FileEntry, len(manifest.Files))
	for _, f := range manifest.Files {
		listed[f.Path] = f
	}
	for key, x := range kept {
		want, ok := listed[x.Name]
		if !ok {
			return nil, nil, fmt.Errorf("extract: archive entry %q is not listed in the manifest — injected or foreign entry, refused", x.Name)
		}
		sum := sha256.Sum256(x.Data)
		if int64(len(x.Data)) != want.Size || hex.EncodeToString(sum[:]) != want.SHA256 {
			return nil, nil, fmt.Errorf("extract: archive entry %q does not match its manifest record (size %d vs %d) — the backup itself is damaged, refused", x.Name, len(x.Data), want.Size)
		}
		kept[key] = x
	}
	return kept, manifest, nil
}
