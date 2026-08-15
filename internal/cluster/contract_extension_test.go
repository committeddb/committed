package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJSONShapeSignature pins the tripwire's shape identity: values never
// enter the signature (two payloads with the same paths+types share a
// fingerprint), any path or type change alters it, and array elements
// contribute a type union under "path[]".
func TestJSONShapeSignature(t *testing.T) {
	shape, fp1, err := JSONShapeSignature([]byte(`{"caption":"a","tags":["x","y"],"meta":{"size":3}}`))
	require.NoError(t, err)
	require.Equal(t, []string{
		"$.caption:string",
		"$.meta.size:number",
		"$.tags[]:string",
	}, shape)

	// Same shape, different values (and key order) → same fingerprint.
	_, fp2, err := JSONShapeSignature([]byte(`{"tags":["z"],"caption":"b","meta":{"size":99}}`))
	require.NoError(t, err)
	require.Equal(t, fp1, fp2, "the fingerprint is a shape identity, not a row identity")

	// An added path is a different shape.
	_, fp3, err := JSONShapeSignature([]byte(`{"caption":"a","tags":["x"],"meta":{"size":3},"ai_labels":{}}`))
	require.NoError(t, err)
	require.NotEqual(t, fp1, fp3)

	// A changed type at one path is a different shape.
	_, fp4, err := JSONShapeSignature([]byte(`{"caption":7,"tags":["x"],"meta":{"size":3}}`))
	require.NoError(t, err)
	require.NotEqual(t, fp1, fp4)

	// Mixed-type arrays record the union; empty containers record their
	// container type; null is a type.
	shape, _, err = JSONShapeSignature([]byte(`{"a":[1,"s"],"b":{},"c":[],"d":null,"e":true}`))
	require.NoError(t, err)
	require.Equal(t, []string{
		"$.a[]:number",
		"$.a[]:string",
		"$.b:object",
		"$.c:array",
		"$.d:null",
		"$.e:bool",
	}, shape)

	// Non-JSON payloads error (the caller logs and skips — never gates).
	_, _, err = JSONShapeSignature([]byte(`not json`))
	require.Error(t, err)
}

// TestContractFingerprintRoundTripAndKey pins the dedupe record's wire
// round-trip and its NUL-separated composite key (an id containing ':' cannot
// collide with another composite).
func TestContractFingerprintRoundTripAndKey(t *testing.T) {
	f := &ContractFingerprint{TypeID: "photos:meta", Version: 3, Fingerprint: "abcd"}
	require.Equal(t, []byte("photos:meta\x002\x00abcd"), (&ContractFingerprint{TypeID: "photos:meta", Version: 2, Fingerprint: "abcd"}).Key())

	bs, err := f.Marshal()
	require.NoError(t, err)
	got := &ContractFingerprint{}
	require.NoError(t, got.Unmarshal(bs))
	require.Equal(t, f, got)

	e, err := NewContractFingerprintEntity(f)
	require.NoError(t, err)
	require.True(t, IsContractFingerprint(e.Type.ID))
	require.True(t, IsInternal(e.Type.ID), "the dedupe mark is internal — syncables never see it")
	require.Equal(t, f.Key(), e.Key)

	// The mark's type is namespaced UNGATED: an older node skips it (at worst
	// a later re-announce, which consumers keyed on the fingerprint absorb).
	state, ok := reservedSystemClass(e.Type.ID)
	require.True(t, ok)
	require.Equal(t, compatUngated, state)
}

// TestContractExtensionKey pins the event key consumers converge on.
func TestContractExtensionKey(t *testing.T) {
	c := &ContractExtension{TypeID: "photo-meta", Version: 2, Fingerprint: "beef"}
	require.Equal(t, []byte("photo-meta:2:beef"), c.Key())
}
