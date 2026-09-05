package http_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The proposal group runs against the real engine (enginetest_test.go): a
// POST /v1/proposal is observed at the far end of the pipeline — the
// recorder sink — so these tests prove intake through consensus to
// delivery, not a conversation with a stub. Error legs the engine cannot
// produce on demand (unconfirmed outcomes, too-large, disk-full) are pinned
// by the writeProposeError matrix in status_mapping_test.go; the
// broken-legacy-schema 422 is pinned in proposal_schema_test.go.

func awaitRow(t *testing.T, e *engine, key, wantJSON string) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, ok := e.sink.row(key)
		return ok && got != ""
	}, 15*time.Second, 10*time.Millisecond, "row %s never reached the sink", key)
	got, _ := e.sink.row(key)
	require.JSONEq(t, wantJSON, got, "payload must survive intake → consensus → delivery byte-faithfully")
}

// TestAddProposal_Success: one upsert flows end to end and the payload
// arrives at the sink intact.
func TestAddProposal_Success(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "photos", "key": "k1", "data": {"foo": "bar"}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"foo": "bar"}`)
}

// TestAddProposal_MultipleEntities: one proposal carrying entities of two
// types delivers both.
func TestAddProposal_MultipleEntities(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addType(t, "albums", "albums")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doJSON(t, "POST", "/v1/proposal", `{"entities": [
		{"typeId": "photos", "key": "k1", "data": {"a": 1}},
		{"typeId": "albums", "key": "k2", "data": {"b": 2}}
	]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"a": 1}`)
	awaitRow(t, e, "k2", `{"b": 2}`)
}

// TestAddProposal_Delete is the intake half of right-to-be-forgotten: a
// `delete: true` entity becomes a tombstone the sink receives as a delete,
// not an upsert.
func TestAddProposal_Delete(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "photos", "key": "k1", "delete": true}]}`)
	mustStatus(t, w, 200)
	require.Eventually(t, func() bool {
		return len(e.sink.deleted()) == 1
	}, 15*time.Second, 10*time.Millisecond, "the tombstone never reached the sink")
	require.Equal(t, []string{"k1"}, e.sink.deleted())
	_, upserted := e.sink.row("k1")
	require.False(t, upserted, "a delete must not arrive as an upsert")
}

// TestAddProposal_MixedUpsertAndDelete: one proposal may carry both; each
// entity is built according to its own delete flag.
func TestAddProposal_MixedUpsertAndDelete(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doJSON(t, "POST", "/v1/proposal", `{"entities": [
		{"typeId": "photos", "key": "k1", "data": {"a": 1}},
		{"typeId": "photos", "key": "k2", "delete": true}
	]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"a": 1}`)
	require.Eventually(t, func() bool {
		return len(e.sink.deleted()) == 1
	}, 15*time.Second, 10*time.Millisecond, "the tombstone never arrived")
	require.Equal(t, []string{"k2"}, e.sink.deleted())
}

// TestAddProposal_RejectsInternalTypeID is the cluster-brick guard: a
// proposal referencing a committed-internal/system type id must be rejected
// at the boundary (400) — committed, it would Fatal every node at apply.
func TestAddProposal_RejectsInternalTypeID(t *testing.T) {
	e := newEngine(t)

	// databaseType's grandfathered built-in id (frozen; cluster.IsInternal == true).
	const systemTypeID = "4698b77e-9a7c-41a2-aae4-984da0cd33c1"
	require.True(t, cluster.IsInternal(systemTypeID), "guard test needs a real internal type id")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "`+systemTypeID+`", "key": "k1", "data": {"not": "a Configuration protobuf"}}]}`)
	requireEnvelope(t, w, 400, "type_reserved")
}

// TestAddProposal_TypeNotFound: an unregistered type is a 400 at the
// boundary — the entry never enters the log.
func TestAddProposal_TypeNotFound(t *testing.T) {
	e := newEngine(t)
	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "missing", "key": "k1", "data": {}}]}`)
	requireEnvelope(t, w, 400, "type_not_found")
}

// --- schema validation, against real admitted types ---

const personSchema = `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}`

// TestAddProposal_SchemaValidation_Valid: a conforming payload under a
// strict (validate = 1) type flows through to the sink.
func TestAddProposal_SchemaValidation_Valid(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "person", "person", personSchema, 1)
	e.addRecorderSyncable(t, "rec-1", "person")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "data": {"name": "Alice", "age": 30}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"name": "Alice", "age": 30}`)
}

// TestAddProposal_SchemaValidation_Invalid: a payload missing a required
// field under a strict type is a 400 with structured details.
func TestAddProposal_SchemaValidation_Invalid(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "person", "person", personSchema, 1)

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "data": {"age": 30}}]}`)
	requireEnvelope(t, w, 400, "schema_validation_failed")
	require.Contains(t, w.Body.String(), "details")
}

// TestAddProposal_SchemaValidation_WrongType: a type mismatch is caught by
// the same gate.
func TestAddProposal_SchemaValidation_WrongType(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "person", "person", personSchema, 1)

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "data": {"name": "Alice", "age": "not a number"}}]}`)
	requireEnvelope(t, w, 400, "schema_validation_failed")
}

// TestAddProposal_SchemaValidation_NoValidation: a type carrying a schema
// with validate = 0 gates nothing.
func TestAddProposal_SchemaValidation_NoValidation(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "loose", "loose", `{"type":"object","required":["name"]}`, 0)
	e.addRecorderSyncable(t, "rec-1", "loose")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "loose", "key": "k1", "data": {"other": 1}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"other": 1}`)
}

// TestAddProposal_Delete_SkipsSchemaValidation: a delete carries no payload,
// so even a strict type must accept a bare tombstone.
func TestAddProposal_Delete_SkipsSchemaValidation(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "person", "person", personSchema, 1)
	e.addRecorderSyncable(t, "rec-1", "person")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "delete": true}]}`)
	mustStatus(t, w, 200)
	require.Eventually(t, func() bool {
		return len(e.sink.deleted()) == 1
	}, 15*time.Second, 10*time.Millisecond, "the tombstone never arrived")
}

// TestAddProposal_AnnounceCommitsDivergentPayload pins the signal-not-reject
// rule on the direct-proposal path: a divergent payload under an
// announce-typed (validate = 2) topic COMMITS — the tripwire announces the
// divergence instead of gating — while the same payload under a strict type
// stays a 400.
func TestAddProposal_AnnounceCommitsDivergentPayload(t *testing.T) {
	schema := `{"type":"object","properties":{"caption":{"type":"string"}},"additionalProperties":false}`
	divergent := `{"entities": [{"typeId": "cap", "key": "k1", "data": {"caption": 7}}]}`

	// Announce: divergence flows through to the sink. validate = 2 requires
	// schemaChangeTopic — the type that receives ContractExtension events.
	e := newEngine(t)
	e.addType(t, "capEvents", "capEvents")
	e.addTypeWithSchemaType(t, "cap", "cap", "JSONSchema", schema, 2, "schemaChangeTopic = \"capEvents\"\n")
	e.addRecorderSyncable(t, "rec-1", "cap")
	w := e.doJSON(t, "POST", "/v1/proposal", divergent)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"caption": 7}`)

	// Strict: the same divergence is still rejected at the gate.
	e2 := newEngine(t)
	e2.addTypeWithSchema(t, "cap", "cap", schema, 1)
	w = e2.doJSON(t, "POST", "/v1/proposal", divergent)
	requireEnvelope(t, w, 400, "schema_validation_failed")
}

// --- protobuf validation, against real admitted types ---

const protoSourcePerson = `syntax = "proto3";
message Person {
    string name = 1;
    int32 age = 2;
}
`

// TestAddProposal_ProtobufValidation covers the protobuf gate against a real
// admitted type: a conforming payload flows to the sink (three times — the
// cached validator serves repeat proposals), a type mismatch and an unknown
// field are 400s. The uncompilable-schema legs (bad source, missing message
// name, empty schema) are legacy-only shapes — admission rejects them — and
// are pinned in proposal_schema_test.go.
func TestAddProposal_ProtobufValidation(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchemaType(t, "person", "Person", "Protobuf", protoSourcePerson, 1, "")
	e.addRecorderSyncable(t, "rec-1", "person")

	for i := 1; i <= 3; i++ {
		w := e.doJSON(t, "POST", "/v1/proposal",
			fmt.Sprintf(`{"entities": [{"typeId": "person", "key": "k%d", "data": {"name": "Alice", "age": 30}}]}`, i))
		mustStatus(t, w, 200)
	}
	awaitRow(t, e, "k3", `{"name": "Alice", "age": 30}`)

	// age is declared int32; a string is a protojson type mismatch.
	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k4", "data": {"name": "Alice", "age": "thirty"}}]}`)
	requireEnvelope(t, w, 400, "schema_validation_failed")

	// An undeclared field is rejected.
	w = e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k5", "data": {"name": "Alice", "nickname": "Al"}}]}`)
	requireEnvelope(t, w, 400, "schema_validation_failed")
}

// TestAddProposal_ProtobufValidation_WithPackage: a package statement
// qualifies the message name; the validator still finds it by Type.Name.
func TestAddProposal_ProtobufValidation_WithPackage(t *testing.T) {
	src := `syntax = "proto3";
package app.v1;
message Person {
    string name = 1;
}
`
	e := newEngine(t)
	e.addTypeWithSchemaType(t, "person", "Person", "Protobuf", src, 1, "")
	e.addRecorderSyncable(t, "rec-1", "person")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "data": {"name": "Alice"}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"name": "Alice"}`)
}

// TestAddProposal_AttachesResolvedTypeVersion: the proposal boundary stamps
// each entity with the RESOLVED type (including its system-assigned
// Version) — the wire stamp Marshal reads. Observed at the sink: after two
// type versions, a proposed row arrives stamped with version 2.
func TestAddProposal_AttachesResolvedTypeVersion(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	// A version bump must declare its migration; none is the identity.
	e.addTypeWithSchemaType(t, "photos", "photos", "JSONSchema", `{"type":"object"}`, 0, "[migration]\nnone = true\n") // version 2
	e.addRecorderSyncable(t, "rec-1", "photos")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "photos", "key": "k1", "data": {"a": 1}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"a": 1}`)
	require.Equal(t, 2, e.sink.typeVersion("k1"),
		"the entity must carry the resolved Type.Version through the pipeline")
}

// TestAddProposal_SchemaValidation_CacheInvalidatesOnVersionBump: the
// compiled-validator cache is keyed by (typeID, version); a re-POSTed type
// bumps the version for real, so the next proposal validates against the
// NEW schema, not a stale cached one.
func TestAddProposal_SchemaValidation_CacheInvalidatesOnVersionBump(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchema(t, "person", "person",
		`{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`, 1)

	// Passes against schema v1 (warming the cache).
	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k1", "data": {"name": "Alice"}}]}`)
	mustStatus(t, w, 200)

	// The schema evolves: now email is required instead of name. (A version
	// bump must declare its migration; none is the identity.)
	e.addTypeWithSchemaType(t, "person", "person", "JSONSchema",
		`{"type":"object","required":["email"],"properties":{"email":{"type":"string"}}}`, 1, "[migration]\nnone = true\n")

	// The same payload must now fail — proof the bumped version missed the
	// cache and compiled the new schema.
	w = e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "person", "key": "k2", "data": {"name": "Bob"}}]}`)
	requireEnvelope(t, w, 400, "schema_validation_failed")
}

// TestAddProposal_BadJSON: a malformed body is a 400 before anything else.
func TestAddProposal_BadJSON(t *testing.T) {
	e := newEngine(t)
	w := e.doJSON(t, "POST", "/v1/proposal", "not json")
	requireEnvelope(t, w, 400, "invalid_json")
}

// TestAddProposal_SchemaValidation_UnknownSchemaType: an unrecognized
// SchemaType fails OPEN (the proposal commits unvalidated) per
// proposal-validation.md's "do not fail-closed for unknown schema types"
// guidance — admitted for real, since admission checks only that a
// validating type names SOME schema language.
func TestAddProposal_SchemaValidation_UnknownSchemaType(t *testing.T) {
	e := newEngine(t)
	e.addTypeWithSchemaType(t, "thing", "Thing", "Thrift", "not a real schema", 1, "")
	e.addRecorderSyncable(t, "rec-1", "thing")

	w := e.doJSON(t, "POST", "/v1/proposal",
		`{"entities": [{"typeId": "thing", "key": "k1", "data": {"anything": true}}]}`)
	mustStatus(t, w, 200)
	awaitRow(t, e, "k1", `{"anything": true}`)
}
