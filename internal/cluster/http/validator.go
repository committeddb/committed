package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// entityValidator validates an entity's raw JSON data against a type
// schema. One concrete implementation exists per Type.SchemaType that
// AddProposal knows how to handle (JSONSchema, Protobuf). Implementations
// own the compiled schema artifact and are cached by (typeID, Version).
type entityValidator interface {
	// validate returns:
	//  - nil: data is schema-valid.
	//  - schemaValidationError: data is well-formed but fails the schema.
	//    The wrapped error's message is safe to surface in HTTP details.
	//  - any other error: the schema or input was structurally unusable
	//    (malformed JSON, malformed descriptor). Treated as 5xx by callers.
	validate(data []byte) error
}

// schemaValidationError marks a validation failure that the caller
// should surface as a 400. Any other error from validate() is a 5xx.
type schemaValidationError struct {
	err error
}

func (e *schemaValidationError) Error() string { return e.err.Error() }
func (e *schemaValidationError) Unwrap() error { return e.err }

// compileValidator builds an entityValidator for the given type, or
// returns (nil, nil) if this type shouldn't be validated. Unknown
// SchemaType values with ValidateSchema fall through to (nil, nil) —
// we fail-open for now per proposal-validation.md's "do not fail-closed
// for unknown schema types" guidance, which becomes the place to
// revisit once Thrift/Avro land.
func compileValidator(t *cluster.Type) (entityValidator, error) {
	if t.Validate != cluster.ValidateSchema {
		return nil, nil
	}
	switch t.SchemaType {
	case "JSONSchema":
		return compileJSONSchemaValidator(t)
	case "Protobuf":
		return compileProtobufValidator(t)
	default:
		return nil, nil
	}
}

// --- JSONSchema ----------------------------------------------------------

type jsonSchemaValidator struct {
	sch *jsonschema.Schema
}

func compileJSONSchemaValidator(t *cluster.Type) (entityValidator, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(t.Schema))
	if err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	url := "urn:committed:type:" + t.ID
	c := jsonschema.NewCompiler()
	if err := c.AddResource(url, doc); err != nil {
		return nil, fmt.Errorf("add resource: %w", err)
	}
	sch, err := c.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return &jsonSchemaValidator{sch: sch}, nil
}

func (v *jsonSchemaValidator) validate(data []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return &schemaValidationError{err: fmt.Errorf("entity data is not valid JSON: %w", err)}
	}
	if err := v.sch.Validate(doc); err != nil {
		return &schemaValidationError{err: err}
	}
	return nil
}

// --- Protobuf -----------------------------------------------------------

// protobufSchemaFile is the synthetic filename we feed to protocompile
// for an in-memory .proto source. It's referenced nowhere else — the
// resolver returns the schema bytes regardless of filename, and the
// compiled FileDescriptor's Path is this constant.
const protobufSchemaFile = "type.proto"

type protobufValidator struct {
	md protoreflect.MessageDescriptor
}

// compileProtobufValidator parses the .proto source in t.Schema and
// selects the message whose name equals t.Name. The .proto may declare
// multiple messages, but only the one named after the type is the
// entity shape — helper messages (nested structs, oneofs) must be
// declared in the same file and referenced by that root.
func compileProtobufValidator(t *cluster.Type) (entityValidator, error) {
	if t.Name == "" {
		return nil, errors.New("Protobuf type requires a non-empty Name that matches the top-level message name")
	}

	// Wrap the in-memory resolver with WithStandardImports so the user's
	// .proto can reference google/protobuf/descriptor.proto and friends
	// without us having to ship their sources. Our Accessor only serves
	// the user's file — every other path returns os.ErrNotExist, which
	// WithStandardImports catches and fills in for the well-known files.
	inMemory := &protocompile.SourceResolver{
		Accessor: func(filename string) (io.ReadCloser, error) {
			if filename == protobufSchemaFile {
				return io.NopCloser(bytes.NewReader(t.Schema)), nil
			}
			return nil, os.ErrNotExist
		},
	}
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(inMemory),
	}

	files, err := compiler.Compile(context.Background(), protobufSchemaFile)
	if err != nil {
		return nil, fmt.Errorf("compile .proto: %w", err)
	}
	if len(files) == 0 {
		return nil, errors.New("compile .proto: no files produced")
	}

	// Look up by short name first, then by fully-qualified name (package
	// + name) if the .proto declares a `package` statement.
	msgs := files[0].Messages()
	md := msgs.ByName(protoreflect.Name(t.Name))
	if md == nil {
		md = findMessageByShortName(msgs, t.Name)
	}
	if md == nil {
		return nil, fmt.Errorf("Protobuf schema has no message named %q (declare `message %s { ... }` in the .proto)", t.Name, t.Name)
	}

	return &protobufValidator{md: md}, nil
}

func findMessageByShortName(msgs protoreflect.MessageDescriptors, short string) protoreflect.MessageDescriptor {
	want := protoreflect.Name(short)
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		if md.Name() == want {
			return md
		}
	}
	return nil
}

func (v *protobufValidator) validate(data []byte) error {
	msg := dynamicpb.NewMessage(v.md)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, msg); err != nil {
		return &schemaValidationError{err: cleanProtoJSONError(err)}
	}
	return nil
}

// cleanProtoJSONError strips protojson's "(line N:M):" prefix because
// entity data is a JSON object, not a multi-line document, so the
// position is noise that doesn't help operators diagnose.
func cleanProtoJSONError(err error) error {
	s := err.Error()
	if i := strings.Index(s, "): "); i >= 0 && strings.HasPrefix(s, "(line ") {
		return errors.New(s[i+3:])
	}
	return err
}
