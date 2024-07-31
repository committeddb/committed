package cluster

import (
	"google.golang.org/protobuf/proto"
)

type ValidationStrategy int

const (
	NoValidation = 0
)

type Type struct {
	ID         string
	Name       string
	Version    int
	SchemaType string // something like Thrift, Protobuf, JSON Schema, etc.
	Schema     []byte // The contents of the schema
	Validate   ValidationStrategy
}

var typeType = &Type{
	ID:      "268e1ac4-7d17-4798-afae-3f1f9aa6fc65",
	Name:    "InternalType",
	Version: 1,
}

func IsType(id string) bool {
	return id == typeType.ID
}

func NewUpsertTypeEntity(t *Type) (*Entity, error) {
	lt := &LogType{
		ID:         t.ID,
		Name:       t.Name,
		Version:    int32(t.Version),
		SchemaType: t.SchemaType,
		Schema:     t.Schema,
		Validate:   LogValidationStrategy(t.Validate),
	}

	bs, err := proto.Marshal(lt)
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(typeType, []byte(t.ID), bs), nil
}

func NewDeleteTypeEntity(t *Type) *Entity {
	return NewDeleteEntity(typeType, []byte(t.ID))
}

func (t *Type) Unmarshal(bs []byte) error {
	lt := &LogType{}
	err := proto.Unmarshal(bs, lt)
	if err != nil {
		return err
	}

	t.ID = lt.ID
	t.Name = lt.Name
	t.Version = int(lt.Version)
	t.Schema = lt.Schema
	t.SchemaType = lt.SchemaType
	t.Validate = ValidationStrategy(lt.Validate)

	return nil
}
