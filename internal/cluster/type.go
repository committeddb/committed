package cluster

import (
	"bytes"
	"encoding/gob"
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

func NewUpsertTypeEntity(t *Type) (*Entity, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(t)
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(typeType, []byte(t.ID), buffer.Bytes()), nil
}

func NewDeleteTypeEntity(t *Type) *Entity {
	return NewDeleteEntity(typeType, []byte(t.ID))
}
