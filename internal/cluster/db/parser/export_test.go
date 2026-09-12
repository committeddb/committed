package parser

// The envelope vocabularies, exposed so the conformance test can prove each
// equals the keys the parsers actually read.
var (
	IngestableEnvelopeKeys = ingestableEnvelopeKeys
	SyncableEnvelopeKeys   = syncableEnvelopeKeys
	DatabaseEnvelopeKeys   = databaseEnvelopeKeys
)
