package b

import "lintfixture/a"

func viaExported(err error) a.Record {
	return a.Record{Message: err.Error()} // want `field Message of a.Record`
}

func viaExportedHelper(err error) a.Record {
	return a.RecordExported(err.Error()) // want `argument err.Error\(\) of RecordExported`
}
