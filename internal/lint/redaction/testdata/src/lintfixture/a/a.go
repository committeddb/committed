package a

import (
	"errors"
	"fmt"
	"lintfixture/internal/cluster"
	"net/http"
	"strings"
)

type Record struct {
	Message string
	Cause   error
	Details any
}

func direct(err error) Record {
	return Record{Message: err.Error()} // want `field Message of Record`
}

func viaLocal(err error) Record {
	msg := fmt.Sprintf("sync failed: %v", err)
	return Record{Message: msg} // want `field Message of Record`
}

func keepChain(err error) Record { return Record{Cause: err} }

func redacted(err error) Record {
	msg, _ := cluster.RedactedMessage(err)
	return Record{Message: msg}
}

func truncate(s string) string {
	if len(s) > 10 {
		return s[:10]
	}
	return s
}

func viaWrapper(err error) Record {
	return Record{Message: truncate(err.Error())} // want `field Message of Record`
}

func viaWrapperRedacted(err error) Record {
	m, _ := cluster.RedactedMessage(err)
	return Record{Message: truncate(m)}
}

func record(msg string) Record { return Record{Message: msg} }

// RecordExported is a surface-forwarding helper for package b.
func RecordExported(msg string) Record { return record(msg) }

func recordf(format string, args ...any) Record { return record(fmt.Sprintf(format, args...)) }

func viaHelper(err error) Record {
	return record(err.Error()) // want `argument err.Error\(\) of record`
}

func viaHelperf(err error) Record {
	return recordf("failed: %v", err) // want `argument err of recordf`
}

func viaHelperRedacted(err error) Record {
	m, _ := cluster.RedactedMessage(err)
	return recordf("failed: %s", m)
}

func handler(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), 500) // want `the HTTP response body`
}

func handlerf(w http.ResponseWriter, err error) {
	fmt.Fprintf(w, "failed: %v", err) // want `the HTTP response body`
}

func handlerOK(w http.ResponseWriter, err error) {
	msg, _ := cluster.RedactedMessage(err)
	http.Error(w, msg, 500)
}

func builder(err error) Record {
	var sb strings.Builder
	fmt.Fprintf(&sb, "x: %v", err)
	return Record{Message: sb.String()} // want `field Message of Record`
}

func actual(a *cluster.Actual) Record {
	return Record{Message: fmt.Sprintf("failed on %v", a)} // want `field Message of Record`
}

func proposalString(p *cluster.Proposal) Record {
	return Record{Message: p.String()} // want `field Message of Record`
}

// An entity's raw bytes are the data plane and flow freely; only a RENDERING
// (String(), %v) is a source.
func entityKeyBytes(e *cluster.Entity) Record {
	return Record{Message: string(e.Key)}
}

func entityName(e *cluster.Entity) Record {
	return Record{Message: e.Name}
}

func entitiesFormatted(es []*cluster.Entity) Record {
	return Record{Message: fmt.Sprintf("entities: %v", es)} // want `field Message of Record`
}

type execError struct {
	label string
	err   error
}

func (e *execError) Error() string           { return e.label + ": " + e.err.Error() }
func (e *execError) RedactedMessage() string { return e.label + ": driver error" }
func (e *execError) Unwrap() error           { return e.err }

type badError struct{ err error }

func (e *badError) Error() string { return e.err.Error() }
func (e *badError) RedactedMessage() string {
	return e.err.Error() // want `RedactedMessage returns unredacted`
}

func authoredField(e *execError) Record {
	return Record{Message: e.label}
}

func methodRedacted(e *execError) Record {
	return Record{Message: e.RedactedMessage()}
}

func interfaceMethodRedacted(red cluster.RedactedError) Record {
	return Record{Message: red.RedactedMessage()}
}

func concreteText(e *execError) Record {
	return Record{Message: e.Error()} // want `field Message of Record`
}

// An Error() with a body is summarized like any function: authored text is
// clean, a rendered cause (execError above) is not.
type authoredError struct{ id string }

func (e *authoredError) Error() string { return "invalid member " + e.id }

func authoredText(e *authoredError) Record {
	return Record{Message: e.Error()}
}

func authoredViaInterface(e *authoredError) Record {
	var err error = e
	return Record{Message: err.Error()} // want `field Message of Record`
}

// A module method that stores its argument is a surface through its summary;
// calling it does not taint the receiver wholesale.
type reporter struct {
	notes []string
	last  error
	id    string
}

func (r *reporter) note(s string)     { r.notes = append(r.notes, s) }
func (r *reporter) observe(err error) { r.last = err }

func viaMethod(err error) []string {
	var r reporter
	r.note(err.Error()) // want `argument err.Error\(\) of note`
	return r.notes
}

func viaObserve(err error) Record {
	var r reporter
	r.observe(err)
	return Record{Message: r.id}
}

// A struct literal holding an error in an error-typed field does not taint
// its sibling string fields.
type raw struct {
	id        string
	decodeErr error
}

func literalSibling(err error) Record {
	r := raw{id: "x", decodeErr: err}
	return Record{Message: r.id}
}

func assign(r *Record, err error) {
	r.Message = err.Error() // want `the assignment to r.Message`
}

func mapLit(err error) map[string]any {
	return map[string]any{"error": err.Error()} // want `a value of map\[string\]any`
}

func sliceLit(err error) []string {
	return []string{err.Error()} // want `an element of \[\]string`
}

func asTarget(err error) Record {
	var ee *execError
	if errors.As(err, &ee) {
		return Record{Message: ee.Error()} // want `field Message of Record`
	}
	return Record{}
}

func logOnly(err error) string { return err.Error() }

func describe(err error) string { return "failed: " + err.Error() }

func viaReturn(err error) Record {
	return Record{Message: describe(err)} // want `field Message of Record`
}

func lookup(id string) (string, error) { return id, errors.New("x") }

func tuple(id string) Record {
	name, err := lookup(id)
	_ = err
	return Record{Message: name}
}

func joined(errs []error) Record {
	for _, e := range errs {
		return Record{Message: e.Error()} // want `field Message of Record`
	}
	return Record{}
}

func lengths(err error) Record {
	return Record{Message: fmt.Sprintf("%d", len(err.Error()))}
}

func typeSwitch(v any) Record {
	switch x := v.(type) {
	case error:
		return Record{Message: x.Error()} // want `field Message of Record`
	case string:
		return Record{Message: x}
	}
	return Record{}
}

func closureSink(err error) Record {
	build := func() Record {
		return Record{Message: err.Error()} // want `field Message of Record`
	}
	return build()
}
