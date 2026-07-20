package http

import (
	"context"
	httpgo "net/http"

	"github.com/committeddb/committed/internal/cluster"
)

// The four versioned config resources — database, ingestable, syncable,
// and type — share an identical handler set: create (POST), list (GET),
// list-versions, get-one-version, and rollback. The bodies differ only by
// the resource name used in error strings and the Cluster accessor method
// they call. Rather than copy each handler four ways, the factories below
// produce a handler closed over (name, accessor); http.go wires one per
// route. A new config resource is a few route lines and zero new handlers.
//
// Keeping the route table explicit (rather than registering a fixed set
// per resource) is deliberate: it lets the resources that deviate stay
// readable — type has no rollback and a bespoke GET /type/{id} graph
// handler, syncable adds errors/status/deadletter/replay routes — without
// special-casing inside a generic registrar.

// ConfigWriteResponse is the body of a config POST or rollback: the id the
// caller addressed and the server-assigned version the config now has (a
// rollback creates a NEW version; a byte-identical re-POST retains the
// existing one). Version is the one field the caller cannot know a priori —
// versions are assigned at apply — which is what makes this response carry
// information the old plain-text id echo did not. Version is omitted only if
// the post-propose version read fails (the write itself succeeded).
type ConfigWriteResponse struct {
	ID      string `json:"id"`
	Version uint64 `json:"version,omitempty"`
}

// currentVersion reads the version currently marked current for id, or 0 if
// it cannot be determined. Propose blocks until the entry is applied locally,
// so a read here observes the version the propose just created.
func currentVersion(versions func(string) ([]cluster.VersionInfo, error), id string) uint64 {
	vs, err := versions(id)
	if err != nil {
		return 0
	}
	for _, v := range vs {
		if v.Current {
			return v.Version
		}
	}
	return 0
}

// addConfig handles POST /{resource}/{id}: build a Configuration from the
// request body, propose it, and return {id, version} — the server-assigned
// version the caller cannot otherwise know.
func (h *HTTP) addConfig(
	name string,
	propose func(context.Context, *cluster.Configuration) error,
	versions func(string) ([]cluster.VersionInfo, error),
) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		c, err := createConfiguration(r)
		if err != nil {
			h.writeReadError(w, r, err, "invalid_config", "invalid "+name+" configuration")
			return
		}

		if err := propose(r.Context(), c); err != nil {
			writeProposeError(w, err, name, "propose "+name)
			return
		}

		writeJSONStatus(w, httpgo.StatusOK, ConfigWriteResponse{ID: c.ID, Version: currentVersion(versions, c.ID)})
	}
}

// listConfig handles GET /{resource}: return every current configuration.
func (h *HTTP) listConfig(name string, list func() ([]*cluster.Configuration, error)) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		if !h.linearize(w, r) {
			return
		}
		cfgs, err := list()
		if err != nil {
			writeInternalError(w, "failed to retrieve "+name+"s", err)
			return
		}

		writeConfigurations(w, cfgs)
	}
}

// getVersions handles GET /{resource}/{id}/versions: the version history.
func (h *HTTP) getVersions(name string, versions func(string) ([]cluster.VersionInfo, error)) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		if !h.linearize(w, r) {
			return
		}
		id := r.PathValue("id")
		vs, err := versions(id)
		if err != nil {
			writeVersionError(w, err, name)
			return
		}
		writeArrayBody(w, vs)
	}
}

// getVersion handles GET /{resource}/{id}/versions/{version}: one version.
func (h *HTTP) getVersion(name string, version func(string, uint64) (*cluster.Configuration, error)) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		id := r.PathValue("id")
		v, ok := parseVersion(w, r)
		if !ok {
			return
		}
		if !h.linearize(w, r) {
			return
		}
		cfg, err := version(id, v)
		if err != nil {
			writeVersionError(w, err, name)
			return
		}
		// A by-key GET returns the bare object; 404 already covers absence.
		writeConfiguration(w, cfg)
	}
}

// rollback handles POST /{resource}/{id}/rollback?to=<version>: re-propose
// the named historical version as the current configuration, returning
// {id, version} where version is the NEW version the rollback created.
func (h *HTTP) rollback(
	name string,
	version func(string, uint64) (*cluster.Configuration, error),
	propose func(context.Context, *cluster.Configuration) error,
	versions func(string) ([]cluster.VersionInfo, error),
) httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		id := r.PathValue("id")
		to, ok := parseRollbackTarget(w, r)
		if !ok {
			return
		}
		// Reading the target version is a linearizable read, exactly like
		// getVersion: without the barrier a lagging follower can 404 a version it
		// simply hasn't applied yet, turning a valid rollback into a spurious
		// not-found.
		if !h.linearize(w, r) {
			return
		}
		cfg, err := version(id, to)
		if err != nil {
			writeVersionError(w, err, name)
			return
		}
		if err := propose(r.Context(), cfg); err != nil {
			writeProposeError(w, err, name, "propose "+name+" rollback")
			return
		}
		writeJSONStatus(w, httpgo.StatusOK, ConfigWriteResponse{ID: cfg.ID, Version: currentVersion(versions, cfg.ID)})
	}
}
