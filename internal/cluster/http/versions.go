package http

import (
	"errors"
	httpgo "net/http"
	"strconv"

	"github.com/committeddb/committed/internal/cluster"
)

// Helpers shared by the version/rollback config handlers in
// config_handlers.go. The handlers themselves are generic factories; these
// parse the {version} path segment and the ?to rollback target, and map
// storage version-lookup errors to response codes.

func parseVersion(w httpgo.ResponseWriter, r *httpgo.Request) (uint64, bool) {
	s := r.PathValue("version")
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		writeError(w, httpgo.StatusBadRequest, "invalid_version", "version must be a positive integer")
		return 0, false
	}
	return v, true
}

func parseRollbackTarget(w httpgo.ResponseWriter, r *httpgo.Request) (uint64, bool) {
	s := r.URL.Query().Get("to")
	if s == "" {
		writeError(w, httpgo.StatusBadRequest, "missing_parameter", "'to' query parameter is required")
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		writeError(w, httpgo.StatusBadRequest, "invalid_version", "'to' query parameter must be a positive integer")
		return 0, false
	}
	return v, true
}

func writeVersionError(w httpgo.ResponseWriter, err error, resource string) {
	if errors.Is(err, cluster.ErrResourceNotFound) {
		writeError(w, httpgo.StatusNotFound, resource+"_not_found", resource+" not found")
	} else if errors.Is(err, cluster.ErrVersionNotFound) {
		writeError(w, httpgo.StatusNotFound, "version_not_found", "version not found")
	} else {
		writeInternalError(w, "failed to retrieve "+resource+" versions", err)
	}
}
