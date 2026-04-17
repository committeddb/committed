package http

import (
	"errors"
	httpgo "net/http"
	"strconv"

	"github.com/philborlin/committed/internal/cluster"
)

// --- Ingestable ---

func (h *HTTP) GetIngestableVersions(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	versions, err := h.c.IngestableVersions(id)
	if err != nil {
		writeVersionError(w, err, "ingestable")
		return
	}
	writeArrayBody(w, versions)
}

func (h *HTTP) GetIngestableVersion(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.IngestableVersion(id, version)
	if err != nil {
		writeVersionError(w, err, "ingestable")
		return
	}
	writeConfigurations(w, []*cluster.Configuration{cfg})
}

func (h *HTTP) RollbackIngestable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	to, ok := parseRollbackTarget(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.IngestableVersion(id, to)
	if err != nil {
		writeVersionError(w, err, "ingestable")
		return
	}
	if err := h.c.ProposeIngestable(r.Context(), cfg); err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_ingestable_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose ingestable rollback")
		}
		return
	}
	_, _ = w.Write([]byte(cfg.ID))
}

// --- Syncable ---

func (h *HTTP) GetSyncableVersions(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	versions, err := h.c.SyncableVersions(id)
	if err != nil {
		writeVersionError(w, err, "syncable")
		return
	}
	writeArrayBody(w, versions)
}

func (h *HTTP) GetSyncableVersion(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.SyncableVersion(id, version)
	if err != nil {
		writeVersionError(w, err, "syncable")
		return
	}
	writeConfigurations(w, []*cluster.Configuration{cfg})
}

func (h *HTTP) RollbackSyncable(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	to, ok := parseRollbackTarget(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.SyncableVersion(id, to)
	if err != nil {
		writeVersionError(w, err, "syncable")
		return
	}
	if err := h.c.ProposeSyncable(r.Context(), cfg); err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_syncable_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose syncable rollback")
		}
		return
	}
	_, _ = w.Write([]byte(cfg.ID))
}

// --- Database ---

func (h *HTTP) GetDatabaseVersions(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	versions, err := h.c.DatabaseVersions(id)
	if err != nil {
		writeVersionError(w, err, "database")
		return
	}
	writeArrayBody(w, versions)
}

func (h *HTTP) GetDatabaseVersion(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.DatabaseVersion(id, version)
	if err != nil {
		writeVersionError(w, err, "database")
		return
	}
	writeConfigurations(w, []*cluster.Configuration{cfg})
}

func (h *HTTP) RollbackDatabase(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	to, ok := parseRollbackTarget(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.DatabaseVersion(id, to)
	if err != nil {
		writeVersionError(w, err, "database")
		return
	}
	if err := h.c.ProposeDatabase(r.Context(), cfg); err != nil {
		var configErr *cluster.ConfigError
		if errors.As(err, &configErr) {
			writeError(w, httpgo.StatusBadRequest, "invalid_database_config", configErr.Error())
		} else {
			writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to propose database rollback")
		}
		return
	}
	_, _ = w.Write([]byte(cfg.ID))
}

// --- Type ---

func (h *HTTP) GetTypeVersions(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	versions, err := h.c.TypeVersions(id)
	if err != nil {
		writeVersionError(w, err, "type")
		return
	}
	writeArrayBody(w, versions)
}

func (h *HTTP) GetTypeVersion(w httpgo.ResponseWriter, r *httpgo.Request) {
	id := r.PathValue("id")
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	cfg, err := h.c.TypeVersion(id, version)
	if err != nil {
		writeVersionError(w, err, "type")
		return
	}
	writeConfigurations(w, []*cluster.Configuration{cfg})
}

// --- Helpers ---

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
		writeError(w, httpgo.StatusInternalServerError, "internal_error", "failed to retrieve "+resource+" versions")
	}
}
