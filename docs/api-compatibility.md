# API compatibility and versioning

This document is the contract for how the Committed HTTP API evolves. It
is for client authors and operators who need to know what an upgrade can
and cannot break.

## URL-prefix versioning

Every API endpoint is served under a major-version URL prefix:

```
/v1/proposal
/v1/database/{id}
/v1/syncable/{id}/status
...
```

The prefix is the *only* version signal. There is no version header,
no media-type parameter, and no query flag. A client that hardcodes
`/v1` in its base URL has pinned itself to a major version.

## What is allowed within a major version

Changes that **do not** bump the prefix (a client written against `/v1`
keeps working):

- Adding a new endpoint.
- Adding a new optional request field. Omitting it must preserve the
  prior behaviour.
- Adding a new field to a response body. Clients must ignore unknown
  fields.
- Adding a new optional query parameter with a backward-compatible
  default.
- Adding a new enum value or error `code` (clients must treat an
  unrecognised `code` as a generic failure for that HTTP status).
- Relaxing a validation rule so that previously-rejected requests now
  succeed.

Clients **must** tolerate all of the above to be considered compatible.

## What bumps the major version

Changes that **do** bump the prefix (e.g. introduce `/v2`):

- Removing or renaming an endpoint, field, or parameter.
- Changing the type or meaning of an existing field.
- Making a previously-optional request field required.
- Tightening validation so previously-valid requests start failing.
- Changing a success status code or the default behaviour of an
  endpoint.

When `/v2` ships, `/v1` continues to be served for **at least one
further release** so clients have an overlap window to migrate.

## Deprecation policy

A deprecation always gives at least one release of overlap between the
old and new behaviour. While something is deprecated:

- It keeps working unchanged.
- Responses carry a `Deprecation: true` header and a `Warning` header
  naming the replacement, so deprecated usage is visible in client and
  proxy logs.
- The removal is called out in the release notes for the release that
  introduces the deprecation and again for the release that removes it.

There are currently no deprecated endpoints. `/v1` is the first and only
major version; there is no pre-`/v1` surface to keep alive.

## Operational endpoints are not API surface

These endpoints are infrastructure for orchestrators and humans, not
part of the versioned API. They are **never** prefixed and are exempt
from this contract (and from authentication):

- `/health` — liveness probe
- `/ready` — readiness probe
- `/version` — build information
- `/openapi.yaml` — the machine-readable spec
- `/docs` — Swagger UI

The OpenAPI document at `/openapi.yaml` is the authoritative description
of the versioned surface; its `info.version` tracks the spec revision,
which is independent of the `/v1` URL major version.
