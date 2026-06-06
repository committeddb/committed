package cluster

import "errors"

// ErrInvalidMember is returned by Cluster.AddMember / Cluster.RemoveMember
// when the supplied node id or url is not usable — a zero id, or an empty
// url on an add. The HTTP layer maps it to 400. It is a static-validation
// error (the request is malformed), distinct from a runtime failure to
// commit the change, which surfaces as a context error.
var ErrInvalidMember = errors.New("cluster: invalid member")
