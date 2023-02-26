package workflow

const (
	usernamePasswordStoreID = "usernamePasswordStoreID"
)

type KeyValueStore interface {
	Get(id string, key string) (string, error)
}

type Log interface {
	Put(id string, key string, value string) error
}

type Workflow struct {
	Log           Log
	KeyValueStore KeyValueStore
}
