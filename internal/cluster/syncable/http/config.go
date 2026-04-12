package http

// Config holds the parsed HTTP webhook syncable configuration.
type Config struct {
	Topic     string
	URL       string
	Method    string // "POST" or "PUT"
	TimeoutMs int
	Headers   []Header
}

// Header is a single HTTP header name-value pair.
type Header struct {
	Name  string
	Value string
}
