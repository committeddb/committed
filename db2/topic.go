package main

// Topic represents a topic in the system
type Topic struct {
	Name string
}

func newTopic(name string) *Topic {
	return &Topic{name}
}
