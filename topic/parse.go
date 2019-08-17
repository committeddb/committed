package topic

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/viper"
)

// ParseTopic turns a toml file into a Topic
func ParseTopic(style string, reader io.Reader, baseDir string) (string, Topic, error) {
	name, err := PreParseTopic(style, reader, baseDir)
	if err != nil {
		return "", nil, err
	}

	topic, err := New(name, baseDir)
	if err != nil {
		return "", nil, err
	}

	return name, topic, nil
}

// PreParseTopic runs through the Parse checks without creating the topic
func PreParseTopic(style string, reader io.Reader, baseDir string) (string, error) {
	v, err := parseBytes(style, reader)
	if err != nil {
		return "", err
	}

	name := v.GetString("topic.name")

	if len(strings.TrimSpace(name)) == 0 {
		return "", fmt.Errorf("topic name is empty")
	}

	return name, nil
}

func parseBytes(style string, reader io.Reader) (*viper.Viper, error) {
	var v = viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	return v, nil
}
