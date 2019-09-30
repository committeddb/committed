package e2e

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

func (c *Control) post(path string, data string) ([]byte, error) {
	url := c.url(path)
	fmt.Printf("[http.go] Posting to %s\n", url)
	b, err := body(http.Post(url, "application/toml", strings.NewReader(data)))
	fmt.Println("[http.go] Waiting for raft consensus")
	// Wait for raft consensus
	time.Sleep(1 * time.Second)
	fmt.Println("[http.go] Returning")
	return b, err
}

func (c *Control) get(path string) ([]string, error) {
	return parseMultipart(http.Get(c.url(path)))
}

func body(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	body := resp.Body
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	body.Close()

	return bytes, nil
}

func parseMultipart(resp *http.Response, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	contentType := resp.Header.Get("Content-Type")
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}

	var parts []string
	mr := multipart.NewReader(resp.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		value, err := ioutil.ReadAll(part)
		if err != nil {
			return nil, err
		}
		part.Close()
		parts = append(parts, string(value))
	}

	return parts, nil
}

func (c *Control) url(path string) string {
	fmt.Printf("Choosing from nodes: %v\n", c.nodes)
	n := c.nodes[rand.Intn(len(c.nodes))]
	return fmt.Sprintf("%s%s", n.url, path)
}
