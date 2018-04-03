package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// Unmarshall unmarshalls a json payload from an http.Request
func Unmarshall(r *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	log.Printf("Body %v\n", string(body))
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &v)
	return err
}

// PostJSON takes a url and an interface, marshals the interface into json and posts to the url
func PostJSON(url string, v interface{}) (resp *http.Response, err error) {
	r, err := json.Marshal(v)
	if err != nil {
		fmt.Printf("%v failed to marshal. Reason: %v\n", v, err)
		return nil, err
	}

	log.Printf("POSTing to %s %v\n", url, string(r))

	return http.Post(url, "application/json", bytes.NewReader(r))
}
