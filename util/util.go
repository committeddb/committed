package util

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
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

// PostJSONAndClose does PostJSON and then closes the body
func PostJSONAndClose(url string, v interface{}) {
	resp, _ := PostJSON(url, v)
	CloseResponseBody(resp)
}

// PostJSON takes a url and an interface, marshals the interface into json and posts to the url
func PostJSON(url string, v interface{}) (resp *http.Response, err error) {
	var http = &http.Client{
		Timeout: time.Second * 10,
	}

	r, err := json.Marshal(v)
	if err != nil {
		log.Printf("%v failed to marshal. Reason: %v\n", v, err)
		return nil, err
	}

	log.Printf("POSTing to %s %v\n", url, string(r))

	resp, err = http.Post(url, "application/json", bytes.NewReader(r))
	if err != nil {
		log.Printf("Error: %v\n", err)
	}

	if resp == nil {
		log.Printf("%s responded with nil\n", url)
	} else {
		log.Printf("%s responded with a %d\n", url, resp.StatusCode)
	}

	return resp, err
}

// CloseResponseBody closes the response body with nil checking along the way
func CloseResponseBody(resp *http.Response) {
	if resp != nil {
		if resp.Body != nil {
			resp.Body.Close()
		}
	}
}
