package util

import (
	"encoding/json"
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
