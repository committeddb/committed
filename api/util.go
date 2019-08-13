package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

// unmarshall unmarshalls a json payload from an http.Request
func unmarshall(r *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &v)
	return err
}

func proposeToml(w http.ResponseWriter, r *http.Request, f func(string) error) {
	toml, err := readerToString(r.Body)
	if err != nil {
		errorTo500(w, err)
		return
	}

	err = f(toml)
	if err != nil {
		errorTo500(w, err)
		return
	}

	w.Write(nil)
}

// readerToString gets a string from a reader
func readerToString(r io.Reader) (string, error) {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// errorTo500 writes the error and sets the header to 500
func errorTo500(w http.ResponseWriter, err error) {
	w.Write([]byte(err.Error()))
	w.WriteHeader(500)
}
