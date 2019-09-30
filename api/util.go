package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
)

// unmarshall unmarshalls a json payload from an http.Request
func unmarshall(r *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	log.Printf("Unmarshalling %s", string(body))
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
	w.WriteHeader(500)
	w.Write([]byte(err.Error()))
}

func writeMultipartAndHandleError(tomlFiles []string, w http.ResponseWriter) {
	err := writeMultipart(tomlFiles, w)
	if err != nil {
		errorTo500(w, err)
	}
}

func writeMultipart(tomlFiles []string, w http.ResponseWriter) error {
	mw := multipart.NewWriter(w)
	w.Header().Set("Content-Type", fmt.Sprintf("multipart/mixed;boundary=%s", mw.Boundary()))
	for _, tomlFile := range tomlFiles {
		part, err := mw.CreatePart(nil)
		if err != nil {
			return err
		}
		_, err = part.Write([]byte(tomlFile))
		if err != nil {
			return err
		}
	}
	if err := mw.Close(); err != nil {
		return err
	}
	return nil
}
