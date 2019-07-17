package api

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
)

// Unmarshall unmarshalls a json payload from an http.Request
func Unmarshall(r *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	// log.Printf("Body %v\n", string(body))
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

// LogMessage is a one stop place for turning off logging of certain message types
func LogMessage(m raftpb.Message) bool {
	// return !(m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgHeartbeatResp)
	// return false
	return !(m.Type == raftpb.MsgBeat)
}

// PostJSON takes a url and an interface, marshals the interface into json and posts to the url
func PostJSON(url string, v interface{}) (resp *http.Response, err error) {
	// var transport = &http.Transport{
	// 	TLSNextProto: make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
	// }

	var http = &http.Client{
		// Transport: transport,
		Timeout: time.Second * 10,
	}

	r, err := json.Marshal(v)
	if err != nil {
		log.Printf("%v failed to marshal. Reason: %v\n", v, err)
		return nil, err
	}

	// log.Printf("POSTing to %s %v\n", url, string(r))
	// log.Printf("POSTing to %s\n", url)

	resp, err = http.Post(url, "application/json", bytes.NewReader(r))
	// resp, err = post(http, url, bytes.NewReader(r))
	if err != nil {
		log.Printf("Error: %v\n", err)
	}

	if resp == nil {
		log.Printf("%s responded with nil\n", url)
	} else {
		// log.Printf("%s responded with a %d\n", url, resp.StatusCode)
	}

	return resp, err
}

func post(c *http.Client, url string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	time := time.Now().String()
	log.Printf("POSTing to %s at [%s]\n", url, time)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Timestamp", time)
	return c.Do(req)
}

// CloseResponseBody closes the response body with nil checking along the way
func CloseResponseBody(resp *http.Response) {
	if resp != nil {
		if resp.Body != nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
		}
	}
}
