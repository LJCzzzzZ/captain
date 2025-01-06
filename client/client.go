package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
)

var errBufTooSamll = errors.New("buffer is too samll to fit a simple message")

const defaultScratchSize = 64 * 1024

type Simple struct {
	addrs []string
	cl    *http.Client
}

// NewSimple creates a new client for the Chukcha server
func NewClient(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

// Send sends the messages to the Chukcha servers
func (s *Simple) Send(msgs []byte) error {
	resp, err := s.cl.Post(s.addrs[0]+"/write", "application/octet-stream", bytes.NewReader(msgs))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}
	// if do not use the data, connections will be close soon
	io.Copy(io.Discard, resp.Body)
	return nil
}

// Receive either wait for new messages or return an
// error in case something goes worong
// The scratch buffer can be used to read data
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}
	resp, err := s.cl.Get(s.addrs[0] + "/read")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return nil, fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}
	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return nil, err
	}
	if b.Len() == 0 {
		return nil, io.EOF
	}
	return b.Bytes(), nil
}
