package client

import (
	"bytes"
	"captain/protocol"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
)

var errBufTooSamll = errors.New("buffer is too samll to fit a simple message")

const defaultScratchSize = 64 * 1024

// Simple represents an instance of client connected to a set of Chukcha servers.
type Simple struct {
	addrs    []string
	cl       *http.Client
	off      uint64
	curChunk protocol.Chunk
}

// NewSimple creates a new client for the Chukcha server
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

// Send sends the messages to the Chukcha servers
func (s *Simple) Send(category string, msgs []byte) error {
	u := url.Values{
		"category": []string{category},
	}
	resp, err := s.cl.Post(s.addrs[0]+"/write?"+u.Encode(), "application/octet-stream", bytes.NewReader(msgs))
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

var errRetry = errors.New("please retry the request")

// Receive either wait for new messages or return an
// error in case something goes worong
// The scratch buffer can be used to read data
func (s *Simple) Process(category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}
	for {
		err := s.process(category, scratch, processFn)
		if err == errRetry {
			continue
		}
		return err
	}
}

func (s *Simple) process(category string, scratch []byte, processFn func([]byte) error) error {
	// select a addr random
	addrIdx := rand.IntN(len(s.addrs))
	addr := s.addrs[addrIdx]
	if err := s.updateCurrentChunk(category, addr); err != nil {
		return fmt.Errorf("updateCurrentChunk: %w", err)
	}

	u := url.Values{
		"off":      []string{strconv.Itoa(int(s.off))},
		"maxSize":  []string{strconv.Itoa(len(scratch))},
		"chunk":    []string{s.curChunk.Name},
		"category": []string{category},
	}

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	resp, err := s.cl.Get(readURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}
	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}
	if b.Len() == 0 {
		if !s.curChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}
			if !s.curChunk.Complete {
				// We actually did read until the end and no new data appeared in between requests
				if s.off >= s.curChunk.Size {
					return io.EOF
				}

				// New data appeared in between us sending the read request and
				// the chunk becoming complete
				return errRetry
			}
		}

		// The chunk has been marked complete. However, new data appeared
		// in between us sending the read request and the chunk becoming complete.
		if s.off < s.curChunk.Size {
			return errRetry
		}

		if err := s.ackCurrentChunk(category, addr); err != nil {
			return err
		}
		// need to read the next chunk so that we do not return empty
		// response
		s.curChunk = protocol.Chunk{}
		s.off = 0
		return errRetry
	}
	err = processFn(b.Bytes())
	if err == nil {
		s.off += uint64(b.Len())
	}
	return err
}

func (s *Simple) updateCurrentChunkCompleteStatus(category, addr string) error {
	chunks, err := s.listChunks(category, addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}
	for _, c := range chunks {
		if c.Name == s.curChunk.Name {
			s.curChunk = c
			return nil
		}
	}
	return nil
}

func (s *Simple) updateCurrentChunk(category, addr string) error {
	if s.curChunk.Name != "" {
		return nil
	}
	chunks, err := s.listChunks(category, addr)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	for _, c := range chunks {
		if c.Complete {
			s.curChunk = c
			return nil
		}
	}
	s.curChunk = chunks[0]
	return nil
}
func (s *Simple) listChunks(category, addr string) ([]protocol.Chunk, error) {
	u := url.Values{
		"category": []string{category},
	}
	listURL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())
	fmt.Println(listURL)
	resp, err := s.cl.Get(listURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("listChunk error: %s", body)
	}

	var res []protocol.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}
func (s *Simple) ackCurrentChunk(category, addr string) error {
	u := url.Values{
		"chunk":    []string{s.curChunk.Name},
		"size":     []string{strconv.Itoa(int(s.off))},
		"category": []string{category},
	}
	resp, err := s.cl.Get(fmt.Sprintf("%s/ack?%s", addr, u.Encode()))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d, %s", resp.StatusCode, b.String())
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}
