package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

const maxInMemoryChunkSize = 10 * 1024 * 1024 //byte
var (
	errBufTooSamll = errors.New("buffer is too samll to fit a simple message")
)

// InMemory stores all the data in memory
type InMemory struct {
	sync.RWMutex
	lastChunk     string
	lastChunkIdx  uint64
	lastChunkSize uint64
	bufs          map[string][]byte
}

// Write accepts the messages from the clients and stores them
func (s *InMemory) Write(msgs []byte) error {
	s.Lock()
	defer s.Unlock()
	if s.lastChunk == "" || (s.lastChunkSize+uint64(len(msgs)) > maxInMemoryChunkSize) {
		s.lastChunk = fmt.Sprintf("chunk%d", s.lastChunkIdx)
		s.lastChunkSize = 0
		s.lastChunkIdx++
	}

	if s.bufs == nil {
		s.bufs = make(map[string][]byte)
	}
	s.bufs[s.lastChunk] = append(s.bufs[s.lastChunk], msgs...)
	s.lastChunkSize += uint64(len(msgs))
	return nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *InMemory) Read(chunk string, off uint64, maxSize uint64, w io.Writer) error {
	s.RLock()
	defer s.RUnlock()
	buf, ok := s.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}
	maxOff := uint64(len(buf))
	if off >= maxOff {
		return nil
	} else if off+maxSize >= maxOff {
		w.Write(buf[off:])
		return nil
	}
	truncated, _, err := cutToLastMessage(buf[off : off+maxSize])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil
}

func (s *InMemory) Ack(chunk string) error {
	s.Lock()
	defer s.Unlock()
	_, ok := s.bufs[chunk]
	if !ok {
		return fmt.Errorf("chunk %q does not exist", chunk)
	}
	if chunk == s.lastChunk {
		return fmt.Errorf("chunk %q is currently being written into and can't be acknowledged", chunk)
	}
	delete(s.bufs, chunk)
	return nil
}

func (s *InMemory) ListChunks() ([]Chunk, error) {
	s.RLock()
	defer s.RUnlock()

	res := make([]Chunk, 0, len(s.bufs))
	for chunk := range s.bufs {
		var c Chunk
		c.Complete = (s.lastChunk != chunk)
		c.Name = chunk
		res = append(res, c)
	}
	return res, nil
}

func cutToLastMessage(scratch []byte) (truncated []byte, rest []byte, err error) {
	len := len(scratch)
	if len == 0 {
		return nil, nil, nil
	}
	if scratch[len-1] == '\n' {
		return scratch, nil, nil
	}
	lastPos := bytes.LastIndexByte(scratch, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSamll
	}
	return scratch[0 : lastPos+1], scratch[lastPos+1:], nil
}
