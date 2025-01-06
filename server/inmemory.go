package server

import (
	"bytes"
	"errors"
	"io"
)

var (
	errBufTooSamll = errors.New("buffer is too samll to fit a simple message")
)

// InMemory stores all the data in memory
type InMemory struct {
	buf []byte
}

// Write accepts the messages from the clients and stores them
func (s *InMemory) Write(msgs []byte) error {
	s.buf = append(s.buf, msgs...)
	return nil
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *InMemory) Read(off uint64, maxSize uint64, w io.Writer) error {
	maxOff := uint64(len(s.buf))
	if off >= maxOff {
		return nil
	} else if off+maxSize >= maxOff {
		w.Write(s.buf[off:])
		return nil
	}
	truncated, _, err := cutToLastMessage(s.buf[off : off+maxSize])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil
}

func (s *InMemory) Ack() error {
	s.buf = s.buf[0:0]
	return nil
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
