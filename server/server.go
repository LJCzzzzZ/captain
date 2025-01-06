package server

import (
	"bytes"
	"errors"
)

var errBufTooSamll = errors.New("buffer is too samll to fit a simple message")

// InMemory stores all the data in memory
type InMemory struct {
	buf     bytes.Buffer
	restBuf bytes.Buffer
}

func (s *InMemory) GetLen() int {
	return s.buf.Len()
}

// Send sends the messages to the Chukcha servers
func (s *InMemory) Send(msgs []byte) error {
	_, err := s.buf.Write(msgs)
	return err
}

// Receive either wait for new messages or return an
// error in case something goes worong
// The scratch buffer can be used to read data
func (s *InMemory) Receive(scratch []byte) ([]byte, error) {
	strat := 0
	if s.restBuf.Len() > 0 {
		if s.restBuf.Len() > s.buf.Len() {
			return nil, errBufTooSamll
		}
		n, err := s.restBuf.Read(scratch)
		if err != nil {
			return nil, err
		}
		s.restBuf.Reset()
		strat += n
	}
	n, err := s.buf.Read(scratch[strat:])
	if err != nil {
		return nil, err
	}

	truncated, rest, err := cutToLastMessage(scratch[0 : n+strat])
	if err != nil {
		return nil, err
	}
	s.restBuf.Reset()
	s.restBuf.Write(rest)
	return truncated, nil
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
