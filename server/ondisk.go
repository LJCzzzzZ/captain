package server

import (
	"fmt"
	"io"
	"os"
)

const readBlockSize = 1024 * 1024

// OnDisk stores all the data on disk
type OnDisk struct {
	fp *os.File
}

// NewOnDisk creates a server that stores all it's data on disk.
func NewOnDisk(fp *os.File) *OnDisk {
	return &OnDisk{fp: fp}
}

// Write accepts the messages from the clients and stores them.
func (s *OnDisk) Write(msgs []byte) error {
	n, err := s.fp.Write(msgs)
	fmt.Println("write %d byte to file", n)
	return err
}

// Read copies the data from the in-memory store and writes
// the data read to the provided Writer, starting with the
// offset provided.
func (s *OnDisk) Read(off uint64, maxSize uint64, w io.Writer) error {
	buf := make([]byte, maxSize)
	n, err := s.fp.ReadAt(buf, int64(off))
	if n == 0 {
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
	truncated, _, err := cutToLastMessage(buf[0:n])
	if err != nil {
		return err
	}
	if _, err := w.Write(truncated); err != nil {
		return err
	}
	return nil
}

// Ack marks the current chunk as done and deletes it's contents.
func (s *OnDisk) Ack() error {
	var err error
	err = s.fp.Truncate(0)
	if err != nil {
		return err
	}
	_, err = s.fp.Seek(0, io.SeekStart)
	return err
}
