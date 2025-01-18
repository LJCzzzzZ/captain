package integration

import (
	"chukcha/server"
	"chukcha/web"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// InitAndServe checks validity of the supplied argumens
// the web server on the specified port
func InitAndServe(dirname string, port uint) error {
	filename := filepath.Join(dirname, "write_test")
	fmt.Println(filename)
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())
	backend, err := server.NewOnDisk(dirname)
	if err != nil {
		return fmt.Errorf("initalize on-disk backed: %v", err)
	}
	s := web.NewServer(backend, port)
	log.Printf("Listening connections")
	return s.Serve()
}
