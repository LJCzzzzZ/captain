package integration

import (
	"captain/web"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
)

// InitAndServe checks validity of the supplied argumens
// the web server on the specified port
func InitAndServe(etcdAddr string, dirname string, port uint) error {

	cfg := clientv3.Config{
		Endpoints:   strings.Split(etcdAddr, ","),
		DialTimeout: 50 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return fmt.Errorf("creating etcd client: %w", err)
	}
	defer c.Close()
	kapi := clientv3.NewKV(c)
	fmt.Println(kapi)
	filename := filepath.Join(dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %s", filename, err)
	}
	fp.Close()
	os.Remove(fp.Name())
	s := web.NewServer(kapi, dirname, port)
	log.Printf("Listening connections")
	return s.Serve()
}
