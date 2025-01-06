package main

import (
	"chukcha/server"
	"chukcha/web"
	"flag"
	"log"
	"os"
)

var (
	filename = flag.String("filename", "", "The directory name where to put all the data")
	inmem    = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port     = flag.Uint("port", 8080, "Network port to listen on")
)

func main() {
	flag.Parse()
	var backend web.Storage
	if *inmem {
		backend = &server.InMemory{}
	} else {
		if *filename == "" {
			log.Fatalf("The flag `--filename` must be provided")
		}
		// filename := filepath.Join(*filename, "write_test")
		fp, err := os.OpenFile(*filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Could noe create test file %q: %s", *filename, err)
		}
		defer fp.Close()
		backend = server.NewOnDisk(fp)
	}
	s := web.NewServer(backend, *port)
	log.Printf("Listening connections")
	s.Serve()
}
