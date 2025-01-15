package main

import (
	"fmt"
	"go/build"
	"log"
	"os"
	"os/exec"
)

func main() {
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}
	log.Printf("Compiling chukcha")
	out, err := exec.Command("go", "install", "-v", "/root/wk/chukcha").CombinedOutput()
	if err != nil {
		log.Printf("Failed to build: %v", err)
		fmt.Errorf("compilation failed: %v (out: %s)", err, string(out))
	}
	cmd := exec.Command(goPath+"/bin/chukcha", fmt.Sprintf("-port=%d", 8080))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Start()
}
