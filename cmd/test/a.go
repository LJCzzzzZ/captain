package main

import (
	"fmt"
	"os"
)

func main() {
	s, _ := os.MkdirTemp(os.TempDir(), "hello")
	fmt.Println(s)
}
