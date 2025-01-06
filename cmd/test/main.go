package main

import (
	"bytes"
	"fmt"
)

func main() {

	buf := []byte("xx")
	b := bytes.NewBuffer(buf)
	fmt.Println(b.String())
	fmt.Println(string(buf))
}
