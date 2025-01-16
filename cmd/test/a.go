package main

import (
	"fmt"
	"regexp"
)

func main() {
	var reg = regexp.MustCompile("^chunk([0-9]+)$")
	s := "chunk1000"
	res := reg.FindStringSubmatch(s)
	fmt.Println(res)
}
