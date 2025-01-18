package main

import "testing"

func funcA(t *testing.T, a, b, c int) {
	t.Helper()
	if a+b != c {
		t.Errorf("Sum a + b != c")
	}
}
func TestMyFunc(t *testing.T) {
	funcA(t, 1, 2, 4)
}
