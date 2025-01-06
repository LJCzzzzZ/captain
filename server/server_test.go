package server

import (
	"bytes"
	"testing"
)

func TestCutToLastMeesage(t *testing.T) {
	res := []byte("100\n101\n10")

	want, wRest := []byte("100\n101\n"), []byte("10")
	get, getRest, err := cutToLastMessage(res)
	if err != nil {
		t.Errorf("cutToLastMessage(%q): got error :%v; want no error", string(res), err)
	}
	if !bytes.Equal(get, want) || !bytes.Equal(wRest, getRest) {
		t.Errorf("Want: %q and get: %q not euqal", get, want)
	}
}
