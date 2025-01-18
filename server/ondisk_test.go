package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitLastChunkIdx(t *testing.T) {
	dir := getTempDir(t)

	testCreateFile(t, filepath.Join(dir, "chunk1"))
	testCreateFile(t, filepath.Join(dir, "chunk10"))
	srv := testNewOnDisk(t, dir)

	want := uint64(11)
	got := srv.lastChunkIdx
	if got != want {
		t.Errorf("Last chunk index = %d, want %d", got, want)
	}
}

func TestGetFileDescriptor(t *testing.T) {
	dir := getTempDir(t)
	testCreateFile(t, filepath.Join(dir, "chunk1"))
	srv := testNewOnDisk(t, dir)

	testCases := []struct {
		desc     string
		filename string
		write    bool
		wantErr  bool
	}{
		{
			desc:     "Read from already existing file should not fail",
			filename: "chunk1",
			write:    false,
			wantErr:  false,
		},
		{
			desc:     "Should not overwrite existing files",
			filename: "chunk1",
			write:    true,
			wantErr:  true,
		},
		{
			desc:     "Should not be to read from files that don't exist",
			filename: "chunk2",
			write:    false,
			wantErr:  true,
		},
		{
			desc:     "Should be able to create files that don't exist",
			filename: "chunk2",
			write:    true,
			wantErr:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := srv.getFileDescriptor(tc.filename, tc.write)
			defer srv.forgetFileDescriptor(tc.filename)

			if tc.wantErr && err == nil {
				t.Errorf("wanted error, got not errors")
			} else if !tc.wantErr && err != nil {
				t.Errorf("wanted no errors, got error %v", err)
			}
		})
	}
}
func getTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "lastchunkidx")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}

	t.Cleanup(func() { os.RemoveAll(dir) })

	return dir
}

func testNewOnDisk(t *testing.T, dir string) *OnDisk {
	t.Helper()

	srv, err := NewOnDisk(dir)
	if err != nil {
		t.Fatalf("NewOnDisk(): %v", err)
	}
	return srv
}

func testCreateFile(t *testing.T, filename string) {
	t.Helper()

	if _, err := os.Create(filename); err != nil {
		t.Fatalf("could not create file %q: %v", filename, err)
	}
}
