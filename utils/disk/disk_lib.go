package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// Atomic write [data] to [path]
func AtomicWrite(data string, path string) error {
	tmp_path := path + ".tmp"

	f, err := CreateFile(tmp_path)
	if err != nil {
		return err
	}

	if _, err := io.WriteString(f, data); err != nil {
		return err
	}

	os.Rename(tmp_path, path)

	return nil
}

// Check if file/dir exists
// Taken from: https://stackoverflow.com/a/10510783
func Exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}

	// Y a la mierda
	panic(err)
}

func CreateFile(path string) (*os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func CreateDir(path string) error {
	err := os.Mkdir("testdir", 0750)

	if err != nil && !os.IsExist(err) {
		return err
	}

	return nil
}
