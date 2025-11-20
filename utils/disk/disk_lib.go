package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// Atomic write [data] to [path]
func AtomicWrite(data []byte, path string) error {
	tmp_path := path + ".tmp"

	f, err := CreateFile(tmp_path)
	if err != nil {
		return err
	}

	length := len(data)
	var written = 0
	for offset := 0; offset < length; offset += written {
		written, err = f.Write(data[offset:])
		if err != nil {
			return err
		}
	}

	os.Rename(tmp_path, path)

	return nil
}

func AtomicWriteString(data string, path string) error {
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

func AtomicAppend(data string, path string) error {
	old_data, err := Read(path)
	if err != nil {
		// Si el archivo no existe, entonces hacemos de cuenta que el archivo
		// estaba vacio.  Ligeramente mas ergonomico y recrea el ">>" de la
		// shell
		if err == fs.ErrNotExist {
			old_data = ""
		} else {
			return err
		}
	}

	new_data := old_data + "\n" + data

	AtomicWriteString(new_data, path)

	return nil
}


func Read(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil
	}

	string_r := string(data)
	return string_r, nil
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
