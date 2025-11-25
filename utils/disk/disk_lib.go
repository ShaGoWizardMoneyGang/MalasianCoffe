package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	TMP_DIR = "/app/packet_receiver/tmp/"
)

// Atomic write [data] to [path]
func AtomicWrite(data []byte, path string) error {
	file_name := filepath.Base(path)

	f, err := os.CreateTemp(TMP_DIR, file_name)
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

	os.Rename(f.Name(), path)

	return nil
}

func AtomicWriteString(data string, path string) error {
	file_name := filepath.Base(path)

	// Por que TMP_DIR y no /tmp? Porque Docker, como siempre
	f, err := os.CreateTemp(TMP_DIR, file_name)
	if err != nil {
		return err
	}

	_, err = io.WriteString(f, data)
	if err != nil {
		return err
	}

	err = os.Rename(f.Name(), path)
	if err != nil {
		return err
	}

	return nil
}

func AtomicAppend(data string, path string) error {
	var old_data string

	// "Aguante go"
	exists := Exists(path)
	if !exists {
		old_data = ""
	} else {
		data_in_file, err := Read(path)
		if err != nil {
			// Si el archivo no existe, entonces hacemos de cuenta que el archivo
			// estaba vacio.  Ligeramente mas ergonomico y recrea el ">>" de la
			// shell
			if err == fs.ErrNotExist {
				old_data = ""
			} else {
				return err
			}
		} else {
			old_data = data_in_file
		}
	}

	new_data := old_data + "\n" + data

	err := AtomicWriteString(new_data, path)

	return err
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
	err = os.Chmod(f.Name(), 0666)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func CreateDir(path string) error {
	err := os.Mkdir(path, 0777)

	if err != nil && !os.IsExist(err) {
		return err
	}

	return nil
}

func DeleteFile(path string) error {
	err := os.Remove(path)

	return err
}

func DeleteDirRecursively(path string) {
	os.RemoveAll(path)
}
