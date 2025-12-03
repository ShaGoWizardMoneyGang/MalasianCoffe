package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Esto es una cagada monumental. Tener hardcodead el directorio temporal es una
// m$#!@a, pero Docker fue hecha por el diablo y ubuntu:latest es su sabueso.
// Es lo que toca.
const (
	TMP_DIR = "/app/packet_receiver/tmp/"
)

// Atomic write [data] to [path]
func AtomicWrite(data []byte, path string) {
	file_name := filepath.Base(path)

	f, err := os.CreateTemp(TMP_DIR, file_name)
	if err != nil {
		panic(err)
	}

	length := len(data)
	var written = 0
	for offset := 0; offset < length; offset += written {
		written, err = f.Write(data[offset:])
		if err != nil {
			panic(err)
		}
	}

	os.Rename(f.Name(), path)
}

func AtomicWriteString(data string, path string) {
	file_name := filepath.Base(path)

	// Por que TMP_DIR y no /tmp? Porque Docker, como siempre
	f, err := os.CreateTemp(TMP_DIR, file_name)
	if err != nil {
		panic(err)
	}

	_, err = io.WriteString(f, data)
	if err != nil {
		panic(err)
	}

	err = os.Rename(f.Name(), path)
	if err != nil {
		panic(err)
	}
}

func AtomicAppend(data string, path string) {
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
				panic(err)
			}
		} else {
			old_data = data_in_file
		}
	}

	new_data := old_data + "\n" + data

	AtomicWriteString(new_data, path)
}


func Read(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	string_r := string(data)
	return string_r, nil
}

func ReadBytes(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return data
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

func CreateFile(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	err = os.Chmod(f.Name(), 0666)
	if err != nil {
		panic(err)
	}

	return f
}

func CreateDir(path string) {
	err := os.Mkdir(path, 0777)

	if err != nil && !os.IsExist(err) {
		panic(err)
	}
}

func DeleteFile(path string) error {
	err := os.Remove(path)

	return err
}

func DeleteDirRecursively(path string) {
	os.RemoveAll(path)
}
