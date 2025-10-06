package main

import (
	"encoding/csv"
	"os"
	"sort"
)

type User struct {
	Store     string
	Birthdate string
}

func readFile(filename string) ([]User, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var users []User

	for i := range lines {
		users = append(users, User{
			Store:     lines[i][0],
			Birthdate: lines[i][1],
		})
	}
	return users, nil
}

func birthdatesByStore(users []User) map[string][]User {
	bdByStores := make(map[string][]User)

	for _, user := range users {
		bdByStores[user.Store] = append(bdByStores[user.Store], user)
	}

	for store := range bdByStores {
		sort.Slice(bdByStores[store], func(i, j int) bool {
			return bdByStores[store][i].Birthdate < bdByStores[store][j].Birthdate
		})
	}

	return bdByStores

}

func userExistsInStore(user User, expectedUsers []User) bool {
	for _, expectedUser := range expectedUsers {
		if expectedUser.Birthdate == user.Birthdate {
			return true
		}
	}
	return false
}

func main() {
	expectedFile := os.Args[1]
	outputFile := os.Args[2]

	expectedUsers, _ := readFile(expectedFile)

	outputUsers, _ := readFile(outputFile)

	expectedByStore := birthdatesByStore(expectedUsers)
	outputByStore := birthdatesByStore(outputUsers)

	isAllValid := true

	for store, outputUsersInStore := range outputByStore {
		// Cuando accedo a un valor del map, me devuelve el valor y además un booleano que me dice si la clave existe o no
		expectedUsersInStore, storeExists := expectedByStore[store]

		if !storeExists {
			isAllValid = false
		} else {
			// Verifico cada ususario en la tienda
			for _, outputUser := range outputUsersInStore {
				if !userExistsInStore(outputUser, expectedUsersInStore) {
					isAllValid = false
				}
			}
		}
	}

	if isAllValid { // Si todo salio bien...
		os.Exit(0)
	} else {
		os.Exit(1)
	}

}
