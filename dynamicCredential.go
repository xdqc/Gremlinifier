package histo

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"
)

type DynamicCredentialProvider struct {
	CredentialFile   string
	UsernameFromFile string `json:"username"`
	PasswordFromFile string `json:"password"`
}

func (dynCred *DynamicCredentialProvider) updateCredentials() error {
	file, err := os.Open(dynCred.CredentialFile)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&dynCred); err != nil {
		return err
	}

	return nil
}

func (dynCred *DynamicCredentialProvider) Username() (string, error) {
	if err := dynCred.updateCredentials(); err != nil {
		return "", errors.Wrapf(err, "reading credentials from '%s'", dynCred.CredentialFile)
	}

	if len(dynCred.UsernameFromFile) == 0 {
		return "", fmt.Errorf("username not set, use export CDB_USERNAME=/dbs/<cosmosdb name>/colls/<graph name> to specify it")
	}
	return dynCred.UsernameFromFile, nil
}

func (dynCred *DynamicCredentialProvider) Password() (string, error) {
	if err := dynCred.updateCredentials(); err != nil {
		return "", errors.Wrapf(err, "reading credentials from '%s'", dynCred.CredentialFile)
	}

	if len(dynCred.PasswordFromFile) == 0 {
		return "", fmt.Errorf("password not set")
	}
	return dynCred.PasswordFromFile, nil
}
