package sqlitehench

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// Decrypt decryptes an array of bytes using the AES algorythm.
func Decrypt(data []byte, passphrase string) ([]byte, error) {
	var plain []byte
	if len(data) == 0 {
		return data, nil
	}
	key := []byte(createHash(passphrase))
	block, err := aes.NewCipher(key)
	if err != nil {
		return plain, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return plain, err
	}
	nonceSize := gcm.NonceSize()

	if nonceSize > len(data) {
		return plain, errors.New("cipher data not found")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plain, err = gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return plain, err
	}
	return plain, nil
}

// Encrypt encryptes an array of bytes using the AES algorythm.
func Encrypt(plainData []byte, passphrase string) ([]byte, error) {
	var cipherData []byte
	block, _ := aes.NewCipher([]byte(createHash(passphrase)))
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return cipherData, err
	}
	nonceBytes := make([]byte, gcm.NonceSize())
	cipherData = gcm.Seal(nonceBytes, nonceBytes, plainData, nil)

	return cipherData, nil

}

// Encrypt encryptes a file using the AES algorythm.
func EncryptFile(p string, pwdPhrase string) error {

	var b []byte
	var binc []byte
	var err error

	if b, err = ioutil.ReadFile(p); err != nil {
		return err
	}

	if binc, err = Encrypt(b, pwdPhrase); err != nil {
		return err
	}

	np := fmt.Sprintf("%s@~%vtmp", p, time.Now().Nanosecond())
	f, err := os.Create(np)
	if err != nil {
		os.Remove(np)
		fmt.Println(err)
	}

	if err = os.Remove(p); err != nil {
		return err
	}

	if err = os.Rename(np, p); err != nil {
		return err
	}

	f.Write(binc)
	f.Close()

	return nil
}

// DecryptFile decryptes a file using the AES algorythm.
func DecryptFile(p string, pwdPhrase string) error {

	var b []byte
	var binc []byte
	var err error

	if b, err = ioutil.ReadFile(p); err != nil {
		return err
	}

	if binc, err = Decrypt(b, pwdPhrase); err != nil {
		return err
	}

	np := fmt.Sprintf("%s@~%vtmp", p, time.Now().Nanosecond())
	f, err := os.Create(np)
	if err != nil {
		os.Remove(np)
		return err
	}

	if err = os.Remove(p); err != nil {
		return err
	}

	if err = os.Rename(np, p); err != nil {
		return err
	}

	f.Write(binc)
	f.Close()

	return nil
}
