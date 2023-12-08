package utils

import (
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

func MD5(in string) string {
	hashed := md5.Sum([]byte(in))
	return hex.EncodeToString(hashed[:])
}
func CRC32(in string) string {
	v := crc32.ChecksumIEEE([]byte(in))
	return fmt.Sprintf("%08X", v)
}

func GetSignatureSha256(in io.Reader, privateKey *rsa.PrivateKey) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, in); err != nil {
		return "", err
	}
	hashed := h.Sum(nil)
	if privateKey != nil {
		signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed)
		if err != nil {
			return "", err
		}
		hashed = signature
	}
	sign := hex.EncodeToString(hashed)
	return sign, nil
}

func WriteFileToDir(dir string, filename string, in io.Reader) (int64, error) {
	filename = filepath.Join(dir, filename)
	fpath := filepath.Dir(filename)
	if err := os.MkdirAll(fpath, 0777); err != nil {
		return -1, errors.Wrap(err, "mkdir local path, "+fpath)
	}
	f, err := os.Create(filename)
	if err != nil {
		return -1, errors.Wrapf(err, "create file, %v", filename)
	}
	defer f.Close()

	n, err := io.Copy(f, in)
	if err != nil {
		return -1, errors.Wrapf(err, "write to file, %v", filename)
	}
	return n, nil
}

func RemoveFileFromDir(dir string, filename string) error {
	filename = filepath.Join(dir, filename)
	path := filepath.Dir(filename)
	if err := os.Remove(filename); err != nil {
		return errors.Wrapf(err, "remove file, %v", filename)
	}
	if err := os.Remove(path); err != nil {
		return errors.Wrapf(err, "remove path, %v", filename)
	}
	return nil
}

func ReadFileFromDir(dir string, filename string) ([]byte, error) {
	buffer := []byte{}
	filename = filepath.Join(dir, filename)
	file, err := os.Open(filename)
	if err != nil {
		return buffer, errors.Wrapf(err, filename)
	}
	defer file.Close()
	buffer, err = io.ReadAll(file)
	if err != nil {
		return buffer, errors.Wrapf(err, filename)
	}
	return buffer, nil
}
