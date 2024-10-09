package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"time"
)

func CopyFile(src, dst string) error {
	// 元ファイルを開く
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// 新しいファイルを作成する
	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	// ファイルの内容をコピーする
	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	// コピー完了後にファイルをフラッシュする
	err = destinationFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

// 5文字のハッシュ値を生成する関数
func GenerateShortHash() string {
	now := time.Now().String()
	hash := sha256.Sum256([]byte(now))
	hashString := hex.EncodeToString(hash[:])

	return hashString[:5]
}
