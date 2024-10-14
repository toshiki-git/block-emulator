package utils

import (
	"crypto/sha256"
	"encoding/hex"
)

type Address = string

// generateEthereumAddress: generates a 20-byte Ethereum address from the hash of the input string
func GenerateEthereumAddress(input Address) Address {
	hash := sha256.Sum256([]byte(input))        // Hash the input string using SHA-256
	return "0x" + hex.EncodeToString(hash[:20]) // Use the first 20 bytes for the Ethereum address
}
