// Definition of transaction

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"time"
)

type Transaction struct {
	Sender    utils.Address
	Recipient utils.Address
	Nonce     uint64
	Signature []byte // not implemented now.
	Value     *big.Int
	TxHash    []byte

	Time time.Time // TimeStamp the tx proposed. (supervisorにtxが生成された時間)

	// used in transaction relaying
	Relayed bool
	// used in broker, if the tx is not a broker1 or broker2 tx, these values should be empty.
	HasBroker      bool // Recepient or Sender is a broker
	SenderIsBroker bool
	OriginalSender utils.Address
	FinalRecipient utils.Address
	RawTxHash      []byte

	// Fields for smart contract transactions
	HasContract          bool
	InternalTxs          []*InternalTransaction
	IsCrossShardFuncCall bool
	IsAllInner           bool
	IsDeleted            bool
	IsExecuteCLPA        bool
	StateChangeAccounts  map[utils.Address]*Account
	RequestID            string
	DivisionCount        int
	MergeCount           int
}

func (tx *Transaction) PrintTx() {
	fmt.Printf("Sender: %s, Recipient: %s, HasContract: %t, InternalTxsNum: %d, IsAllInner: %t, IscrossShardFuncCall: %t, DivisionCount: %d, IsDeleted: %t, IsExecuteCLPA: %t \n",
		tx.Sender, tx.Recipient, tx.HasContract, len(tx.InternalTxs), tx.IsAllInner, tx.IsCrossShardFuncCall, tx.DivisionCount, tx.IsDeleted, tx.IsExecuteCLPA)
}

// Encode transaction for storing
func (tx *Transaction) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Decode transaction
func DecodeTx(to_decode []byte) *Transaction {
	var tx Transaction

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

func (tx *Transaction) TypeTraceAddresses() []string {
	var addresses []string
	for _, internalTx := range tx.InternalTxs {
		addresses = append(addresses, internalTx.TypeTraceAddress)
	}
	return addresses
}

// new a transaction
func NewTransaction(sender, recipient string, value *big.Int, nonce uint64, proposeTime time.Time) *Transaction {
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Time:      proposeTime,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.StateChangeAccounts = make(map[utils.Address]*Account)
	return tx
}
