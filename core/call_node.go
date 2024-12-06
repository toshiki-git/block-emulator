package core

import (
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
)

// CallNode はスマートコントラクトの呼び出しノードを表します
type CallNode struct {
	TypeTraceAddress    string // ex: "0" "0_1", "0_1_0" ...
	CallType            string // ex: "call", "staticcall", "delegatecall", "create"
	Sender              string
	SenderIsContract    bool
	SenderShardID       int
	Recipient           string
	RecipientIsContract bool
	RecipientShardID    int
	Value               *big.Int
	Children            []*CallNode
	Parent              *CallNode `json:"-"`
	IsLeaf              bool      // 葉ノードかどうかを示すフラグ
	IsProcessed         bool      // 処理済みかどうかを示すフラグ
}

// BuildExecutionCallTree は与えられたトレースリストから呼び出しツリーを構築し、root ノードを返します
func BuildExecutionCallTree(tx *Transaction, processedMap map[string]bool) *CallNode {
	root := &CallNode{
		TypeTraceAddress:    "root",
		CallType:            "root",
		Sender:              tx.Sender,
		SenderShardID:       Addr2Shard(tx.Sender),
		SenderIsContract:    false,
		Recipient:           tx.Recipient,
		RecipientIsContract: true,
		RecipientShardID:    Addr2Shard(tx.Recipient),
		Value:               tx.Value,
		IsLeaf:              false,
		IsProcessed:         processedMap["root"], // root の処理状況を設定
	}
	nodeMap := make(map[string]*CallNode)
	nodeMap["root"] = root

	for _, itx := range tx.InternalTxs {
		current := root
		// `TypeTraceAddress` のプレフィックス (`call` や `staticcall`) を無視
		parts := strings.Split(itx.TypeTraceAddress, "_")
		numParts := parts[1:] // ["0", "0", "1"] のみを使用
		currentPath := ""     // 初期は空文字列

		for i, part := range numParts {
			if currentPath == "" {
				currentPath = part
			} else {
				currentPath += "_" + part
			}

			if existing, exists := nodeMap[currentPath]; exists {
				current = existing
				if i == len(numParts)-1 {
					current.IsLeaf = true
				}
				continue
			}

			newNode := &CallNode{
				TypeTraceAddress:    currentPath,
				CallType:            itx.CallType,
				Sender:              itx.Sender,
				SenderIsContract:    itx.SenderIsContract,
				SenderShardID:       Addr2Shard(itx.Sender),
				Recipient:           itx.Recipient,
				RecipientIsContract: itx.RecipientIsContract,
				RecipientShardID:    Addr2Shard(itx.Recipient),
				Value:               itx.Value,
				Parent:              current,
				IsLeaf:              true,                      // 新しいノードは葉ノードとして作成
				IsProcessed:         processedMap[currentPath], // processedAddresses に一致する場合 true に設定
			}
			// 親ノードは葉ノードでなくなる
			current.IsLeaf = false
			current.Children = append(current.Children, newNode)
			nodeMap[currentPath] = newNode
			current = newNode
		}
	}

	return root
}

func Addr2Shard(addr string) int {
	last8_addr := addr
	if len(last8_addr) > 8 {
		last8_addr = last8_addr[len(last8_addr)-8:]
	}
	num, err := strconv.ParseUint(last8_addr, 16, 64)
	if err != nil {
		log.Panic(err)
	}
	return int(num) % 2
}

// TypeTraceAddressからノードを検索する
func (node *CallNode) FindNodeByTTA(target string) *CallNode {
	if node == nil {
		return nil
	}

	// 現在のノードが目的の `TypeTraceAddress` を持っている場合
	if node.TypeTraceAddress == target {
		return node
	}

	// 子ノードを再帰的に探索
	for _, child := range node.Children {
		found := child.FindNodeByTTA(target)
		if found != nil {
			return found
		}
	}

	// 見つからなかった場合
	return nil
}

// TypeTraceAddressからノードの親を検索する
func (node *CallNode) FindParentNodeByTTA(target string) *CallNode {
	if node == nil {
		return nil
	}

	// 現在のノードが目的の `TypeTraceAddress` を持っている場合
	if node.TypeTraceAddress == target {
		return node
	}

	// 子ノードを再帰的に探索
	for _, child := range node.Children {
		if child.TypeTraceAddress == target {
			return node // 親ノードを返す
		}

		// 再帰的に探索
		foundParent := child.FindParentNodeByTTA(target)
		if foundParent != nil {
			return foundParent
		}
	}

	// 見つからなかった場合
	return nil
}

func (node *CallNode) DFS() []*CallNode {
	if node == nil || node.IsProcessed {
		return []*CallNode{}
	}

	// 探索経路を格納する配列
	var path []*CallNode

	// ノードを処理済みとしてマークし、経路に追加
	node.IsProcessed = true
	path = append(path, node)
	fmt.Printf("Processing Node: %s\n", node.TypeTraceAddress)

	// 子ノードに対して再帰的にDFSを実行し、探索経路を結合
	for _, child := range node.Children {
		if !child.IsProcessed {
			childPath := child.DFS()
			path = append(path, childPath...) // 子ノードの経路を結合
			path = append(path, node)         // バックトラック時に親ノードを追加
		}
	}

	// 親ノードがルートでない場合のみ戻り経路を追加
	if node.Parent == nil {
		return path
	}

	return path
}

// PrintTree は CallNode ツリーをインデント付きで出力します
func (node *CallNode) PrintTree(level int) {
	if node == nil {
		return
	}
	indent := strings.Repeat("  ", level)
	fmt.Printf("%s%s_%s Sender: %s %d, Recepient: %s %d IsLeaf: %t IsProceed: %t\n",
		indent, node.CallType, node.TypeTraceAddress, node.Sender, node.SenderShardID, node.Recipient, node.RecipientShardID, node.IsLeaf, node.IsProcessed)
	for _, child := range node.Children {
		child.PrintTree(level + 1)
	}
}
