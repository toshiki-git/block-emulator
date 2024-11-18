package core

import (
	"fmt"
	"math/big"
	"strings"
)

// CallNode はスマートコントラクトの呼び出しノードを表します
type CallNode struct {
	TypeTraceAddress string
	Sender           string
	Recipient        string
	Value            *big.Int
	Children         []*CallNode
	Parent           *CallNode `json:"-"`
	IsLeaf           bool      // 葉ノードかどうかを示すフラグ
	IsProcessed      bool      // 処理済みかどうかを示すフラグ
}

// BuildExecutionCallTree は与えられたトレースリストから呼び出しツリーを構築し、root ノードを返します
func BuildExecutionCallTree(tx *Transaction) *CallNode {
	root := &CallNode{TypeTraceAddress: "root", Sender: tx.Sender, Recipient: tx.Recipient, IsLeaf: false, IsProcessed: false}
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
				TypeTraceAddress: currentPath,
				Sender:           itx.Sender,
				Recipient:        itx.Recipient,
				Value:            itx.Value,
				Parent:           current,
				IsLeaf:           true, // 新しいノードは葉ノードとして作成
				IsProcessed:      false,
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
	fmt.Printf("%s%s Sender: %s, Recepient: %s IsLeaf: %t IsProceed: %t\n",
		indent, node.TypeTraceAddress, node.Sender, node.Recipient, node.IsLeaf, node.IsProcessed)
	for _, child := range node.Children {
		child.PrintTree(level + 1)
	}
}
