package core

import (
	"fmt"
	"os"
	"testing"
)

func Test_Step(t *testing.T) {
	// トランザクションのモックデータを作成
	tx := Transaction{
		Sender:    "ae2",
		Recipient: "6b7",
		InternalTxs: []*InternalTransaction{
			{TypeTraceAddress: "call_0", Sender: "6b7", Recipient: "c02"},
			{TypeTraceAddress: "call_1", Sender: "6b7", Recipient: "4fc"},
			{TypeTraceAddress: "call_1_0", Sender: "4fc", Recipient: "5ea"},
			{TypeTraceAddress: "staticcall_1_1", Sender: "4fc", Recipient: "5ea"},
			{TypeTraceAddress: "staticcall_1_2", Sender: "4fc", Recipient: "c02"},
			{TypeTraceAddress: "call_2", Sender: "6b7", Recipient: "0f4"},
			{TypeTraceAddress: "call_3", Sender: "6b7", Recipient: "fd4"},
			{TypeTraceAddress: "call_3_0", Sender: "fd4", Recipient: "c02"},
			{TypeTraceAddress: "staticcall_3_1", Sender: "fd4", Recipient: "0f4"},
			{TypeTraceAddress: "staticcall_3_2", Sender: "fd4", Recipient: "c02"},
			{TypeTraceAddress: "call_4", Sender: "6b7", Recipient: "c02"},
			{TypeTraceAddress: "call_5", Sender: "6b7", Recipient: "e3c"},
			{TypeTraceAddress: "call_5_0", Sender: "e3c", Recipient: "207"},
			{TypeTraceAddress: "staticcall_5_1", Sender: "e3c", Recipient: "207"},
			{TypeTraceAddress: "staticcall_5_2", Sender: "e3c", Recipient: "c02"},
			{TypeTraceAddress: "call_6", Sender: "6b7", Recipient: "c02"},
			{TypeTraceAddress: "call_7", Sender: "6b7", Recipient: "f1d"},
			{TypeTraceAddress: "call_7_0", Sender: "f1d", Recipient: "195"},
			{TypeTraceAddress: "staticcall_7_1", Sender: "f1d", Recipient: "195"},
			{TypeTraceAddress: "staticcall_7_2", Sender: "f1d", Recipient: "c02"},
		},
	}

	// 呼び出しツリーの構築
	root := BuildExecutionCallTree(&tx, map[string]bool{"1_1": true})
	fmt.Println("=== Call Tree ===")
	root.PrintTree(0)

	// DFS探索の開始
	fmt.Println("=== DFS Traversal ===")
	traversalPath := root.DFS()
	fmt.Println("Traversal Path:")
	for _, node := range traversalPath {
		fmt.Printf("Node: %s Sender: %s, Recipient: %s\n", node.TypeTraceAddress, node.Sender, node.Recipient)
	}

	err := root.ToDotFile("call_tree.dot")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("DOT file generated successfully.")
	}
}

// ToDotFile は CallNode ツリーを .dot ファイルに出力します
func (node *CallNode) ToDotFile(filename string) error {
	// ファイルを作成
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// グラフのヘッダーを書き込み
	_, err = file.WriteString("digraph CallTree {\n")
	if err != nil {
		return err
	}

	// ノードとエッジを書き込む
	node.writeDotFile(file)

	// グラフのフッターを書き込み
	_, err = file.WriteString("}\n")
	if err != nil {
		return err
	}

	return nil
}

// writeDotFile は再帰的にノードとエッジを .dot 形式で出力します
func (node *CallNode) writeDotFile(file *os.File) {
	if node == nil {
		return
	}

	// ノード情報の書き込み
	fmt.Fprintf(file, "  \"%s\" [label=\"%s\\nSender: %s\\nRecipient: %s\\nIsLeaf: %t\"];\n",
		node.TypeTraceAddress, node.TypeTraceAddress, node.Sender, node.Recipient, node.IsLeaf)

	// エッジ情報の書き込み
	for _, child := range node.Children {
		fmt.Fprintf(file, "  \"%s\" -> \"%s\";\n", node.TypeTraceAddress, child.TypeTraceAddress)
		child.writeDotFile(file) // 子ノードを再帰的に処理
	}
}
