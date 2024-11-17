package core

import (
	"fmt"
	"os"
	"testing"
)

func Test_Step(t *testing.T) {
	// トランザクションのモックデータを作成
	tx := Transaction{
		Sender:    "0xae2",
		Recipient: "0x6b7",
		InternalTxs: []*InternalTransaction{
			{TypeTraceAddress: "call_0", Sender: "0x6b7", Recipient: "0xc02"},
			{TypeTraceAddress: "call_1", Sender: "0x6b7", Recipient: "0x4fc"},
			{TypeTraceAddress: "call_1_0", Sender: "0x4fc", Recipient: "0x5ea"},
			{TypeTraceAddress: "staticcall_1_1", Sender: "0x4fc", Recipient: "0x5ea"},
			{TypeTraceAddress: "staticcall_1_2", Sender: "0x4fc", Recipient: "0xc02"},
			{TypeTraceAddress: "call_2", Sender: "0x6b7", Recipient: "0x0f4"},
			{TypeTraceAddress: "call_3", Sender: "0x6b7", Recipient: "0xfd4"},
			{TypeTraceAddress: "call_3_0", Sender: "0xfd4", Recipient: "0xc02"},
			{TypeTraceAddress: "staticcall_3_1", Sender: "0xfd4", Recipient: "0x0f4"},
			{TypeTraceAddress: "staticcall_3_2", Sender: "0xfd4", Recipient: "0xc02"},
			{TypeTraceAddress: "call_4", Sender: "0x6b7", Recipient: "0xc02"},
			{TypeTraceAddress: "call_5", Sender: "0x6b7", Recipient: "0xe3c"},
			{TypeTraceAddress: "call_5_0", Sender: "0xe3c", Recipient: "0x207"},
			{TypeTraceAddress: "staticcall_5_1", Sender: "0xe3c", Recipient: "0x207"},
			{TypeTraceAddress: "staticcall_5_2", Sender: "0xe3c", Recipient: "0xc02"},
			{TypeTraceAddress: "call_6", Sender: "0x6b7", Recipient: "0xc02"},
			{TypeTraceAddress: "call_7", Sender: "0x6b7", Recipient: "0xf1d"},
			{TypeTraceAddress: "call_7_0", Sender: "0xf1d", Recipient: "0x195"},
			{TypeTraceAddress: "staticcall_7_1", Sender: "0xf1d", Recipient: "0x195"},
			{TypeTraceAddress: "staticcall_7_2", Sender: "0xf1d", Recipient: "0xc02"},
		},
	}

	// 呼び出しツリーの構築
	root := BuildExecutionCallTree(tx)
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
	fmt.Fprintf(file, "  \"%s\" [label=\"%s\\nSender: %s\\nRecipient: %s\"];\n",
		node.TypeTraceAddress, node.TypeTraceAddress, node.Sender, node.Recipient)

	// エッジ情報の書き込み
	for _, child := range node.Children {
		fmt.Fprintf(file, "  \"%s\" -> \"%s\";\n", node.TypeTraceAddress, child.TypeTraceAddress)
		child.writeDotFile(file) // 子ノードを再帰的に処理
	}
}
