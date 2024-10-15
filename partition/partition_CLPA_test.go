package partition

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// 定数
const (
	WeightPenalty           = 0.5
	MaxIterations           = 100
	ShardNum                = 8
	AccountGraphShape       = "circle"
	SmartContractGraphShape = "box"
	IsLoadInternalTx        = true
	IsSkipContractTx        = false // コントラクトのトランザクションをスキップするかどうか
	BlockTxFilePath         = "../20000000to20249999_BlockTransaction_1000000rows.csv"
	ReadBlockNumber         = 20000816
	InternalTxFilePath      = "../20000000to20249999_InternalTransaction_1000000rows.csv"
)

// shardの色を定義(67色)
var predefinedColors = []string{
	"red", "green", "blue", "yellow", "purple", "orange", "pink",
	"cyan", "brown", "magenta", "lime", "indigo", "violet",
	"gold", "silver", "coral", "turquoise", "teal", "navy",
	"olive", "maroon", "salmon", "khaki", "plum", "orchid",
	"lavender", "beige", "mint", "chocolate", "crimson", "periwinkle",
	"peach", "apricot", "amethyst", "skyblue", "lightgreen", "aquamarine",
	"sienna", "ivory", "tan", "forestgreen", "steelblue", "slategray",
	"lightcoral", "darkcyan", "deepskyblue", "firebrick", "fuchsia", "darkgoldenrod",
	"lightseagreen", "midnightblue", "rosybrown", "dodgerblue", "darkorchid", "palegoldenrod",
	"springgreen", "tomato", "wheat", "lemonchiffon", "darkolivegreen", "mediumaquamarine",
	"hotpink", "papayawhip", "darkseagreen", "lightpink", "royalblue", "seagreen",
}

// グラフの色情報を取得
func getColorForShard(shard int) string {
	return predefinedColors[shard%len(predefinedColors)]
}

// .dotファイルへの出力
func writeGraphToDotFile(filename string, clpaState CLPAState, contractAddrs map[string]bool) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintln(file, "graph G {")
	writeNodes(file, clpaState, contractAddrs)
	writeEdges(file, clpaState)
	writeLegend(file)
	fmt.Fprintln(file, "}")
	return nil
}

// ノード情報を書き込む
func writeNodes(file *os.File, clpaState CLPAState, contractAddrs map[string]bool) {
	for v, shard := range clpaState.PartitionMap {
		label := v.Addr[len(v.Addr)-3:]
		color := getColorForShard(shard)
		shape := AccountGraphShape
		if contractAddrs[v.Addr] {
			shape = SmartContractGraphShape
		}
		fmt.Fprintf(file, "    \"%s\" [label=\"%s\", color=\"%s\", shape=%s, style=filled];\n", v.Addr, label, color, shape)
	}
}

// エッジ情報を書き込む
func writeEdges(file *os.File, clpaState CLPAState) {
	edgeSet := make(map[string]bool) // 重複を防ぐためのエッジセット
	for v, neighbors := range clpaState.NetGraph.EdgeSet {
		for _, u := range neighbors {
			// エッジのソート済みのキーを作成 (例えば "addr1--addr2" の形式)
			key := fmt.Sprintf("%s--%s", min(v.Addr, u.Addr), max(v.Addr, u.Addr))

			if !edgeSet[key] { // エッジがまだ追加されていない場合
				fmt.Fprintf(file, "    \"%s\" -- \"%s\";\n", v.Addr, u.Addr)
				edgeSet[key] = true // エッジをセットに追加
			}
		}
	}
}

// v.Addr と u.Addr の小さい方と大きい方を比較するためのヘルパー関数
func min(a, b string) string {
	if a < b {
		return a
	}
	return b
}

func max(a, b string) string {
	if a > b {
		return a
	}
	return b
}

// 凡例を書き込む
func writeLegend(file *os.File) {
	fmt.Fprintln(file, "    subgraph cluster_legend {")
	fmt.Fprintln(file, "        label = \"Shard Legend\";")
	for i := 0; i < ShardNum; i++ {
		color := getColorForShard(i)
		fmt.Fprintf(file, "        shard%d [label=\"Shard %d\", shape=box, style=filled, color=\"%s\"];\n", i, i, color)
	}
	fmt.Fprintln(file, "    }")
}

// 正しいアドレス形式かをチェックする関数
func isValidAddress(addr string) bool {
	// アドレスが "0x" で始まり、40文字の16進数であるかを確認
	match, _ := regexp.MatchString(`^0x[0-9a-fA-F]{40}$`, addr)
	return match
}

// ノードとエッジを追加
func processTxData(data [][]string, graph *Graph, contractAddrs map[string]bool) {
	for _, row := range data {
		fromAddr, toAddr := row[3], row[4]
		fromIsContract := row[6] == "1"
		toIsContract := row[7] == "1"

		// 不正なアドレスをスキップ
		if !isValidAddress(fromAddr) || !isValidAddress(toAddr) {
			fmt.Printf("Skipping invalid address: from %s, to %s\n", fromAddr, toAddr)
			continue
		}

		// コントラクトのトランザクションをスキップする場合
		if IsSkipContractTx && (fromIsContract || toIsContract) {
			continue
		}

		if fromIsContract {
			contractAddrs[fromAddr] = true
		}
		if toIsContract {
			contractAddrs[toAddr] = true
		}

		addEdgeToGraph(graph, fromAddr, toAddr)
	}
}

// ノードとエッジを追加
func processInternalTxData(data [][]string, graph *Graph, contractAddrs map[string]bool) {
	for _, row := range data {
		fromAddr, toAddr := row[4], row[5]
		fromIsContract := row[6] == "1"
		toIsContract := row[7] == "1"

		// 不正なアドレスをスキップ
		if !isValidAddress(fromAddr) || !isValidAddress(toAddr) {
			fmt.Printf("Skipping invalid address: from %s, to %s\n", fromAddr, toAddr)
			continue
		}

		if IsSkipContractTx && (fromIsContract || toIsContract) {
			continue
		}

		if fromIsContract {
			contractAddrs[fromAddr] = true
		}
		if toIsContract {
			contractAddrs[toAddr] = true
		}

		addEdgeToGraph(graph, fromAddr, toAddr)
	}
}

// グラフにエッジを追加
func addEdgeToGraph(graph *Graph, fromAddr, toAddr string) {
	fromVertex := Vertex{Addr: fromAddr}
	toVertex := Vertex{Addr: toAddr}
	graph.AddVertex(fromVertex)
	graph.AddVertex(toVertex)
	graph.AddEdge(fromVertex, toVertex)
}

// CSVファイルを blockNumber に基づいて読み込む関数
func readTxCSVUntilBlock(filename string, maxBlockNumber int) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, _ = reader.Read() // ヘッダーをスキップ

	var data [][]string
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		// blockNumber が maxBlockNumber を超えたら読み込み終了
		blockNumber := parseBlockNumber(row[0]) // blockNumber が1列目にあると仮定
		if blockNumber > maxBlockNumber {
			break
		}

		data = append(data, row)
	}
	fmt.Printf("Read %d rows from %s\n", len(data), filename)
	return data, nil
}

// blockNumber をパースするためのヘルパー関数
func parseBlockNumber(blockNumberStr string) int {
	blockNumber, _ := strconv.Atoi(blockNumberStr) // エラーは無視するか、エラーハンドリングを追加
	return blockNumber
}

// InternalTransactionを blockNumber に基づいて読み込む関数
func readInternalTxCSVUntilBlock(internalTxFile string, maxBlockNumber int) ([][]string, error) {
	file, err := os.Open(internalTxFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, _ = reader.Read() // ヘッダーをスキップ

	var data [][]string
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		// blockNumber が maxBlockNumber を超えたら読み込み終了
		blockNumber := parseBlockNumber(row[0]) // InternalTransactionで blockNumber が2列目にあると仮定
		if blockNumber > maxBlockNumber {
			break
		}

		data = append(data, row)
	}
	fmt.Printf("Read %d rows from %s\n", len(data), internalTxFile)
	return data, nil
}

// 初期シャード割り当てを表示する関数（ファイル出力対応）
func printPartitionToFile(clpaState CLPAState, label string, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "%sのシャードの割り当て:\n", label)
	for v, shard := range clpaState.PartitionMap {
		fmt.Fprintf(file, "Node %s is in shard %d\n", v.Addr, shard)
	}
	fmt.Fprintf(file, "Edges2Shard: %v\n", clpaState.Edges2Shard)
	fmt.Fprintf(file, "CrossShardEdgeNum: %d\n\n", clpaState.CrossShardEdgeNum)

	return nil
}

// CSVファイルからグラフを作成
func createGraphFromCSV(blockTxFilePath, internalTxFilePath string, maxBlockNumber int) (Graph, map[string]bool, error) {
	graph := Graph{
		VertexSet: make(map[Vertex]bool),
		EdgeSet:   make(map[Vertex][]Vertex),
	}
	contractAddrs := make(map[string]bool)

	// Block Transaction CSVの読み込み
	blockTxData, err := readTxCSVUntilBlock(blockTxFilePath, maxBlockNumber)
	if err != nil {
		return graph, contractAddrs, err
	}
	processTxData(blockTxData, &graph, contractAddrs)

	// Internal Transaction CSVの読み込み
	if IsLoadInternalTx {
		internalTxData, err := readInternalTxCSVUntilBlock(internalTxFilePath, maxBlockNumber)
		if err != nil {
			return graph, contractAddrs, err
		}
		processInternalTxData(internalTxData, &graph, contractAddrs)
	}

	return graph, contractAddrs, nil
}

// スマートコントラクトアドレスをファイルに出力する関数
func printContractAddrsToFile(contractAddrs map[string]bool, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for addr := range contractAddrs {
		fmt.Fprintf(file, "%s\n", addr)
	}
	return nil
}

// メインテスト関数
func TestCLPA_PartitionFromCSV(t *testing.T) {
	fmt.Println("Creating graph from CSV files...")
	graph, contractAddrs, err := createGraphFromCSV(BlockTxFilePath, InternalTxFilePath, ReadBlockNumber)
	if err != nil {
		t.Fatalf("Error creating graph: %v", err)
	}

	// スマートコントラクトアドレスをファイルに出力
	err = printContractAddrsToFile(contractAddrs, "smart_contract_addresses.txt")
	if err != nil {
		t.Fatalf("Error writing contract addresses to file: %v", err)
	}

	clpaState := CLPAState{NetGraph: graph}
	clpaState.Init_CLPAState(WeightPenalty, MaxIterations, ShardNum)
	clpaState.Init_Partition()

	printPartitionToFile(clpaState, "初期", "initial_partition.txt")
	err = writeGraphToDotFile("initial_partition.dot", clpaState, contractAddrs)
	if err != nil {
		t.Fatalf("Error writing .dot file: %v", err)
	}

	// 実行時間の計測開始
	start := time.Now()

	// CLPAアルゴリズムを実行
	clpaState.CLPA_Partition()

	// 実行時間の計測終了
	duration := time.Since(start)
	fmt.Printf("CLPA_Partition execution time: %v\n", duration)

	printPartitionToFile(clpaState, "CLPA後", "final_partition.txt")

	err = writeGraphToDotFile("final_partition.dot", clpaState, contractAddrs)
	if err != nil {
		t.Fatalf("Error writing .dot file: %v", err)
	}
}

// Test case 1: Neither u nor v is merged, and they are merged into a new vertex
func TestMergeContracts_NewMerge(t *testing.T) {
	state := new(CLPAState)
	state.Init_CLPAState(0.5, 100, 4) // MergedContracts の初期化を確認

	u := Vertex{Addr: "A"}
	v := Vertex{Addr: "B"}
	w := Vertex{Addr: "C"}
	x := Vertex{Addr: "D"}

	state.NetGraph.AddEdge(u, v)
	state.NetGraph.AddEdge(v, w)
	state.NetGraph.AddEdge(v, x)

	state.NetGraph.PrintGraph()

	mergedVertex := state.MergeContracts(v, w)

	fmt.Println("mergedVertex: ", mergedVertex)
	state.NetGraph.PrintGraph()
}

// Test case 2: u is already merged, and v is merged into u's merged vertex
func TestMergeContracts_UAlreadyMerged(t *testing.T) {
	state := new(CLPAState)
	// shardNumは4以上必要テストの場合、params.ShardNumを参照しているのでそれが4だから
	state.Init_CLPAState(0.5, 100, 4)

	u := Vertex{Addr: "A"}
	v := Vertex{Addr: "B"}
	mergedU := Vertex{Addr: "merged_A"}

	state.AddVertex(u)
	state.AddVertex(v)
	state.MergedContracts[u.Addr] = mergedU

	mergedVertex := state.MergeContracts(u, v)

	fmt.Println(mergedVertex)

}

// Test case 3: Both u and v are already merged, and they are merged into a new vertex
func TestMergeContracts_BothAlreadyMerged(t *testing.T) {
	state := new(CLPAState)
	state.Init_CLPAState(0.5, 100, 4)

	u := Vertex{Addr: "A"}
	v := Vertex{Addr: "B"}
	mergedU := Vertex{Addr: "merged_A"}
	mergedV := Vertex{Addr: "merged_B"}

	state.AddVertex(u)
	state.AddVertex(v)
	state.MergedContracts[u.Addr] = mergedU
	state.MergedContracts[v.Addr] = mergedV

	mergedVertex := state.MergeContracts(u, v)

	fmt.Println(mergedVertex)
}

// Test case 4: Edges are correctly updated after merging
func TestMergeContracts_UpdateEdges(t *testing.T) {
	state := new(CLPAState)
	state.Init_CLPAState(0.5, 100, 4)

	u := Vertex{Addr: "A"}
	v := Vertex{Addr: "B"}
	w := Vertex{Addr: "C"} // Neighbor of A and B

	state.AddVertex(u)
	state.AddVertex(v)
	state.AddVertex(w)

	// Add edges between u, v, and w
	state.NetGraph.AddEdge(u, w)
	state.NetGraph.AddEdge(v, w)

	// Perform merge
	mergedVertex := state.MergeContracts(u, v)

	// Check if the new merged vertex is connected to w
	neighbors := state.NetGraph.EdgeSet[mergedVertex]
	found := false
	for _, neighbor := range neighbors {
		if neighbor == w {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected merged vertex to be connected to w, but it was not")
	}

	// Check that u and v no longer exist in the graph
	if _, exists := state.NetGraph.EdgeSet[u]; exists {
		t.Errorf("Expected u to be removed from the graph, but it still exists")
	}

	if _, exists := state.NetGraph.EdgeSet[v]; exists {
		t.Errorf("Expected v to be removed from the graph, but it still exists")
	}
}

func TestMergeContracts_ComplexMerge(t *testing.T) {
	// CLPAStateの初期化
	state := new(CLPAState)
	state.Init_CLPAState(0.5, 100, 4) // MergedContracts の初期化を確認

	// 複数の頂点を作成
	a := Vertex{Addr: "A"}
	b := Vertex{Addr: "B"}
	c := Vertex{Addr: "C"}
	d := Vertex{Addr: "D"}
	e := Vertex{Addr: "E"}
	f := Vertex{Addr: "F"}

	// 初期グラフにエッジを追加
	state.NetGraph.AddEdge(a, b)
	state.NetGraph.AddEdge(a, d)
	state.NetGraph.AddEdge(b, c)
	state.NetGraph.AddEdge(b, e)
	state.NetGraph.AddEdge(c, e)
	state.NetGraph.AddEdge(d, e)
	state.NetGraph.AddEdge(e, f)

	// 初期グラフの状態を表示
	fmt.Println("Before any merges:")
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	// 1回目のマージ: A と B のマージ
	fmt.Println("Merging A and B...")
	mergedVertex1 := state.MergeContracts(a, b)
	fmt.Println("mergedVertexA_B: ", mergedVertex1)
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	// 2回目のマージ: A と E のマージ
	fmt.Println("Merging A and E...")
	mergedVertex2 := state.MergeContracts(a, e)
	fmt.Println("mergedVertexA_B_E: ", mergedVertex2)
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	/* 	// 最終的なグラフの状態を確認
	   	fmt.Println("Final graph after all merges:")
	   	state.NetGraph.PrintGraph()
	   	fmt.Printf("MergedContracts: %v\n", state.MergedContracts) */
}

func TestMergeContracts_ComplexMerge2(t *testing.T) {
	// CLPAStateの初期化
	state := new(CLPAState)
	state.Init_CLPAState(0.5, 100, 4) // MergedContracts の初期化を確認

	// 複数の頂点を作成
	a := Vertex{Addr: "A"}
	b := Vertex{Addr: "B"}
	c := Vertex{Addr: "C"}
	d := Vertex{Addr: "D"}
	e := Vertex{Addr: "E"}
	f := Vertex{Addr: "F"}

	// 初期グラフにエッジを追加
	state.NetGraph.AddEdge(a, b)
	state.NetGraph.AddEdge(a, d)
	state.NetGraph.AddEdge(b, c)
	state.NetGraph.AddEdge(b, e)
	state.NetGraph.AddEdge(c, e)
	state.NetGraph.AddEdge(d, e)
	state.NetGraph.AddEdge(e, f)

	// 初期グラフの状態を表示
	fmt.Println("Before any merges:")
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	// 1回目のマージ: A と B のマージ
	fmt.Println("Merging A and B...")
	mergedVertex1 := state.MergeContracts(a, b)
	fmt.Println("mergedVertexA_B: ", mergedVertex1)
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	// 2回目のマージ: D と E のマージ
	fmt.Println("Merging D and E...")
	mergedVertex2 := state.MergeContracts(d, e)
	fmt.Println("mergedVertexD_E: ", mergedVertex2)
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	fmt.Println("Merging A and D...")
	mergedVertex3 := state.MergeContracts(a, d)
	fmt.Println("mergedVertexD_E: ", mergedVertex3)
	state.NetGraph.PrintGraph()
	fmt.Printf("MergedContracts: %v\n", state.MergedContracts)

	/* 	// 最終的なグラフの状態を確認
	   	fmt.Println("Final graph after all merges:")
	   	state.NetGraph.PrintGraph()
	   	fmt.Printf("MergedContracts: %v\n", state.MergedContracts) */
}
