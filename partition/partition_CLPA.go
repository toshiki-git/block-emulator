package partition

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

// State of the Constraint Label Propagation Algorithm (CLPA)
type CLPAState struct {
	NetGraph          Graph          // Graph on which the CLPA algorithm needs to run
	PartitionMap      map[Vertex]int // Map recording partition information, which shard a node belongs to
	Edges2Shard       []int          // Number of edges adjacent to a shard, corresponding to "total weight of edges associated with label k" in the paper
	VertexsNumInShard []int          // Number of nodes within a shard
	WeightPenalty     float64        // Weight penalty, corresponding to "beta" in the paper
	MinEdges2Shard    int            // Minimum number of shard-adjacent edges, minimum "total weight of edges associated with label k"
	MaxIterations     int            // Maximum number of iterations, constraint, corresponding to "\tau" in the paper
	CrossShardEdgeNum int            // Total number of cross-shard edges
	ShardNum          int            // Number of shards
	GraphHash         []byte
	ExecutionTime     time.Duration
	UnionFind         *UnionFind // Union-Find構造体
}

func (graph *CLPAState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *CLPAState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Add a node, it needs to be assigned to a shard by default
func (cs *CLPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1 // This can be modified after batch processing the VertexsNumInShard parameter
	// Alternatively, it can be left unprocessed since the CLPA algorithm will update the latest parameters before running
}

// Add an edge, the endpoints (if they do not exist) need to be assigned to a shard by default
func (cs *CLPAState) AddEdge(u, v Vertex) {
	// If the node doesn't exist, add it. The weight is always 1.
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// Parameters like Edges2Shard can be modified after batch processing
	// Alternatively, it can be left unprocessed since the CLPA algorithm will update the latest parameters before running
}

// My Code
// Contract to Contractの時のみ呼び出される
func (cs *CLPAState) MergeContracts(u, v Vertex) Vertex {
	rootU := cs.UnionFind.Find(u.Addr)
	rootV := cs.UnionFind.Find(v.Addr)

	// 既に同じグループに属している場合はスキップ
	if rootU == rootV {
		return Vertex{}
	}

	// Union-Find構造で統合
	parentAddr := cs.UnionFind.Union(rootU, rootV)

	// マージが行われなかった場合、parentAddrはroot1のままになる
	if parentAddr == rootU && rootU != rootV {
		// マージが拒否されたので、何もしない
		return Vertex{}
	} else if parentAddr == rootV && rootU != rootV {
		// 同様にマージが拒否された場合
		return Vertex{}
	}

	parentVertex := Vertex{Addr: parentAddr}

	// EdgeSetを更新
	var vertexToDelete Vertex
	if parentAddr == rootU {
		vertexToDelete = v
	} else if parentAddr == rootV {
		vertexToDelete = u
	} else {
		log.Panic("Invalid parent address")
	}

	cs.NetGraph.UpdateGraphForPartialMerge(vertexToDelete, parentVertex)
	delete(cs.PartitionMap, vertexToDelete)

	return parentVertex
}

// Copy CLPA state
func (dst *CLPAState) CopyCLPA(src CLPAState) {
	dst.NetGraph.CopyGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Edges2Shard = make([]int, src.ShardNum)
	copy(dst.Edges2Shard, src.Edges2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.WeightPenalty = src.WeightPenalty
	dst.MinEdges2Shard = src.MinEdges2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum
}

// Print CLPA
func (cs *CLPAState) PrintCLPA() {
	cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()
}

// Calculate Wk, i.e., Edges2Shard, based on the current partition
func (cs *CLPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0
		interEdge[idx] = 0
	}

	for v, lst := range cs.NetGraph.EdgeSet {
		// Get the shard to which node v belongs
		vShard := cs.PartitionMap[v]
		for _, u := range lst {
			// Similarly, get the shard to which node u belongs
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// If nodes v and u do not belong to the same shard, increment the corresponding Edges2Shard by one
				// Only calculate the in-degree to avoid double counting
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val
	}
	cs.CrossShardEdgeNum /= 2

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2
	}
	// Update MinEdges2Shard and CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// Recalculate parameters when the shard of an account changes, faster
func (cs *CLPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Edges2Shard[new]++
			cs.Edges2Shard[old]--
		} else if neighborShard == new {
			cs.Edges2Shard[old]--
			cs.CrossShardEdgeNum--
		} else {
			cs.Edges2Shard[new]++
			cs.CrossShardEdgeNum++
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	// Update MinEdges2Shard and CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// Set parameters
func (cs *CLPAState) Init_CLPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cs.UnionFind = NewUnionFind()
}

// Initialize partition, using the last digits of the node address, ensuring no empty shards at initialization
func (cs *CLPAState) Init_Partition() {
	// Set default partition parameters
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
	cs.ComputeEdges2Shard() // Removing this will be faster, but this facilitates output (after all, Init is only executed once, so it won't be much faster)
}

// Initialize partition without empty shards
func (cs *CLPAState) Stable_Init_Partition() error {
	// Set default partition parameters
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	// Assign nodes to shards
	// NOTE: The order of nodes in the map is random, so the assignment will also be random.
	// Nodes are assigned in sequence starting from 0 using a counter (cnt), and the shard number is determined by cnt % ShardNum.
	// This ensures that nodes are evenly distributed across the shards.
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // Removing this will be faster, but this facilitates output (after all, Init is only executed once, so it won't be much faster)
	return nil
}

// Calculate the score of placing node v into uShard
func (cs *CLPAState) getShard_score(v Vertex, uShard int) float64 {
	var score float64
	// Out-degree of node v
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	// Number of edges connecting uShard and node v
	Edgesto_uShard := 0
	for _, item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1
		}
	}
	//v_outdegree: 自分の持つエッジの数
	//Edgesto_uShard: 自分の持つエッジの内、計算対象のシャードに接続しているエッジの数
	//Edgesto_uShard: v_outdegree以下になる、例えば隣接ノードが4つで、そのうち計算対象のシャードに接続しているエッジが2つの場合, 2になる
	//uShard: 計算対象のシャード
	//スコアの意味: 自分の持つエッジの数に対して、uShardに接続しているエッジの数が多いほどスコアが高くなる
	score = float64(Edgesto_uShard) / float64(v_outdegree) * (1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))
	//fmt.Printf("Addr: %s, uShard: %d, score: %f, Edgesto_uShard: %d, v_outdegree: %d\n", v.Addr, uShard, score, Edgesto_uShard, v_outdegree)
	return score
}

// CLPA partitioning algorithm returns the partition map and the number of cross-shard edges
// 戻り値は、イテレーションの中で移動したアカウントです。結局初期のシャードに割り当てられても、1→0→1なら戻り値に含まれる。
func (cs *CLPAState) CLPA_Partition() (map[string]uint64, int) {
	// ディレクトリが存在するか確認し、なければ作成する
	err := os.MkdirAll(params.ExpDataRootDir, os.ModePerm)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		return nil, 0
	}

	// ログファイルを追記モードで開く
	csvFile, err := os.OpenFile(params.ExpDataRootDir+"/graph.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening log file:", err)
		return nil, 0
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// CLPA実行前の状態を収集
	cs.ComputeEdges2Shard()
	beforeCrossShardEdgeNum := cs.CrossShardEdgeNum
	beforeVertexsNumInShard := fmt.Sprintf("%v", cs.VertexsNumInShard)
	beforeEdges2Shard := fmt.Sprintf("%v", cs.Edges2Shard)

	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)

	startTime := time.Now()

	for iter := 0; iter < cs.MaxIterations; iter += 1 { // The outer loop controls the number of iterations, constraint
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// For neighbors belonging to uShard, only calculate once
				// 同じアカウントに対してあるシャードのスコアは1回だけ計算。
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				//fmt.Printf("Addr: %s, vNowShard: %d, max_scoreShard: %d移動しました\n", v.Addr, vNowShard, max_scoreShard)
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				// Recalculate VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				// Recalculate Wk
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}

	// MergedContractを考慮して、シャードの割り当てを戻り値として含める
	for v := range cs.NetGraph.VertexSet {
		if IsUnioned := cs.UnionFind.HasBeenUnioned(v.Addr); IsUnioned {
			res[v.Addr] = uint64(cs.PartitionMap[v])
		}
	}

	endTime := time.Now()

	executionTime := endTime.Sub(startTime) // 実行時間を計算
	cs.ExecutionTime = executionTime

	cs.ComputeEdges2Shard()
	afterCrossShardEdgeNum := cs.CrossShardEdgeNum
	afterVertexsNumInShard := fmt.Sprintf("%v", cs.VertexsNumInShard)
	afterEdges2Shard := fmt.Sprintf("%v", cs.Edges2Shard)

	// 統計情報の収集
	totalVertex := CountTrueVertices(cs.NetGraph.VertexSet)
	totalEdge := CountTrueEdges(cs.NetGraph.EdgeSet)
	sumVertex := SumVertex(cs.VertexsNumInShard)

	// CSV行のデータを作成
	row := []string{
		strconv.Itoa(666),
		beforeVertexsNumInShard,
		beforeEdges2Shard,
		strconv.Itoa(beforeCrossShardEdgeNum),
		afterVertexsNumInShard,
		afterEdges2Shard,
		strconv.Itoa(afterCrossShardEdgeNum),
		strconv.Itoa(totalVertex),
		strconv.Itoa(totalEdge),
		strconv.Itoa(sumVertex),
	}

	// データをCSVに書き込み
	if err := writer.Write(row); err != nil {
		fmt.Println("Error writing to CSV file:", err)
	}

	return res, cs.CrossShardEdgeNum
}

// CLPAの頂点数を数える関数
// map[Vertex]bool 型に対応
// My code
func CountTrueVertices(vertexSet map[Vertex]bool) int {
	return len(vertexSet)
}

// CLPAのエッジ数を数える関数
// My code
func CountTrueEdges(edgeSet map[Vertex][]Vertex) int {
	totalEdges := 0
	for _, edges := range edgeSet {
		totalEdges += len(edges)
	}
	return totalEdges
}

// シャード内の頂点数の合計を計算する関数
// []int 型に対応
// My code
func SumVertex(vertexsNumInShard []int) int {
	sum := 0
	for _, num := range vertexsNumInShard {
		sum += num
	}
	return sum
}

func (cs *CLPAState) EraseEdges() {
	cs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}
