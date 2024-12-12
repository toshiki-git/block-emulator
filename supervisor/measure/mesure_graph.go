package measure

import (
	"blockEmulator/message"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// to test cross-transaction rate
// CLPA適用後の情報
type TestModule_CLPA struct {
	epochID           []int
	crossShardEdgeNum []int
	vertexsNumInShard [][]int
	vertexNum         []int
	totalVertexNum    []int
	edges2Shard       [][]int
	mergedVertexNum   []int
	mergedContractNum []int
	executionTime     []time.Duration

	lock sync.Mutex
}

func NewTestModule_CLPA() *TestModule_CLPA {
	return &TestModule_CLPA{
		epochID:           make([]int, 0),
		crossShardEdgeNum: make([]int, 0),
		vertexsNumInShard: make([][]int, 0),
		vertexNum:         make([]int, 0),
		totalVertexNum:    make([]int, 0),
		edges2Shard:       make([][]int, 0),
		mergedVertexNum:   make([]int, 0),
		mergedContractNum: make([]int, 0),
		executionTime:     make([]time.Duration, 0),
	}
}

func (tg *TestModule_CLPA) OutputMetricName() string {
	return "CLPA"
}

func (tg *TestModule_CLPA) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.CLPAResult == nil {
		return
	}

	tg.lock.Lock()
	defer tg.lock.Unlock()

	tg.epochID = append(tg.epochID, b.Epoch-1)
	tg.vertexsNumInShard = append(tg.vertexsNumInShard, b.CLPAResult.VertexsNumInShard)
	tg.vertexNum = append(tg.vertexNum, sum(b.CLPAResult.VertexsNumInShard))
	tg.totalVertexNum = append(tg.totalVertexNum, len(b.CLPAResult.PartitionMap))
	tg.crossShardEdgeNum = append(tg.crossShardEdgeNum, b.CLPAResult.CrossShardEdgeNum)
	tg.edges2Shard = append(tg.edges2Shard, b.CLPAResult.Edges2Shard)
	tg.mergedVertexNum = append(tg.mergedVertexNum, uniqueValueCount(b.CLPAResult.UnionFind.GetParentMap()))
	tg.mergedContractNum = append(tg.mergedContractNum, len(b.CLPAResult.UnionFind.GetParentMap()))
	tg.executionTime = append(tg.executionTime, b.CLPAResult.ExecutionTime)
}

func (tg *TestModule_CLPA) HandleExtraMessage([]byte) {}

func (tg *TestModule_CLPA) OutputRecord() (epochDurationsInSeconds []float64, averageDurationInSeconds float64) {
	if len(tg.executionTime) == 0 {
		return nil, 0
	}

	// executionTimeを秒形式で変換して格納
	epochDurationsInSeconds = make([]float64, len(tg.executionTime))
	var totalDuration time.Duration

	for i, duration := range tg.executionTime {
		seconds := duration.Seconds()
		epochDurationsInSeconds[i] = seconds
		totalDuration += duration
	}

	// 平均値を計算
	averageDurationInSeconds = totalDuration.Seconds() / float64(len(tg.executionTime))

	// CSVに書き出し
	tg.writeToCSV()
	return epochDurationsInSeconds, averageDurationInSeconds
}

func (tg *TestModule_CLPA) writeToCSV() {
	fileName := tg.OutputMetricName()
	measureName := []string{
		"EpochID",
		"crossShardEdgeNum",
		"vertexsNumInShard",
		"vertexNum",
		"totalVertexNum",
		"edges2Shard",
		"mergedVertexNum",
		"mergedContractNum",
		"executionTime",
	}

	measureVals := make([][]string, 0)

	for i, eid := range tg.epochID {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.Itoa(tg.crossShardEdgeNum[i]),
			fmt.Sprint(tg.vertexsNumInShard[i]),
			strconv.Itoa(tg.vertexNum[i]),
			strconv.Itoa(tg.totalVertexNum[i]),
			fmt.Sprint(tg.edges2Shard[i]),
			strconv.Itoa(tg.mergedVertexNum[i]),
			strconv.Itoa(tg.mergedContractNum[i]),
			tg.executionTime[i].String(),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
