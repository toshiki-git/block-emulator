package measure

import (
	"blockEmulator/partition"
	"fmt"
	"strconv"
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
}

func NewTestModule_CLPA() *TestModule_CLPA {
	return &TestModule_CLPA{
		epochID:           make([]int, 0),
		crossShardEdgeNum: make([]int, 0),
		vertexsNumInShard: make([][]int, 0),
		vertexNum:         make([]int, 0),
		totalVertexNum:    make([]int, 0),
		edges2Shard:       make([][]int, 0),
	}
}

func (tg *TestModule_CLPA) OutputMetricName() string {
	return "CLPA"
}

func (tg *TestModule_CLPA) UpdateMeasureRecord(cs *partition.CLPAState) {
	tg.epochID = append(tg.epochID, len(tg.epochID)+1)
	tg.vertexsNumInShard = append(tg.vertexsNumInShard, cs.VertexsNumInShard)
	tg.vertexNum = append(tg.vertexNum, sum(cs.VertexsNumInShard))
	tg.totalVertexNum = append(tg.totalVertexNum, len(cs.PartitionMap))
	tg.crossShardEdgeNum = append(tg.crossShardEdgeNum, cs.CrossShardEdgeNum)
	tg.edges2Shard = append(tg.edges2Shard, cs.Edges2Shard)
}

func (tg *TestModule_CLPA) HandleExtraMessage([]byte) {}

func (tg *TestModule_CLPA) OutputRecord() (perEpochLatency []float64, totLatency float64) {
	tg.writeToCSV()
	return nil, 0
}

func (tg *TestModule_CLPA) writeToCSV() {
	fileName := tg.OutputMetricName()
	measureName := []string{"EpochID", "crossShardEdgeNum", "vertexsNumInShard", "vertexNum", "totalVertexNum", "edges2Shard"}
	measureVals := make([][]string, 0)

	for i, eid := range tg.epochID {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.Itoa(tg.crossShardEdgeNum[i]),
			fmt.Sprint(tg.vertexsNumInShard[i]),
			strconv.Itoa(tg.vertexNum[i]),
			strconv.Itoa(tg.totalVertexNum[i]),
			fmt.Sprint(tg.edges2Shard[i]),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}

func sum(numbers []int) int {
	sum := 0
	for _, number := range numbers {
		sum += number
	}
	return sum
}
