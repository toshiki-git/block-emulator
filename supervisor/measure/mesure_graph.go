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
	edges2Shard       [][]int
}

func NewTestModule_CLPA() *TestModule_CLPA {
	return &TestModule_CLPA{
		epochID:           make([]int, 0),
		crossShardEdgeNum: make([]int, 0),
		vertexsNumInShard: make([][]int, 0),
		edges2Shard:       make([][]int, 0),
	}
}

func (tg *TestModule_CLPA) OutputMetricName() string {
	return "CLPA"
}

func (tg *TestModule_CLPA) UpdateMeasureRecord(cs *partition.CLPAState) {
	tg.epochID = append(tg.epochID, len(tg.epochID)+1)
	tg.vertexsNumInShard = append(tg.vertexsNumInShard, cs.VertexsNumInShard)
	tg.crossShardEdgeNum = append(tg.crossShardEdgeNum, cs.CrossShardEdgeNum)
	tg.edges2Shard = append(tg.edges2Shard, cs.Edges2Shard)
}

func (tg *TestModule_CLPA) HandleExtraMessage([]byte) {}

func (tg *TestModule_CLPA) OutputRecord() (perEpochCTXratio []float64, totCTXratio float64) {
	tg.writeToCSV()
	return []float64{}, 0
}

func (tg *TestModule_CLPA) writeToCSV() {
	fileName := tg.OutputMetricName()
	measureName := []string{"EpochID", "crossShardEdgeNum", "vertexsNumInShard", "edges2Shard"}
	measureVals := make([][]string, 0)

	for i, eid := range tg.epochID {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.Itoa(tg.crossShardEdgeNum[i]),
			fmt.Sprint(tg.vertexsNumInShard[i]),
			fmt.Sprint(tg.edges2Shard[i]),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
