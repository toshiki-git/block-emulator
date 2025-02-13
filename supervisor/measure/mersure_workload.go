package measure

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"strconv"
	"sync"
)

type TestModule_Workload struct {
	epochID      int
	txNumByShard []map[uint64]int

	lock sync.Mutex
}

func NewTestModule_Workload() *TestModule_Workload {
	return &TestModule_Workload{
		epochID:      -1,
		txNumByShard: make([]map[uint64]int, 0),
	}
}

func (tw *TestModule_Workload) OutputMetricName() string {
	return "Workload"
}

func (tw *TestModule_Workload) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	tw.lock.Lock()
	defer tw.lock.Unlock()

	epochid := b.Epoch
	shardID := b.SenderShardID

	// extend
	for tw.epochID < epochid {
		tw.txNumByShard = append(tw.txNumByShard, make(map[uint64]int))
		tw.epochID++
	}

	totalTxNum := 0

	normalTxNum := len(b.InnerShardTxs)

	broker1TxNum := len(b.Broker1Txs)
	broker2TxNum := len(b.Broker2Txs)

	relay1TxNum := len(b.Relay1Txs)
	relay2TxNum := len(b.Relay2Txs)

	crossShardFunctionCallTxNum := len(b.CrossShardFunctionCall)
	innerSCTxNum := len(b.InnerSCTxs)

	totalTxNum += normalTxNum + broker1TxNum + broker2TxNum + crossShardFunctionCallTxNum + innerSCTxNum + relay1TxNum + relay2TxNum

	tw.txNumByShard[epochid][shardID] += totalTxNum
}

func (tw *TestModule_Workload) HandleExtraMessage([]byte) {}

func (tw *TestModule_Workload) OutputRecord() (txNumByShard []float64, totTxNum float64) {
	tw.writeToCSV()
	return txNumByShard, totTxNum
}

func (tw *TestModule_Workload) writeToCSV() {
	fileName := tw.OutputMetricName()

	measureName := []string{"EpochID"}
	for i := 0; i < params.ShardNum; i++ {
		measureName = append(measureName, "#"+strconv.Itoa(i))
	}

	measureVals := make([][]string, 0)
	txSumByShard := make(map[uint64]int)

	for eid, txNumByShard := range tw.txNumByShard {
		csvLine := []string{strconv.Itoa(eid)}
		for i := 0; i < params.ShardNum; i++ {
			csvLine = append(csvLine, strconv.Itoa(txNumByShard[uint64(i)]))
			txSumByShard[uint64(i)] += txNumByShard[uint64(i)]
		}
		measureVals = append(measureVals, csvLine)
	}

	lastCsvLine := []string{"Total"}
	for i := 0; i < params.ShardNum; i++ {
		lastCsvLine = append(lastCsvLine, strconv.Itoa(txSumByShard[uint64(i)]))
	}
	measureVals = append(measureVals, lastCsvLine)
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
