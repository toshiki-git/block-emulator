package measure

import (
	"blockEmulator/message"
	"strconv"
	"time"
)

// to test average TPS in this system
type TestModule_avgTPS_Relay struct {
	epochID      int
	excutedTxNum []float64 // record how many excuted txs in a epoch, maybe the cross shard tx will be calculated as a 0.5 tx

	normalTxNum []int
	relay1TxNum []int
	relay2TxNum []int

	crossShardFunctionCallTxNum []int
	innerSCTxNum                []int

	startTime         []time.Time // record when the epoch starts
	endTime           []time.Time // record when the epoch ends
	clpaExecutionTime []time.Duration
}

func NewTestModule_avgTPS_Relay() *TestModule_avgTPS_Relay {
	return &TestModule_avgTPS_Relay{
		epochID:           -1,
		excutedTxNum:      make([]float64, 0),
		startTime:         make([]time.Time, 0),
		endTime:           make([]time.Time, 0),
		clpaExecutionTime: make([]time.Duration, 0),

		normalTxNum: make([]int, 0),
		relay1TxNum: make([]int, 0),
		relay2TxNum: make([]int, 0),

		crossShardFunctionCallTxNum: make([]int, 0),
		innerSCTxNum:                make([]int, 0),
	}
}

func (tat *TestModule_avgTPS_Relay) OutputMetricName() string {
	return "Average_TPS"
}

// add the number of excuted txs, and change the time records
func (tat *TestModule_avgTPS_Relay) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.CLPAResult != nil {
		tat.clpaExecutionTime = append(tat.clpaExecutionTime, b.CLPAResult.ExecutionTime)
	}

	if b.BlockBodyLength == 0 { // empty block
		return
	}

	epochid := b.Epoch
	earliestTime := b.ProposeTime
	latestTime := b.CommitTime
	r1TxNum := len(b.Relay1Txs)
	r2TxNum := len(b.Relay2Txs)

	// extend
	for tat.epochID < epochid {
		tat.excutedTxNum = append(tat.excutedTxNum, 0)
		tat.startTime = append(tat.startTime, time.Time{})
		tat.endTime = append(tat.endTime, time.Time{})

		tat.relay1TxNum = append(tat.relay1TxNum, 0)
		tat.relay2TxNum = append(tat.relay2TxNum, 0)
		tat.normalTxNum = append(tat.normalTxNum, 0)

		tat.crossShardFunctionCallTxNum = append(tat.crossShardFunctionCallTxNum, 0)
		tat.innerSCTxNum = append(tat.innerSCTxNum, 0)

		tat.epochID++
	}

	// modify the local epoch data
	tat.excutedTxNum[epochid] += float64(r1TxNum+r2TxNum) / 2
	tat.excutedTxNum[epochid] += float64(len(b.InnerShardTxs))
	for _, tx := range b.CrossShardFunctionCall {
		tat.excutedTxNum[epochid] += 1 / float64(tx.DivisionCount)
	}
	tat.excutedTxNum[epochid] += float64(len(b.InnerSCTxs))

	tat.normalTxNum[epochid] += len(b.InnerShardTxs)
	tat.relay1TxNum[epochid] += r1TxNum
	tat.relay2TxNum[epochid] += r2TxNum

	tat.crossShardFunctionCallTxNum[epochid] += len(b.CrossShardFunctionCall)
	tat.innerSCTxNum[epochid] += len(b.InnerSCTxs)

	if tat.startTime[epochid].IsZero() || tat.startTime[epochid].After(earliestTime) {
		tat.startTime[epochid] = earliestTime
	}
	if tat.endTime[epochid].IsZero() || latestTime.After(tat.endTime[epochid]) {
		tat.endTime[epochid] = latestTime
	}
}

func (tat *TestModule_avgTPS_Relay) HandleExtraMessage([]byte) {}

// output the average TPS
func (tat *TestModule_avgTPS_Relay) OutputRecord() (perEpochTPS []float64, totalTPS float64) {
	tat.writeToCSV()

	// calculate the simple result
	perEpochTPS = make([]float64, tat.epochID+1)
	totalTxNum := 0.0
	eTime := time.Now()
	lTime := time.Time{}

	totalExecutionTime := time.Duration(0)
	for _, et := range tat.clpaExecutionTime {
		totalExecutionTime += et
	}

	for eid, exTxNum := range tat.excutedTxNum {
		timeGap := tat.endTime[eid].Sub(tat.startTime[eid]).Seconds()
		perEpochTPS[eid] = exTxNum / timeGap
		totalTxNum += exTxNum
		if eTime.After(tat.startTime[eid]) {
			eTime = tat.startTime[eid]
		}
		if tat.endTime[eid].After(lTime) {
			lTime = tat.endTime[eid]
		}
	}
	totalTPS = totalTxNum / (lTime.Sub(eTime).Seconds() + totalExecutionTime.Seconds())
	return
}

func (tat *TestModule_avgTPS_Relay) writeToCSV() {
	fileName := tat.OutputMetricName()
	measureName := []string{
		"EpochID",
		"Total tx # in this epoch",
		"Normal tx # in this epoch",
		"Relay1 tx # in this epoch",
		"Relay2 tx # in this epoch",
		"Cross Shard Function Call tx # in this epoch",
		"Inner Shard SC tx # in this epoch",
		"Epoch start time",
		"Epoch end time",
		"CLPA execution time",
		"Avg. TPS of this epoch",
	}
	measureVals := make([][]string, 0)

	for eid, exTxNum := range tat.excutedTxNum {
		timeGap := tat.endTime[eid].Sub(tat.startTime[eid]).Seconds()
		var clpaExecutionTimeStr string

		if eid < len(tat.clpaExecutionTime) {
			clpaExecutionTimeStr = tat.clpaExecutionTime[eid].String()
		} else {
			clpaExecutionTimeStr = time.Duration(0).String()
		}

		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(exTxNum, 'f', 8, 64),
			strconv.Itoa(tat.normalTxNum[eid]),
			strconv.Itoa(tat.relay1TxNum[eid]),
			strconv.Itoa(tat.relay2TxNum[eid]),
			strconv.Itoa(tat.crossShardFunctionCallTxNum[eid]),
			strconv.Itoa(tat.innerSCTxNum[eid]),
			strconv.FormatInt(tat.startTime[eid].UnixMilli(), 10),
			strconv.FormatInt(tat.endTime[eid].UnixMilli(), 10),
			clpaExecutionTimeStr,
			strconv.FormatFloat(exTxNum/timeGap, 'f', 8, 64),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
