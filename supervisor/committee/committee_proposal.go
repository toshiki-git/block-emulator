package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ProposalCommitteeModule struct {
	csvPath           string
	internalTxCsvPath string
	dataTotalNum      int
	nowDataNum        int
	batchDataNum      int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// smart contract internal transaction
	internalTxMap map[string][]*core.InternalTransaction
}

func NewProposalCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath, internalTxCsvPath string, dataNum, batchNum, clpaFrequency int) *ProposalCommitteeModule {
	cg := new(partition.CLPAState)
	// argument (WeightPenalty, MaxIterations, ShardNum)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)

	pcm := &ProposalCommitteeModule{
		csvPath:             csvFilePath,
		internalTxCsvPath:   internalTxCsvPath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
	}
	pcm.internalTxMap = pcm.LoadInternalTxsFromCSV()

	return pcm
}

func (pcm *ProposalCommitteeModule) HandleOtherMessage([]byte) {}

func (pcm *ProposalCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := pcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (pcm *ProposalCommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		// InjectSpeedの倍数ごとに送信
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				// send to leader node
				go networks.TcpDial(send_msg, pcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := pcm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

// 2者間の送金のTXだけではなく、スマートコントラクトTXも生成
func (pcm *ProposalCommitteeModule) data2txWithContract(data []string, nonce uint64) (*core.Transaction, bool) {
	// data[2]: txHash
	// data[3]: from e.g. 0x1234567890abcdef1234567890abcdef12345678
	// data[4]: to e.g. 0x1234567890abcdef1234567890abcdef12345678
	// data[6]: fromIsContract (0: not contract, 1: contract)
	// data[7]: toIsContract (0: not contract, 1: contract)
	// data[8]: value e.g. 1000000000000000000

	// データの各要素を変数に格納
	txHash := data[2]
	from := data[3]
	to := data[4]
	fromIsContract := data[6] == "1"
	toIsContract := data[7] == "1"
	valueStr := data[8]

	// TX for money transfer between two parties
	if !fromIsContract && !toIsContract && len(from) > 16 && len(to) > 16 && from != to {
		val, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Panic("Failed to parse value")
		}
		tx := core.NewTransaction(from[2:], to[2:], val, nonce, time.Now())
		return tx, true
	}

	// TX for smart contract
	if toIsContract && len(from) > 16 && len(to) > 16 && from != to {
		val, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Panic("Failed to parse value")
		}
		tx := core.NewTransaction(from[2:], to[2:], val, nonce, time.Now())
		// add internal transactions
		tx.RecipientIsContract = true
		if internalTxs, ok := pcm.internalTxMap[txHash]; ok {
			tx.InternalTxs = internalTxs
		}
		return tx, true
	}

	return &core.Transaction{}, false
}

func (pcm *ProposalCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(pcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := pcm.data2txWithContract(data, uint64(pcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			pcm.nowDataNum++
		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(pcm.batchDataNum) || pcm.nowDataNum == pcm.dataTotalNum {
			// set the algorithm timer begins
			if pcm.clpaLastRunningTime.IsZero() {
				pcm.clpaLastRunningTime = time.Now()
			}
			pcm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			pcm.Ss.StopGap_Reset()
		}

		if !pcm.clpaLastRunningTime.IsZero() && time.Since(pcm.clpaLastRunningTime) >= time.Duration(pcm.clpaFreq)*time.Second {
			pcm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := pcm.clpaGraph.CLPA_Partition()

			pcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pcm.modifiedMap[key] = val
			}
			pcm.clpaReset()
			pcm.clpaLock.Unlock()

			for atomic.LoadInt32(&pcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pcm.clpaLastRunningTime = time.Now()
			pcm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if pcm.nowDataNum == pcm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !pcm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(pcm.clpaLastRunningTime) >= time.Duration(pcm.clpaFreq)*time.Second {
			pcm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := pcm.clpaGraph.CLPA_Partition()
			pcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pcm.modifiedMap[key] = val
			}
			pcm.clpaReset()
			pcm.clpaLock.Unlock()

			for atomic.LoadInt32(&pcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pcm.sl.Slog.Println("Next CLPA epoch begins. ")
			pcm.clpaLastRunningTime = time.Now()
		}
	}
}

func (pcm *ProposalCommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, pcm.IpNodeTable[i][0])
	}
	pcm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (pcm *ProposalCommitteeModule) clpaReset() {
	pcm.clpaGraph = new(partition.CLPAState)
	pcm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range pcm.modifiedMap {
		pcm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (pcm *ProposalCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	pcm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&pcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		pcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	pcm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		pcm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, r2tx := range b.Relay2Txs {
		pcm.clpaGraph.AddEdge(partition.Vertex{Addr: r2tx.Sender}, partition.Vertex{Addr: r2tx.Recipient})
	}
	pcm.clpaLock.Unlock()
}

func (pcm *ProposalCommitteeModule) LoadInternalTxsFromCSV() map[string][]*core.InternalTransaction {
	file, err := os.Open(pcm.internalTxCsvPath)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	internalTxMap := make(map[string][]*core.InternalTransaction) //parentTxHash -> []*InternalTransaction

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		// `data` はCSVの各行を表すスライス
		parentTxHash := data[2] // transactionHash (CSV内のユニーク識別子)
		typeTraceAddress := getTraceType(data[3])
		sender := data[4]
		recipient := data[5]
		valueStr := data[8]
		senderIsContract := data[6] == "1"
		recipientIsContract := data[7] == "1"
		value, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Panic("Failed to parse value for internal transaction")
		}

		nonce := uint64(0)

		internalTx := core.NewInternalTransaction(sender[2:], recipient[2:], parentTxHash, typeTraceAddress, value, nonce, time.Now(), senderIsContract, recipientIsContract)

		// 内部トランザクションのリストを、元のトランザクションハッシュでマップに関連付ける
		internalTxMap[parentTxHash] = append(internalTxMap[parentTxHash], internalTx)
	}
	return internalTxMap
}

func getTraceType(input string) string {
	// "_"で分割して最初の要素を返す
	parts := strings.Split(input, "_")
	return parts[0]
}
