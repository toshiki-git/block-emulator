package committee

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	ClpaGraph           *partition.CLPAState
	ClpaTest            *measure.TestModule_CLPA
	modifiedMap         map[string]uint64 // key: address, value: shardID
	clpaLastRunningTime time.Time
	clpaFreq            int
	MergedContracts     map[string]partition.Vertex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// smart contract internal transaction
	internalTxMap map[string][]*core.InternalTransaction
}

func NewProposalCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath, internalTxCsvPath string, dataNum, batchNum, clpaFrequency int, clpaTest *measure.TestModule_CLPA) *ProposalCommitteeModule {
	cg := new(partition.CLPAState)
	// argument (WeightPenalty, MaxIterations, ShardNum)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)

	pcm := &ProposalCommitteeModule{
		csvPath:             csvFilePath,
		internalTxCsvPath:   internalTxCsvPath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		ClpaGraph:           cg,
		ClpaTest:            clpaTest,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		MergedContracts:     make(map[string]partition.Vertex),
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		internalTxMap:       make(map[string][]*core.InternalTransaction),
	}

	pcm.internalTxMap = pcm.LoadInternalTxsFromCSV()
	pcm.sl.Slog.Println("Internal transactionsの読み込みが完了しました。")

	return pcm
}

func (pcm *ProposalCommitteeModule) HandleOtherMessage([]byte) {}

// アドレスからシャードIDを取得、なければデフォルトの値を返す
func (pcm *ProposalCommitteeModule) fetchModifiedMap(key string) uint64 {
	//TODO: ここでMergedContractsの複数のgoroutineでのアクセスを考慮する必要がある。senderしか見てないので下のコードは不要かも
	/* if mergedVertex, ok := pcm.ClpaGraph.MergedContracts[key]; ok {
		key = mergedVertex.Addr
	} */

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
			pcm.sl.Slog.Println(len(txlist), "txs have been sent. ")
			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			pcm.Ss.StopGap_Reset()

		}

		if !pcm.clpaLastRunningTime.IsZero() && time.Since(pcm.clpaLastRunningTime) >= time.Duration(pcm.clpaFreq)*time.Second {
			pcm.clpaLock.Lock()
			clpaCnt++

			// PartitionModified マップの書き込み処理
			partitionFile, err := os.OpenFile(params.ExpDataRootDir+"/before_partition_modified.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("failed to open partition_modified.txt: %v", err)
			}

			partitionFile.WriteString(fmt.Sprintf("Epoch: %d\n", pcm.curEpoch))
			for key, value := range pcm.ClpaGraph.PartitionMap {
				line := fmt.Sprintf("%s: %d\n", key.Addr, value)
				if _, err := partitionFile.WriteString(line); err != nil {
					log.Fatalf("failed to write to partition_modified.txt: %v", err)
				}
			}
			partitionFile.Close()

			mmap, _ := pcm.ClpaGraph.CLPA_Partition() // mmapはPartitionMapとは違って、移動するもののみを含む
			pcm.ClpaTest.UpdateMeasureRecord(pcm.ClpaGraph)

			// マージされたコントラクトのマップがResetされないように. 更新してからclpaMapSendする
			pcm.MergedContracts = pcm.ClpaGraph.MergedContracts

			pcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pcm.modifiedMap[key] = val
			}

			pcm.clpaReset()
			pcm.clpaLock.Unlock()

			//　多分partitionのブロックがコミットされて、次のepochになるまで待つ
			for atomic.LoadInt32(&pcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pcm.clpaLastRunningTime = time.Now()
			pcm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if pcm.nowDataNum == pcm.dataTotalNum {
			pcm.sl.Slog.Println("All txs have been sent!!!!!")
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !pcm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(pcm.clpaLastRunningTime) >= time.Duration(pcm.clpaFreq)*time.Second {
			pcm.clpaLock.Lock()
			clpaCnt++

			// PartitionModified マップの書き込み処理
			partitionFile, err := os.OpenFile(params.ExpDataRootDir+"/before_partition_modified.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("failed to open before_partition_modified.txt: %v", err)
			}

			partitionFile.WriteString(fmt.Sprintf("Epoch: %d\n", pcm.curEpoch))
			for key, value := range pcm.ClpaGraph.PartitionMap {
				line := fmt.Sprintf("%s: %d\n", key.Addr, value)
				if _, err := partitionFile.WriteString(line); err != nil {
					log.Fatalf("failed to write to before_partition_modified.txt: %v", err)
				}
			}
			partitionFile.Close()

			// PartitionModified マップの書き込み処理
			vertexSetFile, err := os.OpenFile(params.ExpDataRootDir+"/vertexSetFile.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("failed to open vertexSetFile.txt: %v", err)
			}

			vertexSetFile.WriteString(fmt.Sprintf("Epoch: %d\n", pcm.curEpoch))
			for key, value := range pcm.ClpaGraph.NetGraph.VertexSet {
				line := fmt.Sprintf("%v: %t\n", key.Addr, value)
				if _, err := vertexSetFile.WriteString(line); err != nil {
					log.Fatalf("failed to write to vertexSetFile.txt: %v", err)
				}
			}
			vertexSetFile.Close()

			mmap, _ := pcm.ClpaGraph.CLPA_Partition() // mmapはPartitionMapとは違って、移動するもののみを含む
			pcm.ClpaTest.UpdateMeasureRecord(pcm.ClpaGraph)

			// マージされたコントラクトのマップがResetされないように
			pcm.MergedContracts = pcm.ClpaGraph.MergedContracts

			pcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pcm.modifiedMap[key] = val
			}

			pcm.clpaReset()
			pcm.clpaLock.Unlock()

			//　多分partitionのブロックがコミットされて、次のepochになるまで待つ
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
		MergedContracts:   pcm.MergedContracts, // MergedContractsを追加
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

	// PartitionModified マップの書き込み処理
	partitionFile, err := os.OpenFile(params.ExpDataRootDir+"/partition_modified.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open partition_modified.txt: %v", err)
	}
	defer partitionFile.Close()
	partitionFile.WriteString(fmt.Sprintf("Epoch: %d\n", pcm.curEpoch))
	for key, value := range m {
		line := fmt.Sprintf("%s: %d\n", key, value)
		if _, err := partitionFile.WriteString(line); err != nil {
			log.Fatalf("failed to write to partition_modified.txt: %v", err)
		}
	}

	// MergedContracts マップの書き込み処理
	mergedContractsFile, err := os.OpenFile(params.ExpDataRootDir+"/merged_contracts.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open merged_contracts.txt: %v", err)
	}
	defer mergedContractsFile.Close()
	mergedContractsFile.WriteString(fmt.Sprintf("Epoch: %d\n", pcm.curEpoch))
	for key, vertex := range pcm.MergedContracts {
		line := fmt.Sprintf("%s: {Addr: %s, IsMerged: %t}\n", key, vertex.Addr, vertex.IsMerged)
		if _, err := mergedContractsFile.WriteString(line); err != nil {
			log.Fatalf("failed to write to merged_contracts.txt: %v", err)
		}
	}

	// Reversed MergedContracts の書き込み処理
	reversedMap := pbft_all.ReverseMap(pcm.MergedContracts)
	reversedContractsFile, err := os.OpenFile(params.ExpDataRootDir+"/reversed_merged_contracts.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open reversed_merged_contracts.txt: %v", err)
	}
	defer reversedContractsFile.Close()

	for vertex, addresses := range reversedMap {
		line := fmt.Sprintf("{Addr: %s, IsMerged: %t}: %v\n", vertex.Addr, vertex.IsMerged, addresses)
		if _, err := reversedContractsFile.WriteString(line); err != nil {
			log.Fatalf("failed to write to reversed_merged_contracts.txt: %v", err)
		}
	}
}

func (pcm *ProposalCommitteeModule) clpaReset() {
	pcm.ClpaGraph = new(partition.CLPAState)
	pcm.ClpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range pcm.modifiedMap {
		pcm.ClpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	// MergedContractsがResetされないように
	pcm.ClpaGraph.MergedContracts = pcm.MergedContracts
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
	defer pcm.clpaLock.Unlock()

	// コミットされたブロックでグラフを更新
	pcm.sl.Slog.Printf("グラフを更新開始 InnerShardTxs: %d txs, Relay2Txs: %d txs", len(b.InnerShardTxs), len(b.Relay2Txs))
	pcm.processTransactions(b.InnerShardTxs)
	pcm.processTransactions(b.Relay2Txs)
}

func (pcm *ProposalCommitteeModule) processTransactions(txs []*core.Transaction) {
	for _, tx := range txs {
		if mergedVertex, ok := pcm.ClpaGraph.MergedContracts[tx.Recipient]; ok {
			pcm.ClpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, mergedVertex)
		} else {
			pcm.ClpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
		}

		// 内部トランザクションを処理
		for _, itx := range tx.InternalTxs {
			if pcm.shouldSkipInternalTx(itx) {
				// Edgeを追加しない
				continue
			}
			pcm.processInternalTx(itx)
		}
	}
}

// 内部トランザクション処理
func (pcm *ProposalCommitteeModule) processInternalTx(itx *core.InternalTransaction) {
	itxSender := pcm.getMergedVertex(itx.Sender)
	itxRecipient := pcm.getMergedVertex(itx.Recipient)

	// マージされた送信者と受信者を使ってエッジを追加
	pcm.ClpaGraph.AddEdge(itxSender, itxRecipient)

	// 両方がコントラクトの場合はマージ操作を実行
	if itx.SenderIsContract && itx.RecipientIsContract {
		// ATTENTION: MergeContractsの引数は、partition.Vertex{Addr: itx.Sender}これを使う
		pcm.ClpaGraph.MergeContracts(partition.Vertex{Addr: itx.Sender}, partition.Vertex{Addr: itx.Recipient})
	}
}

// マージされた頂点を取得
func (pcm *ProposalCommitteeModule) getMergedVertex(addr string) partition.Vertex {
	if mergedVertex, ok := pcm.ClpaGraph.MergedContracts[addr]; ok {
		return mergedVertex
	}
	return partition.Vertex{Addr: addr}
}

// 内部トランザクションをスキップするかどうかの判定
func (pcm *ProposalCommitteeModule) shouldSkipInternalTx(itx *core.InternalTransaction) bool {
	mergedU, isMergedU := pcm.ClpaGraph.MergedContracts[itx.Recipient]
	mergedV, isMergedV := pcm.ClpaGraph.MergedContracts[itx.Sender]
	return isMergedU && isMergedV && mergedU == mergedV
}

func (pcm *ProposalCommitteeModule) LoadInternalTxsFromCSV() map[string][]*core.InternalTransaction {
	file, err := os.Open(pcm.internalTxCsvPath)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	internalTxMap := make(map[string][]*core.InternalTransaction) // parentTxHash -> []*InternalTransaction

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		// バリデーション：行が9列以上あるか確認
		if len(data) < 9 {
			log.Printf("Skipping row due to insufficient columns: %v", data)
			continue
		}

		// バリデーション：parentTxHashが空でないか確認
		parentTxHash := data[2]
		if parentTxHash == "" {
			log.Printf("Skipping row due to empty parentTxHash: %v", data)
			continue
		}

		// バリデーション：typeTraceAddressが空でないか確認
		typeTraceAddress := getTraceType(data[3])
		if typeTraceAddress == "" {
			log.Printf("Skipping row due to empty typeTraceAddress: %v", data)
			continue
		}

		// バリデーション：senderとrecipientが正しい形式であるか確認
		sender := data[4]
		recipient := data[5]
		if len(sender) < 40 || len(recipient) < 40 {
			log.Printf("Skipping row due to invalid sender or recipient: %v", data)
			continue
		}

		// バリデーション：senderIsContract, recipientIsContractが1または0であるか確認
		senderIsContract := data[6] == "1"
		recipientIsContract := data[7] == "1"
		if data[6] != "1" && data[6] != "0" {
			log.Printf("Skipping row due to invalid senderIsContract: %v", data)
			continue
		}
		if data[7] != "1" && data[7] != "0" {
			log.Printf("Skipping row due to invalid recipientIsContract: %v", data)
			continue
		}

		// バリデーション：valueが正しい整数としてパースできるか確認
		valueStr := data[8]
		value, ok := new(big.Int).SetString(valueStr, 10)
		if !ok {
			log.Printf("Skipping row due to invalid value: %v", data)
			continue
		}

		// nonceを初期化
		nonce := uint64(0)

		// 内部トランザクションを作成
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
