package committee

import (
	"blockEmulator/consensus_shard/pbft_all"
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
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
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
	curEpoch                int32
	clpaLock                sync.Mutex
	ClpaGraph               *partition.CLPAState
	ClpaGraphHistory        []*partition.CLPAState
	modifiedMap             map[string]uint64 // key: address, value: shardID
	clpaLastRunningTime     time.Time
	clpaFreq                int
	MergedContracts         map[string]partition.Vertex
	ReversedMergedContracts map[partition.Vertex][]string
	UnionFind               *partition.UnionFind // Union-Find構造体

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
		ClpaGraph:           cg,
		ClpaGraphHistory:    make([]*partition.CLPAState, 0),
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		MergedContracts:     make(map[string]partition.Vertex),
		UnionFind:           partition.NewUnionFind(),
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		internalTxMap:       make(map[string][]*core.InternalTransaction),
	}

	pcm.internalTxMap = LoadInternalTxsFromCSV(pcm.internalTxCsvPath)
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
	// シャードごとにトランザクションを分類
	sendToShard := make(map[uint64][]*core.Transaction)         // HasContractがfalseの場合
	contractSendToShard := make(map[uint64][]*core.Transaction) // HasContractがtrueの場合

	for idx := 0; idx <= len(txlist); idx++ {
		// `InjectSpeed` の倍数ごと、または最後のトランザクションで送信
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// `CInject` メッセージの送信
			pcm.sendInjectTransactions(sendToShard)
			// `CContractInject` メッセージの送信
			pcm.sendContractInjectTransactions(contractSendToShard)

			// マップをリセット
			sendToShard = make(map[uint64][]*core.Transaction)
			contractSendToShard = make(map[uint64][]*core.Transaction)

			time.Sleep(time.Second)
		}

		// 最後のトランザクション処理を終了
		if idx == len(txlist) {
			break
		}

		// トランザクションの送信先シャードを計算
		tx := txlist[idx]
		senderSid := pcm.fetchModifiedMap(tx.Sender)

		// `HasContract` に応じて分類
		if tx.HasContract {
			contractSendToShard[senderSid] = append(contractSendToShard[senderSid], tx)
		} else {
			sendToShard[senderSid] = append(sendToShard[senderSid], tx)
		}
	}

	pcm.sl.Slog.Println(len(txlist), "txs have been sent.")
}

// `CInject` 用のトランザクション送信
func (pcm *ProposalCommitteeModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
	for sid, txs := range sendToShard {
		it := message.InjectTxs{
			Txs:       txs,
			ToShardID: sid,
		}
		itByte, err := json.Marshal(it)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CInject, itByte)
		// リーダーノードに送信
		go networks.TcpDial(sendMsg, pcm.IpNodeTable[sid][0])
		pcm.sl.Slog.Printf("Shard %d に %d 件の Inject トランザクションを送信しました。\n", sid, len(txs))
	}
}

// `CContractInject` 用のトランザクション送信
func (pcm *ProposalCommitteeModule) sendContractInjectTransactions(contractSendToShard map[uint64][]*core.Transaction) {
	for sid, txs := range contractSendToShard {
		cit := message.ContractInjectTxs{
			Txs:       txs,
			ToShardID: sid,
		}
		citByte, err := json.Marshal(cit)
		if err != nil {
			log.Panic(err)
		}
		sendMsg := message.MergeMessage(message.CContractInject, citByte)
		// リーダーノードに送信
		go networks.TcpDial(sendMsg, pcm.IpNodeTable[sid][0])
		pcm.sl.Slog.Printf("Shard %d に %d 件の ContractInject トランザクションを送信しました。\n", sid, len(txs))
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
		tx.HasContract = true
		if internalTxs, ok := pcm.internalTxMap[txHash]; ok {
			tx.InternalTxs = internalTxs
			delete(pcm.internalTxMap, txHash)
		}

		if len(tx.InternalTxs) > 30 {
			pcm.sl.Slog.Printf("Internal TXが多すぎます。txHash: %s, InternalTxs: %d\n", txHash, len(tx.InternalTxs))
			if params.IsSkipLongInternalTx == 1 {
				pcm.sl.Slog.Println("Internal TXをスキップします。")
				return &core.Transaction{}, false
			}
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

			// マージされたコントラクトのマップがResetされないように. 更新してからclpaMapSendする
			pcm.MergedContracts = pcm.ClpaGraph.UnionFind.GetParentMap()
			pcm.ReversedMergedContracts = pbft_all.ReverseMap(pcm.MergedContracts)
			pcm.UnionFind = pcm.ClpaGraph.UnionFind

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

			// マージされたコントラクトのマップがResetされないように
			pcm.MergedContracts = pcm.ClpaGraph.UnionFind.GetParentMap()
			pcm.ReversedMergedContracts = pbft_all.ReverseMap(pcm.MergedContracts)
			pcm.UnionFind = pcm.ClpaGraph.UnionFind

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
		line := fmt.Sprintf("%s: {Addr: %s}\n", key, vertex.Addr)
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
		line := fmt.Sprintf("{Addr: %s}: %d\n", vertex.Addr, len(addresses))
		if _, err := reversedContractsFile.WriteString(line); err != nil {
			log.Fatalf("failed to write to reversed_merged_contracts.txt: %v", err)
		}
	}
}

func (pcm *ProposalCommitteeModule) clpaReset() {
	pcm.ClpaGraphHistory = append(pcm.ClpaGraphHistory, pcm.ClpaGraph)
	pcm.ClpaGraph = new(partition.CLPAState)
	pcm.ClpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range pcm.modifiedMap {
		pcm.ClpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	// Resetされないように
	pcm.ClpaGraph.UnionFind = pcm.UnionFind
}

func (pcm *ProposalCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	start := time.Now()
	pcm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	IsChangeEpoch := false
	if atomic.CompareAndSwapInt32(&pcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		pcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
		IsChangeEpoch = true
	}
	if b.BlockBodyLength == 0 {
		return
	}

	pcm.clpaLock.Lock()
	defer pcm.clpaLock.Unlock()

	// コミットされたブロックでグラフを更新
	pcm.sl.Slog.Printf("グラフを更新開始 InnerShardTxs: %d txs, Relay2Txs: %d txs, CrossShardFuctionCallTxs: %d, InnerSCTxs: %d",
		len(b.InnerShardTxs), len(b.Relay2Txs), len(b.CrossShardFunctionCall), len(b.InnerSCTxs))

	pcm.processTransactions(b.InnerShardTxs)
	pcm.processTransactions(b.Relay2Txs)
	pcm.processTransactions(b.CrossShardFunctionCall)
	pcm.processTransactions(b.InnerSCTxs)

	duration := time.Since(start)
	pcm.sl.Slog.Printf("シャード %d のBlockInfoMsg()の実行時間は %v.\n", b.SenderShardID, duration)

	if IsChangeEpoch {
		pcm.updateCLPAResult(b)
	}
}

func (pcm *ProposalCommitteeModule) updateCLPAResult(b *message.BlockInfoMsg) {
	if b.CLPAResult == nil {
		b.CLPAResult = &partition.CLPAState{} // 必要な構造体で初期化
	}
	pcm.sl.Slog.Println("Epochが変わったのでResultの集計")
	b.CLPAResult = pcm.ClpaGraphHistory[b.Epoch-1]
}

func (pcm *ProposalCommitteeModule) processTransactions(txs []*core.Transaction) {
	for _, tx := range txs {
		recepient := partition.Vertex{Addr: pcm.ClpaGraph.UnionFind.Find(tx.Recipient)}
		sender := partition.Vertex{Addr: pcm.ClpaGraph.UnionFind.Find(tx.Sender)}

		pcm.ClpaGraph.AddEdge(sender, recepient)

		// 内部トランザクションを処理
		for _, itx := range tx.InternalTxs {
			if pcm.UnionFind.IsConnected(itx.Sender, itx.Recipient) {
				// Edgeを追加しない
				continue
			}
			pcm.processInternalTx(itx)
		}
	}
}

// 内部トランザクション処理
func (pcm *ProposalCommitteeModule) processInternalTx(itx *core.InternalTransaction) {
	itxSender := partition.Vertex{Addr: pcm.ClpaGraph.UnionFind.Find(itx.Sender)}
	itxRecipient := partition.Vertex{Addr: pcm.ClpaGraph.UnionFind.Find(itx.Recipient)}

	// マージされた送信者と受信者を使ってエッジを追加
	pcm.ClpaGraph.AddEdge(itxSender, itxRecipient)

	// 両方がコントラクトの場合はマージ操作を実行
	if params.IsMerge == 1 && itx.SenderIsContract && itx.RecipientIsContract {
		// ATTENTION: MergeContractsの引数は、partition.Vertex{Addr: itx.Sender}これを使う
		pcm.ClpaGraph.MergeContracts(partition.Vertex{Addr: itx.Sender}, partition.Vertex{Addr: itx.Recipient})
	}
}
