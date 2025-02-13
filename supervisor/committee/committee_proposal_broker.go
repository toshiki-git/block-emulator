package committee

import (
	"blockEmulator/broker"
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
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

// CLPA committee operations
type ProposalBrokerCommitteeModule struct {
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
	modifiedMap             map[string]uint64
	clpaLastRunningTime     time.Time
	clpaFreq                int
	IsCLPAExecuted          map[string]bool // txhash -> bool
	MergedContracts         map[string]partition.Vertex
	ReversedMergedContracts map[partition.Vertex][]string
	UnionFind               *partition.UnionFind // Union-Find構造体

	//broker related  attributes avatar
	broker             *broker.Broker
	brokerConfirm1Pool map[string]*message.Mag1Confirm
	brokerConfirm2Pool map[string]*message.Mag2Confirm
	brokerTxPool       []*core.Transaction
	brokerModuleLock   sync.Mutex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// smart contract internal transaction
	internalTxMap map[string][]*core.InternalTransaction
}

func NewProposalBrokerCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath, internalTxCsvPath string, dataNum, batchNum, clpaFrequency int) *ProposalBrokerCommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum)

	broker := new(broker.Broker)
	broker.NewBroker(nil)

	pbcm := &ProposalBrokerCommitteeModule{
		csvPath:             csvFilePath,
		internalTxCsvPath:   internalTxCsvPath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		ClpaGraph:           cg,
		ClpaGraphHistory:    make([]*partition.CLPAState, 0),
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		IsCLPAExecuted:      make(map[string]bool),
		clpaLastRunningTime: time.Time{},
		MergedContracts:     make(map[string]partition.Vertex),
		UnionFind:           partition.NewUnionFind(),
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		internalTxMap:       make(map[string][]*core.InternalTransaction),

		brokerConfirm1Pool: make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool: make(map[string]*message.Mag2Confirm),
		brokerTxPool:       make([]*core.Transaction, 0),
		broker:             broker,
	}

	pbcm.internalTxMap = LoadInternalTxsFromCSV(pbcm.internalTxCsvPath)
	pbcm.sl.Slog.Println("Internal transactionsの読み込みが完了しました。")

	return pbcm
}

// for CLPA_Broker committee, it only handle the extra CInner2CrossTx message.
func (pbcm *ProposalBrokerCommitteeModule) HandleOtherMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	if msgType != message.CInner2CrossTx {
		return
	}
	itct := new(message.InnerTx2CrossTx)
	err := json.Unmarshal(content, itct)
	if err != nil {
		log.Panic()
	}
	itxs := pbcm.dealTxByBroker(itct.Txs)
	pbcm.txSending(itxs)
}

// get shard id by address
func (pbcm *ProposalBrokerCommitteeModule) fetchModifiedMap(key string) uint64 {
	//TODO: ここでMergedContractsの複数のgoroutineでのアクセスを考慮する必要がある。senderしか見てないので下のコードは不要かも
	/* if mergedVertex, ok := pcm.ClpaGraph.MergedContracts[key]; ok {
		key = mergedVertex.Addr
	} */

	if val, ok := pbcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (pbcm *ProposalBrokerCommitteeModule) txSending(txlist []*core.Transaction) {
	// シャードごとにトランザクションを分類
	sendToShard := make(map[uint64][]*core.Transaction)         // HasContractがfalseの場合
	contractSendToShard := make(map[uint64][]*core.Transaction) // HasContractがtrueの場合

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// `CInject` メッセージの送信
			pbcm.sendInjectTransactions(sendToShard)
			// `CContractInject` メッセージの送信
			pbcm.sendContractInjectTransactions(contractSendToShard)

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
		pbcm.clpaLock.Lock()
		sendersid := pbcm.fetchModifiedMap(tx.Sender)

		// `HasContract` に応じて分類
		if tx.HasContract {
			contractSendToShard[sendersid] = append(contractSendToShard[sendersid], tx)
		} else {
			if pbcm.broker.IsBroker(tx.Sender) {
				sendersid = pbcm.fetchModifiedMap(tx.Recipient)
			}
			sendToShard[sendersid] = append(sendToShard[sendersid], tx)
		}
		pbcm.clpaLock.Unlock()
	}

	pbcm.sl.Slog.Println(len(txlist), "txs have been sent.")
}

// `CInject` 用のトランザクション送信
func (pbcm *ProposalBrokerCommitteeModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, pbcm.IpNodeTable[sid][0])
	}
}

// `CContractInject` 用のトランザクション送信
func (pbcm *ProposalBrokerCommitteeModule) sendContractInjectTransactions(contractSendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, pbcm.IpNodeTable[sid][0])
	}
}

// 2者間の送金のTXだけではなく、スマートコントラクトTXも生成
func (pbcm *ProposalBrokerCommitteeModule) data2txWithContract(data []string, nonce uint64) (*core.Transaction, bool) {
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
		if internalTxs, ok := pbcm.internalTxMap[txHash]; ok {
			tx.InternalTxs = internalTxs
			delete(pbcm.internalTxMap, txHash)
		}

		if len(tx.InternalTxs) > params.SkipThresholdInternalTx {
			pbcm.sl.Slog.Printf("Internal TXが多すぎます。txHash: %s, InternalTxs: %d\n", txHash, len(tx.InternalTxs))
			if params.IsSkipLongInternalTx == 1 {
				// pcm.sl.Slog.Println("Internal TXをスキップします。")
				return &core.Transaction{}, false
			}
		}
		return tx, true
	}

	return &core.Transaction{}, false
}

func (pbcm *ProposalBrokerCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(pbcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0
	skipTxCnt := 0

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := pbcm.data2txWithContract(data, uint64(pbcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			pbcm.nowDataNum++
		} else {
			skipTxCnt++
			continue
		}

		// batch sending condition
		if len(txlist) == int(pbcm.batchDataNum) || pbcm.nowDataNum == pbcm.dataTotalNum {
			// set the algorithm timer begins
			if pbcm.clpaLastRunningTime.IsZero() {
				pbcm.clpaLastRunningTime = time.Now()
			}

			pbcm.sl.Slog.Printf("Current Skiped txs: %d\n", skipTxCnt)
			pbcm.sl.Slog.Printf("読み込まれた txs: %d\n", pbcm.nowDataNum)
			innerTxs := pbcm.dealTxByBroker(txlist) // ctxのSendingが行われる

			pbcm.txSending(innerTxs)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			pbcm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !pbcm.clpaLastRunningTime.IsZero() && time.Since(pbcm.clpaLastRunningTime) >= time.Duration(pbcm.clpaFreq)*time.Second {
			pbcm.clpaLock.Lock()
			clpaCnt++

			emptyData := []byte{}
			if err != nil {
				log.Panic()
			}
			startCLPA_msg := message.MergeMessage(message.StartCLPA, emptyData)
			// send to worker shards
			for i := uint64(0); i < uint64(params.ShardNum); i++ {
				go networks.TcpDial(startCLPA_msg, pbcm.IpNodeTable[i][0])
			}

			pbcm.sl.Slog.Println("CLPAを開始します。")
			start := time.Now()

			if err := writePartition(pbcm.curEpoch, "before_partition_modified.txt", pbcm.ClpaGraph.PartitionMap); err != nil {
				log.Fatalf("error writing partition modified info: %v", err)
			}

			mmap, _ := pbcm.ClpaGraph.CLPA_Partition() // mmapはPartitionMapとは違って、移動するもののみを含む
			executionTime := pbcm.ClpaGraph.ExecutionTime

			// マージされたコントラクトのマップがResetされないように. 更新してからclpaMapSendする
			pbcm.MergedContracts = pbcm.ClpaGraph.UnionFind.GetParentMap()
			pbcm.ReversedMergedContracts = pbft_all.ReverseMap(pbcm.MergedContracts)
			pbcm.UnionFind = pbcm.ClpaGraph.UnionFind

			pbcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pbcm.modifiedMap[key] = val
			}

			pbcm.clpaReset()
			pbcm.clpaLock.Unlock()

			//　多分partitionのブロックがコミットされて、次のepochになるまで待つ
			for atomic.LoadInt32(&pbcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}

			pbcm.clpaLastRunningTime = time.Now()
			pbcm.sl.Slog.Println("Next CLPA epoch begins. ")
			pbcm.sl.Slog.Printf("CLPAの全体の実行時間: %v, アルゴリズム: %v, その他: %v\n", time.Since(start), executionTime, time.Since(start)-executionTime)
		}

		if pbcm.nowDataNum == pbcm.dataTotalNum {
			pbcm.sl.Slog.Println("All txs have been sent!!!!!")
			pbcm.sl.Slog.Printf("Skiped txs: %d\n", skipTxCnt)
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !pbcm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(pbcm.clpaLastRunningTime) >= time.Duration(pbcm.clpaFreq)*time.Second {
			pbcm.clpaLock.Lock()
			clpaCnt++

			emptyData := []byte{}
			if err != nil {
				log.Panic()
			}
			startCLPA_msg := message.MergeMessage(message.StartCLPA, emptyData)
			// send to worker shards
			for i := uint64(0); i < uint64(params.ShardNum); i++ {
				go networks.TcpDial(startCLPA_msg, pbcm.IpNodeTable[i][0])
			}

			pbcm.sl.Slog.Println("CLPAを開始します。")
			start := time.Now()

			// PartitionModified マップの書き込み処理
			if err := writePartition(pbcm.curEpoch, "before_partition_modified.txt", pbcm.ClpaGraph.PartitionMap); err != nil {
				log.Fatalf("error writing partition modified info: %v", err)
			}

			// VertexSetの書き込み処理
			if err := writeVertexSet(pbcm.curEpoch, "before_partition_modified.txt", pbcm.ClpaGraph.NetGraph.VertexSet); err != nil {
				log.Fatalf("error writing partition modified info: %v", err)
			}

			mmap, _ := pbcm.ClpaGraph.CLPA_Partition() // mmapはPartitionMapとは違って、移動するもののみを含む
			executionTime := pbcm.ClpaGraph.ExecutionTime

			// マージされたコントラクトのマップがResetされないように
			pbcm.MergedContracts = pbcm.ClpaGraph.UnionFind.GetParentMap()
			pbcm.ReversedMergedContracts = pbft_all.ReverseMap(pbcm.MergedContracts)
			pbcm.UnionFind = pbcm.ClpaGraph.UnionFind

			pbcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pbcm.modifiedMap[key] = val
			}

			pbcm.clpaReset()
			pbcm.clpaLock.Unlock()

			//　多分partitionのブロックがコミットされて、次のepochになるまで待つ
			for atomic.LoadInt32(&pbcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pbcm.sl.Slog.Println("Next CLPA epoch begins. ")
			pbcm.sl.Slog.Printf("CLPAの全体の実行時間: %v, アルゴリズム: %v, その他: %v\n", time.Since(start), executionTime, time.Since(start)-executionTime)
			pbcm.clpaLastRunningTime = time.Now()
		}
	}
	pbcm.sl.Slog.Printf("clpaの実行累計: %d", len(pbcm.IsCLPAExecuted))
}

func (pbcm *ProposalBrokerCommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
		MergedContracts:   pbcm.MergedContracts, // MergedContractsを追加
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, pbcm.IpNodeTable[i][0])
	}
	pbcm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")

	// PartitionModified マップの書き込み処理
	if err := writeStringPartition(pbcm.curEpoch, "partition_modified.txt", m); err != nil {
		log.Fatalf("error writing partition modified info: %v", err)
	}

	// MergedContracts マップの書き込み処理
	if err := writeMergedContracts(pbcm.curEpoch, "merged_contracts.txt", pbcm.MergedContracts); err != nil {
		log.Fatalf("error writing merged contracts info: %v", err)
	}

	// Reversed MergedContracts の書き込み処理
	reversedMap := pbft_all.ReverseMap(pbcm.MergedContracts)
	if err := writeReversedContracts("reversed_merged_contracts.txt", reversedMap); err != nil {
		log.Fatalf("error writing reversed merged contracts info: %v", err)
	}
}

func (pbcm *ProposalBrokerCommitteeModule) clpaReset() {
	pbcm.ClpaGraphHistory = append(pbcm.ClpaGraphHistory, pbcm.ClpaGraph)
	pbcm.ClpaGraph = new(partition.CLPAState)
	pbcm.ClpaGraph.Init_CLPAState(0.5, params.CLPAIterationNum, params.ShardNum)
	for key, val := range pbcm.modifiedMap {
		pbcm.ClpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
	// Resetされないように
	pbcm.ClpaGraph.UnionFind = pbcm.UnionFind
}

// handle block information when received CBlockInfo message(pbft node commited)pbftノードがコミットしたとき
func (pbcm *ProposalBrokerCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	start := time.Now()
	pbcm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	IsChangeEpoch := false

	if atomic.CompareAndSwapInt32(&pbcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		pbcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
		IsChangeEpoch = true
	}

	// ATTENTION: b.BlockBodyLengthより上に書く
	if IsChangeEpoch {
		pbcm.updateCLPAResult(b)
	}

	if b.BlockBodyLength == 0 {
		return
	}

	// add createConfirm
	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	go pbcm.createConfirm(txs)

	waitLockTime := time.Now()
	pbcm.clpaLock.Lock()
	defer pbcm.clpaLock.Unlock()

	if time.Since(waitLockTime) > 1*time.Second {
		pbcm.sl.Slog.Printf("clpaLockの待機時間は %v.\n", time.Since(waitLockTime))
	}

	graphTime := time.Now()
	pbcm.processTransactions(b.InnerShardTxs)
	pbcm.processTransactions(b.Broker1Txs)
	pbcm.processTransactions(b.CrossShardFunctionCall)
	pbcm.processTransactions(b.InnerSCTxs)

	if time.Since(graphTime) > 1*time.Second {
		pbcm.sl.Slog.Printf("グラフの処理時間は %v.\n", time.Since(graphTime))
	}

	duration := time.Since(start)
	if duration > 1*time.Second {
		pbcm.sl.Slog.Printf("シャード %d のBlockInfoMsg()の実行時間は %v.\n", b.SenderShardID, duration)
	}
}

func (pbcm *ProposalBrokerCommitteeModule) updateCLPAResult(b *message.BlockInfoMsg) {
	if b.CLPAResult == nil {
		b.CLPAResult = &partition.CLPAState{} // 必要な構造体で初期化
	}
	pbcm.sl.Slog.Println("Epochが変わったのでResultの集計")
	b.CLPAResult = pbcm.ClpaGraphHistory[b.Epoch-1]
}

func (pbcm *ProposalBrokerCommitteeModule) processTransactions(txs []*core.Transaction) {
	skipCount := 0
	for _, tx := range txs {
		if _, ok := pbcm.IsCLPAExecuted[string(tx.TxHash)]; !ok {
			pbcm.IsCLPAExecuted[string(tx.TxHash)] = true
		} else {
			skipCount++
			continue
		}

		recepient := partition.Vertex{Addr: pbcm.ClpaGraph.UnionFind.Find(tx.Recipient)}
		sender := partition.Vertex{Addr: pbcm.ClpaGraph.UnionFind.Find(tx.Sender)}

		pbcm.ClpaGraph.AddEdge(sender, recepient)
		// 内部トランザクションを処理
		for _, itx := range tx.InternalTxs {
			if pbcm.UnionFind.IsConnected(itx.Sender, itx.Recipient) {
				// Edgeを追加しない
				continue
			}
			pbcm.processInternalTx(itx)
		}
	}

	if skipCount > 0 {
		pbcm.sl.Slog.Printf("IsCrossShardFuncCallで %d 回スキップしました。\n", skipCount)
	}

}

// 内部トランザクション処理
func (pbcm *ProposalBrokerCommitteeModule) processInternalTx(itx *core.InternalTransaction) {
	itxSender := partition.Vertex{Addr: pbcm.ClpaGraph.UnionFind.Find(itx.Sender)}
	itxRecipient := partition.Vertex{Addr: pbcm.ClpaGraph.UnionFind.Find(itx.Recipient)}

	// マージされた送信者と受信者を使ってエッジを追加
	pbcm.ClpaGraph.AddEdge(itxSender, itxRecipient)

	// 両方がコントラクトの場合はマージ操作を実行
	if params.IsMerge == 1 && itx.SenderIsContract && itx.RecipientIsContract {
		// ATTENTION: MergeContractsの引数は、partition.Vertex{Addr: itx.Sender}これを使う
		pbcm.ClpaGraph.MergeContracts(partition.Vertex{Addr: itx.Sender}, partition.Vertex{Addr: itx.Recipient})
	}
}

func (pbcm *ProposalBrokerCommitteeModule) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.Mag1Confirm, 0)
	confirm2s := make([]*message.Mag2Confirm, 0)
	pbcm.brokerModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := pbcm.brokerConfirm1Pool[string(tx.TxHash)]; ok {
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := pbcm.brokerConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	pbcm.brokerModuleLock.Unlock()

	if len(confirm1s) != 0 {
		pbcm.handleTx1ConfirmMag(confirm1s)
	}

	if len(confirm2s) != 0 {
		pbcm.handleTx2ConfirmMag(confirm2s)
	}
}

func (pbcm *ProposalBrokerCommitteeModule) dealTxByBroker(txs []*core.Transaction) (innerTxs []*core.Transaction) {
	innerTxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
	for _, tx := range txs {
		if !tx.HasContract {
			pbcm.clpaLock.Lock()
			rSid := pbcm.fetchModifiedMap(tx.Recipient)
			sSid := pbcm.fetchModifiedMap(tx.Sender)
			pbcm.clpaLock.Unlock()
			//　どっちもブローカーじゃないし、受信者と送信者が違うシャード場合
			if rSid != sSid && !pbcm.broker.IsBroker(tx.Recipient) && !pbcm.broker.IsBroker(tx.Sender) {
				// Cross shard transaction. BrokerChain context
				brokerRawMeg := &message.BrokerRawMeg{
					Tx:     tx,
					Broker: pbcm.broker.BrokerAddress[0],
				}
				brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
			} else {
				// Inner shard transaction. BrokerChain context
				if pbcm.broker.IsBroker(tx.Recipient) || pbcm.broker.IsBroker(tx.Sender) {
					tx.HasBroker = true
					tx.SenderIsBroker = pbcm.broker.IsBroker(tx.Sender)
				}
				innerTxs = append(innerTxs, tx)
			}
		}
		if tx.HasContract {
			// Smart contract transaction
			innerTxs = append(innerTxs, tx)
		}
	}
	if len(brokerRawMegs) != 0 {
		// Cross shard transaction
		pbcm.handleBrokerRawMag(brokerRawMegs)
	}
	return innerTxs
}

func (pbcm *ProposalBrokerCommitteeModule) handleBrokerType1Mes(brokerType1Megs []*message.BrokerType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range brokerType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce, time.Now())
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient
		tx1.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx1.RawTxHash, ctx.TxHash)
		tx1s = append(tx1s, tx1)
		confirm1 := &message.Mag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash,
		}
		pbcm.brokerModuleLock.Lock()
		pbcm.brokerConfirm1Pool[string(tx1.TxHash)] = confirm1
		pbcm.brokerModuleLock.Unlock()
	}
	pbcm.txSending(tx1s)
	fmt.Println("BrokerType1Mes received by shard,  add brokerTx1 len ", len(tx1s))
}

func (pbcm *ProposalBrokerCommitteeModule) handleBrokerType2Mes(brokerType2Megs []*message.BrokerType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce, time.Now())
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx2.RawTxHash, ctx.TxHash)
		tx2s = append(tx2s, tx2)

		confirm2 := &message.Mag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		pbcm.brokerModuleLock.Lock()
		pbcm.brokerConfirm2Pool[string(tx2.TxHash)] = confirm2
		pbcm.brokerModuleLock.Unlock()
	}
	pbcm.txSending(tx2s)
	fmt.Println("broker tx2 add to pool len ", len(tx2s))
}

// get the digest of rawMeg
func (pbcm *ProposalBrokerCommitteeModule) getBrokerRawMagDigest(r *message.BrokerRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

// handle broker raw message(Cross shard transaction)
func (pbcm *ProposalBrokerCommitteeModule) handleBrokerRawMag(brokerRawMags []*message.BrokerRawMeg) {
	b := pbcm.broker
	brokerType1Mags := make([]*message.BrokerType1Meg, 0)
	fmt.Println("broker receive ctx ", len(brokerRawMags))
	pbcm.brokerModuleLock.Lock()
	for _, meg := range brokerRawMags {
		b.BrokerRawMegs[string(pbcm.getBrokerRawMagDigest(meg))] = meg

		brokerType1Mag := &message.BrokerType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	pbcm.brokerModuleLock.Unlock()
	pbcm.handleBrokerType1Mes(brokerType1Mags)
}

func (pbcm *ProposalBrokerCommitteeModule) handleTx1ConfirmMag(mag1confirms []*message.Mag1Confirm) {
	brokerType2Mags := make([]*message.BrokerType2Meg, 0)
	b := pbcm.broker

	fmt.Println("receive confirm  brokerTx1 len ", len(mag1confirms))
	pbcm.brokerModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.BrokerRawMegs[string(pbcm.getBrokerRawMagDigest(RawMeg))]
		if !ok {
			fmt.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))

		brokerType2Mag := &message.BrokerType2Meg{
			Broker: pbcm.broker.BrokerAddress[0],
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	pbcm.brokerModuleLock.Unlock()
	pbcm.handleBrokerType2Mes(brokerType2Mags)
}

func (pbcm *ProposalBrokerCommitteeModule) handleTx2ConfirmMag(mag2confirms []*message.Mag2Confirm) {
	b := pbcm.broker
	fmt.Println("receive confirm  brokerTx2 len ", len(mag2confirms))
	num := 0
	pbcm.brokerModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag2confirm.Tx2Hash))
		if len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]) == 2 {
			num++
		} else {
			fmt.Println(len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]))
		}
	}
	pbcm.brokerModuleLock.Unlock()
	fmt.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
}
