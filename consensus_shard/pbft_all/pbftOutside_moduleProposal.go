package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type ProposalRelayOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (prom *ProposalRelayOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		prom.handleRelay(content)
	case message.CInject:
		prom.handleInjectTx(content)

	// messages about CLPA
	case message.CPartitionMsg:
		prom.handlePartitionMsg(content) //TODO: ここで、MergedContractを受け取る処理を追加する
	case message.AccountState_and_TX:
		prom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		prom.handlePartitionReady(content)

	// for smart contract
	case message.CContractInject:
		prom.handleContractInject(content) // supervisorから受け取る
	case message.CContractRequest:
		prom.handleCotractRequest(content) // ほかのshardから受け取る
	case message.CContactResponse:
		prom.handleContractResponse(content) // ほかのshardから受け取る(CContractRequestで追加されたtxがCommitされたときに呼び出される)

	default:
	}
	return true
}

// receive relay transaction, which is for cross shard txs
func (prom *ProposalRelayOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	prom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay %d txs from shard %d, the senderSeq is %d\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID, len(relay.Txs), relay.SenderShardID, relay.SenderSeq)
	prom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	prom.pbftNode.seqMapLock.Lock()
	prom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	prom.pbftNode.seqMapLock.Unlock()
	prom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID)
}

func (prom *ProposalRelayOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	prom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	prom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", prom.pbftNode.ShardID, prom.pbftNode.NodeID, len(it.Txs))
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (prom *ProposalRelayOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}

	prom.cdm.ModifiedMap = append(prom.cdm.ModifiedMap, pm.PartitionModified)
	// TODO: ここで、MergedContractを受け取りたい
	prom.cdm.MergedContracts = append(prom.cdm.MergedContracts, pm.MergedContracts)
	prom.cdm.ReversedMergedContracts = append(prom.cdm.ReversedMergedContracts, ReverseMap(pm.MergedContracts))
	prom.pbftNode.pl.Plog.Printf("%d個のMergedContractsを受け取りました。 \n", len(pm.MergedContracts))
	prom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID)
	prom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
func (prom *ProposalRelayOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	prom.cdm.P_ReadyLock.Lock()
	prom.cdm.PartitionReady[pr.FromShard] = true
	prom.cdm.P_ReadyLock.Unlock()

	prom.pbftNode.seqMapLock.Lock()
	prom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	prom.pbftNode.seqMapLock.Unlock()

	prom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (prom *ProposalRelayOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	prom.cdm.AccountStateTx[at.FromShard] = at
	prom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID, at.FromShard)

	if len(prom.cdm.AccountStateTx) == int(prom.pbftNode.pbftChainConfig.ShardNums)-1 {
		prom.cdm.CollectLock.Lock()
		prom.cdm.CollectOver = true
		prom.cdm.CollectLock.Unlock()
		prom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID)
	}
}

// SupervisorからコントラクトTxsを受け取って、初回の処理する
func (prom *ProposalRelayOutsideModule) handleContractInject(content []byte) {
	ci := new(message.ContractInjectTxs)
	err := json.Unmarshal(content, ci)
	if err != nil {
		log.Panic()
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, innerTxList := prom.processContractInject(ci.Txs)

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)

	// TxPoolにトランザクションを追加
	if len(innerTxList) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(innerTxList)
	}
}

// ほかのシャードからのリクエストを処理する
func (prom *ProposalRelayOutsideModule) handleCotractRequest(content []byte) {
	csfreqList := []*message.CrossShardFunctionRequest{}
	err := json.Unmarshal(content, csfreqList)
	if err != nil {
		log.Panic()
	}

	// シャードごとのリクエストとLeafトランザクションを処理
	requestsByShard, leafTxs := prom.processContractRequests(csfreqList)

	// Leaf ContractのトランザクションをTxPoolに追加
	if len(leafTxs) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(leafTxs)
		prom.pbftNode.pl.Plog.Printf("%d 件のLeaf Contract用トランザクションをTxPoolに追加しました。\n", len(leafTxs))
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)
	prom.pbftNode.pl.Plog.Printf("handleCotractRequest処理が完了しました。\n")
}

// コミットされたFuction Call Txsを処理する
func (prom *ProposalRelayOutsideModule) handleContractResponse(content []byte) {
	csfresList := []*message.CrossShardFunctionResponse{}
	err := json.Unmarshal(content, csfresList)
	if err != nil {
		log.Panic()
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, txList := prom.processContractResponses(csfresList)

	// TxPoolにトランザクションを追加
	if len(txList) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(txList)
		prom.pbftNode.pl.Plog.Printf("TxPoolに %d 件のトランザクションを追加しました。\n", len(txList))
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)
}

// 共通関数: トランザクションの処理
func (prom *ProposalRelayOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		tx.CurrentCallNode = core.BuildExecutionCallTree(*tx)
		differentShardNode, hasDiffShard := prom.DFS(tx.CurrentCallNode, true)

		if hasDiffShard {
			rsid := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			csfreq := prom.createRequest(differentShardNode, rsid)
			requestsByShard[rsid] = append(requestsByShard[rsid], csfreq)
		} else {
			innerTxList = append(innerTxList, tx)
			prom.pbftNode.pl.Plog.Println("CrossShardFunctionがなく、シャード内で処理されるトランザクションでした。")
		}
	}

	return requestsByShard, innerTxList
}

// 共通関数: リクエストの処理
func (prom *ProposalRelayOutsideModule) processContractRequests(csfreqList []*message.CrossShardFunctionRequest) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	leafTxs := make([]*core.Transaction, 0)

	for _, csfreq := range csfreqList {
		if csfreq.CurrentCallNode.IsLeaf {
			// Leafが見つかった場合、トランザクションを生成
			tx := core.NewTransaction(csfreq.CurrentCallNode.Sender, csfreq.CurrentCallNode.Recipient, csfreq.CurrentCallNode.Value, 0, time.Now())
			tx.IsCrossShardFuncCall = true
			tx.HasContract = true
			leafTxs = append(leafTxs, tx)
		} else {
			// Leafが見つかった場合、その子からDFSを開始
			differentShardNode, hasDiffShard := prom.DFS(csfreq.CurrentCallNode, false)
			if hasDiffShard {
				rsid := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
				csfRequest := prom.createRequest(differentShardNode, rsid)
				requestsByShard[rsid] = append(requestsByShard[rsid], csfRequest)
			}
		}
	}

	return requestsByShard, leafTxs
}

// 共通関数: レスポンスの処理
func (prom *ProposalRelayOutsideModule) processContractResponses(csfrList []*message.CrossShardFunctionResponse) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	completedChildrenTxList := make([]*core.Transaction, 0)

	for _, csfr := range csfrList {
		differentShardNode, allChildrenProcessed := prom.DFS(csfr.CurrentCallNode, false)

		if allChildrenProcessed {
			tx := core.NewTransaction(csfr.CurrentCallNode.Sender, csfr.CurrentCallNode.Recipient, csfr.CurrentCallNode.Value, 0, time.Now())
			tx.IsCrossShardFuncCall = true
			tx.HasContract = true
			completedChildrenTxList = append(completedChildrenTxList, tx)
		} else if differentShardNode != nil {
			rsid := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			csfreq := prom.createRequest(differentShardNode, rsid)
			requestsByShard[rsid] = append(requestsByShard[rsid], csfreq)
		}
	}

	return requestsByShard, completedChildrenTxList
}

// 共通関数: リクエストの作成
func (prom *ProposalRelayOutsideModule) createRequest(differentShardNode *core.CallNode, rsid uint64) *message.CrossShardFunctionRequest {
	return &message.CrossShardFunctionRequest{
		SourceShardID:      prom.pbftNode.ShardID,
		DestinationShardID: rsid,
		Sender:             differentShardNode.Sender,
		Recepient:          differentShardNode.Recipient,
		Value:              differentShardNode.Value,
		ContractAddress:    differentShardNode.Recipient,
		MethodSignature:    "execute",
		Arguments:          []byte{0x01, 0x02, 0x03, 0x04},
		RequestID:          "0x1234567890",
		Timestamp:          time.Now().Unix(),
		Signature:          "",
		CurrentCallNode:    differentShardNode,
	}
}

// 共通関数: リクエストの送信
func (prom *ProposalRelayOutsideModule) sendRequests(requestsByShard map[uint64][]*message.CrossShardFunctionRequest, msgType message.MessageType) {
	for sid, requests := range requestsByShard {
		if sid == prom.pbftNode.ShardID {
			prom.pbftNode.pl.Plog.Println("自分自身のシャードには送信しません。DFSの処理が正しく行われていない可能性があります。")
			continue
		}
		rByte, err := json.Marshal(requests)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(msgType, rByte)
		go networks.TcpDial(msg_send, prom.pbftNode.ip_nodeTable[sid][0])
		prom.pbftNode.pl.Plog.Printf("Shard %d にリクエストを %d 件送信しました。\n", sid, len(requests))
	}
}

// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
func (prom *ProposalRelayOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool) {
	// 内部関数で再帰処理を定義
	var dfsHelper func(node *core.CallNode) (*core.CallNode, bool)
	dfsHelper = func(node *core.CallNode) (*core.CallNode, bool) {
		if node == nil || node.IsProcessed { // IsProcessed が true のノードはスキップ
			return nil, false
		}

		// senderとrecipientのシャードを取得
		ssid := prom.pbftNode.CurChain.Get_PartitionMap(node.Sender)
		rsid := prom.pbftNode.CurChain.Get_PartitionMap(node.Recipient)

		// シャードが異なる場合、このノードとステータスを返す
		if ssid != rsid {
			return node, true
		}

		// 子ノードを再帰的に探索
		allChildrenProcessed := true
		for _, child := range node.Children {
			result, found := dfsHelper(child)
			if found {
				return result, true // 異なるシャードが見つかった場合、処理を終了
			}
			// 子ノードが未処理の場合、フラグを false にする
			if !child.IsProcessed {
				allChildrenProcessed = false
			}
		}

		// 戻り処理：すべての子ノードが処理済みの場合、このノードを処理済みにする
		if allChildrenProcessed {
			node.IsProcessed = true
		}

		// シャードが異なる部分が見つからなかった場合
		return nil, false
	}

	// checkSelf の値に応じて探索範囲を決定
	if checkSelf {
		if result, found := dfsHelper(root); found {
			return result, true
		}
		return root, false // シャードが異なる部分が見つからなかった場合に root を返す
	}

	// 子ノードのみを探索
	for _, child := range root.Children {
		result, found := dfsHelper(child)
		if found {
			return result, true
		}
	}

	// シャードが異なる部分が見つからなかった場合に root を返す
	return root, false
}
