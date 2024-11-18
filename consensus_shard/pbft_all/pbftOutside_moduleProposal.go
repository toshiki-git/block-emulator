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
		go prom.handleRelay(content)
	case message.CInject:
		go prom.handleInjectTx(content)

	// messages about CLPA
	case message.CPartitionMsg:
		prom.handlePartitionMsg(content) //TODO: ここで、MergedContractを受け取る処理を追加する
	case message.AccountState_and_TX:
		prom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		prom.handlePartitionReady(content)

	// for smart contract
	case message.CContractInject:
		go prom.handleContractInject(content) // supervisorから受け取る
	case message.CContractRequest:
		go prom.handleCotractRequest(content) // ほかのshardから受け取る
	case message.CContactResponse:
		go prom.handleContractResponse(content) // ほかのshardから受け取る(CContractRequestで追加されたtxがCommitされたときに呼び出される)

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
	prom.pbftNode.pl.Plog.Println("handleContractInject: 開始")
	ci := new(message.ContractInjectTxs)
	err := json.Unmarshal(content, ci)
	if err != nil {
		log.Panic("handleContractInject: Unmarshal エラー", err)
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, innerTxList := prom.processContractInject(ci.Txs)
	prom.pbftNode.pl.Plog.Printf("handleContractInject: シャードごとのリクエスト数: %d, TxPoolへの追加数: %d\n", len(requestsByShard), len(innerTxList))

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)

	// TxPoolにトランザクションを追加
	if len(innerTxList) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(innerTxList)
		prom.pbftNode.pl.Plog.Printf("handleContractInject: %d 件のトランザクションをTxPoolに追加しました。\n", len(innerTxList))
	}
	prom.pbftNode.pl.Plog.Println("handleContractInject: 終了")
}

func (prom *ProposalRelayOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		tx.CurrentCallNode = core.BuildExecutionCallTree(tx)
		prom.pbftNode.pl.Plog.Println(tx.InternalTxs[0].ParentTxHash, tx.InternalTxs[0].TypeTraceAddress, tx.InternalTxs[0].Sender, tx.InternalTxs[0].Recipient, tx.InternalTxs[0].Value)
		prom.pbftNode.pl.Plog.Println("Treeを表示します")
		tx.CurrentCallNode.PrintTree(0)

		differentShardNode, hasDiffShard := prom.DFS(tx.CurrentCallNode, true)

		if hasDiffShard {
			rsid := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			csfreq := prom.createRequest(differentShardNode, rsid)
			requestsByShard[rsid] = append(requestsByShard[rsid], csfreq)
		} else {
			innerTxList = append(innerTxList, tx)

		}
	}
	return requestsByShard, innerTxList
}

// ほかのシャードからのリクエストを処理する
func (prom *ProposalRelayOutsideModule) handleCotractRequest(content []byte) {
	//TODO: handleCotractRequest: リクエストを受信しました。Sender: 3b8c27038848592a51384334d8090dd869a816cb, Recipient: 0b1981a9fcc24a445de15141390d3e46da0e425c, Value: <nil>
	// Valueがnilになっているので、何かしらのエラーがあるかもしれない

	prom.pbftNode.pl.Plog.Println("handleCotractRequest: 開始")
	csfreqList := []*message.CrossShardFunctionRequest{}
	err := json.Unmarshal(content, &csfreqList)
	if err != nil {
		log.Panic("handleCotractRequest: Unmarshal エラー", err)
	}

	prom.pbftNode.pl.Plog.Printf("handleCotractRequest: Shard %d から受信したリクエスト数: %d\n", csfreqList[0].SourceShardID, len(csfreqList))

	// シャードごとのリクエストとLeafトランザクションを処理
	requestsByShard, leafTxs := prom.processContractRequests(csfreqList)
	prom.pbftNode.pl.Plog.Printf("handleCotractRequest: シャードごとのリクエスト数: %d, Leaf Tx数: %d\n", len(requestsByShard), len(leafTxs))

	// Leaf ContractのトランザクションをTxPoolに追加
	if len(leafTxs) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(leafTxs)
		prom.pbftNode.pl.Plog.Printf("handleCotractRequest: %d 件のLeaf Contract用トランザクションをTxPoolに追加しました。\n", len(leafTxs))
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)
	prom.pbftNode.pl.Plog.Println("handleCotractRequest: 終了")
}

// 共通関数: リクエストの処理
func (prom *ProposalRelayOutsideModule) processContractRequests(csfreqList []*message.CrossShardFunctionRequest) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	prom.pbftNode.pl.Plog.Println("processContractRequests: 開始")
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	leafTxs := make([]*core.Transaction, 0)

	childrenNilCount := 0
	for _, csfreq := range csfreqList {
		if csfreq.CurrentCallNode.Children == nil {
			childrenNilCount++
		}
	}
	prom.pbftNode.pl.Plog.Printf("processContractRequests: Childrenがnilの数: %d\n", childrenNilCount)

	for _, csfreq := range csfreqList {
		if csfreq.CurrentCallNode.IsLeaf {
			// Leafが見つかった場合、トランザクションを生成
			tx := core.NewTransaction(csfreq.CurrentCallNode.Sender, csfreq.CurrentCallNode.Recipient, csfreq.CurrentCallNode.Value, 0, time.Now())
			tx.IsCrossShardFuncCall = true
			tx.HasContract = true
			tx.CurrentCallNode = csfreq.CurrentCallNode
			leafTxs = append(leafTxs, tx)
		} else {
			// Leafが見つかった場合、その子からDFSを開始
			differentShardNode, hasDiffShard := prom.DFS(csfreq.CurrentCallNode, false)
			// prom.pbftNode.pl.Plog.Printf("processContractRequests: hasDiffShard: %v\n", hasDiffShard)
			if hasDiffShard {
				rsid := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
				csfRequest := prom.createRequest(differentShardNode, rsid)
				requestsByShard[rsid] = append(requestsByShard[rsid], csfRequest)
			}
		}
	}
	prom.pbftNode.pl.Plog.Printf("processContractRequests: シャードごとのリクエスト数: %d, Leaf Tx数: %d\n", len(requestsByShard), len(leafTxs))
	prom.pbftNode.pl.Plog.Println("processContractRequests: 終了")
	return requestsByShard, leafTxs
}

// コミットされたFuction Call Txsを処理する
func (prom *ProposalRelayOutsideModule) handleContractResponse(content []byte) {
	prom.pbftNode.pl.Plog.Println("handleContractResponse: 開始")
	csfresList := []*message.CrossShardFunctionResponse{}
	err := json.Unmarshal(content, &csfresList)
	if err != nil {
		log.Panic("handleContractResponse: Unmarshal エラー", err)
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, txList := prom.processContractResponses(csfresList)
	prom.pbftNode.pl.Plog.Printf("handleContractResponse: シャードごとのリクエスト数: %d, TxPoolへの追加数: %d\n", len(requestsByShard), len(txList))

	// TxPoolにトランザクションを追加
	if len(txList) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(txList)
		prom.pbftNode.pl.Plog.Printf("handleContractResponse: TxPoolに %d 件のトランザクションを追加しました。\n", len(txList))
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard, message.CContractRequest)
	prom.pbftNode.pl.Plog.Println("handleContractResponse: 終了")
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
			tx.CurrentCallNode = csfr.CurrentCallNode
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
			log.Panic("sendRequests: Unmarshal エラー", err)
		}
		msg_send := message.MergeMessage(msgType, rByte)
		go networks.TcpDial(msg_send, prom.pbftNode.ip_nodeTable[sid][0])
		prom.pbftNode.pl.Plog.Printf("sendRequests: Shard %d にリクエストを %d 件送信しました。\n", sid, len(requests))
	}
}

// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
func (prom *ProposalRelayOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool) {
	// 内部関数で再帰処理を定義
	var dfsHelper func(node *core.CallNode) (*core.CallNode, bool)
	dfsHelper = func(node *core.CallNode) (*core.CallNode, bool) {
		if node == nil || node.IsProcessed { // IsProcessed が true のノードはスキップ
			// prom.pbftNode.pl.Plog.Printf("DFS: nodeがnil、または処理済み NodeID: %v\n", node)
			return nil, false
		}

		// senderとrecipientのシャードを取得
		ssid := prom.pbftNode.CurChain.Get_PartitionMap(node.Sender)
		rsid := prom.pbftNode.CurChain.Get_PartitionMap(node.Recipient)

		// シャードが異なる場合、このノードとステータスを返す
		if ssid != rsid {
			// prom.pbftNode.pl.Plog.Printf("DFS: 異なるシャードが見つかりました SenderShard: %d, RecipientShard: %d\n", ssid, rsid)
			return node, true
		}

		// 子ノードを再帰的に探索
		allChildrenProcessed := true
		for _, child := range node.Children {
			// prom.pbftNode.pl.Plog.Printf("DFS: 子ノードを探索中 NodeID: %v\n", child)
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
			// prom.pbftNode.pl.Plog.Printf("DFS: すべての子ノードが処理済み NodeID: %v を処理済みに設定\n", node)
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
		// prom.pbftNode.pl.Plog.Printf("DFS: Root Nodeの子を探索 NodeID: %v\n", child)
		result, found := dfsHelper(child)
		if found {
			return result, true
		}
	}

	// シャードが異なる部分が見つからなかった場合に root を返す
	// prom.pbftNode.pl.Plog.Println("DFS: 異なるシャードが見つからなかったため Root を返します")
	return root, false
}
