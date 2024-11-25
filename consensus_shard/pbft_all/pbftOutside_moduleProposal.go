package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type ProposalRelayOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
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
		prom.cfcpm.HandleContractRequest(content) // ほかのshardから受け取る
	case message.CContactResponse:
		prom.cfcpm.HandleContractResponse(content) // ほかのshardから受け取る(CContractRequestで追加されたtxがCommitされたときに呼び出される)

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
		log.Panic("handleContractInject: Unmarshal エラー", err)
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, innerTxList := prom.processContractInject(ci.Txs)

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard)

	// TxPoolにトランザクションを追加
	if len(innerTxList) > 0 {
		prom.pbftNode.CurChain.Txpool.AddTxs2Pool(innerTxList)
		prom.pbftNode.pl.Plog.Printf("handleContractInject: %d 件のトランザクションをTxPoolに追加しました。\n", len(innerTxList))
	} else {
		prom.pbftNode.pl.Plog.Println("handleContractInject: TxPoolに追加するトランザクションがありませんでした。")
	}
}

func (prom *ProposalRelayOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		// 初回のProcessedMapは空
		root := core.BuildExecutionCallTree(tx, make(map[string]bool))

		differentShardNode, hasDiffShard, processedMap := prom.DFS(root, true)

		if hasDiffShard {
			destShardID := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			visitedShards := make(map[uint64]bool)
			visitedShards[destShardID] = true
			visitedShards[prom.pbftNode.ShardID] = true

			request := &message.CrossShardFunctionRequest{
				OriginalSender:     tx.Sender,
				SourceShardID:      prom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             differentShardNode.Sender,
				Recepient:          differentShardNode.Recipient,
				Value:              differentShardNode.Value,
				MethodSignature:    "execute",
				Arguments:          []byte{0x01, 0x02, 0x03, 0x04},
				RequestID:          hex.EncodeToString(tx.TxHash[:]),
				Timestamp:          time.Now().Unix(),
				Signature:          "",
				TypeTraceAddress:   differentShardNode.TypeTraceAddress,
				Tx:                 tx,
				ProcessedMap:       processedMap,
				VisitedShards:      visitedShards,
			}
			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		} else {
			tx.IsAllInner = true
			innerTxList = append(innerTxList, tx)
		}
	}
	return requestsByShard, innerTxList
}

// 共通関数: リクエストの処理
func (prom *ProposalRelayOutsideModule) processBatchRequests(requests []*message.CrossShardFunctionRequest) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)

	for _, req := range requests {
		if req.VisitedShards == nil {
			prom.pbftNode.pl.Plog.Println("processContractRequests: VisitedShards が nil です。")
			req.VisitedShards = make(map[uint64]bool)
		}
		root := core.BuildExecutionCallTree(req.Tx, req.ProcessedMap)
		currentCallNode := root.FindNodeByTTA(req.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(req.TypeTraceAddress)
			log.Panic("processContractRequests: currentCallNode が nil です。")
		}

		if currentCallNode.IsLeaf {
			req.ProcessedMap[currentCallNode.TypeTraceAddress] = true
			destShardID := req.SourceShardID
			req.VisitedShards[destShardID] = true
			req.VisitedShards[prom.pbftNode.ShardID] = true

			response := &message.CrossShardFunctionResponse{
				OriginalSender:     req.OriginalSender,
				SourceShardID:      prom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             currentCallNode.Sender,
				Recipient:          currentCallNode.Recipient,
				Value:              currentCallNode.Value,
				RequestID:          req.RequestID, // 適切なRequestIDを設定
				StatusCode:         0,
				ResultData:         []byte(""), // 必要なら適切な結果データを設定
				Timestamp:          time.Now().Unix(),
				Signature:          "",                               // 必要なら署名を設定
				TypeTraceAddress:   currentCallNode.TypeTraceAddress, // 子のTypeTraceAddressをそのままコピー
				Tx:                 req.Tx,
				ProcessedMap:       req.ProcessedMap,
				VisitedShards:      req.VisitedShards,
			}
			responseByShard[destShardID] = append(responseByShard[destShardID], response)
		} else {
			// Leafが見つからない場合、その子からDFSを開始
			differentShardNode, hasDiffShard, processedMap := prom.DFS(currentCallNode, false)
			for k, v := range processedMap {
				req.ProcessedMap[k] = v
			}

			if hasDiffShard {
				destShardID := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
				req.VisitedShards[destShardID] = true
				req.VisitedShards[prom.pbftNode.ShardID] = true

				request := &message.CrossShardFunctionRequest{
					OriginalSender:     req.OriginalSender,
					SourceShardID:      prom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             differentShardNode.Sender,
					Recepient:          differentShardNode.Recipient,
					Value:              differentShardNode.Value,
					MethodSignature:    "execute",
					Arguments:          []byte{0x01, 0x02, 0x03, 0x04},
					RequestID:          req.RequestID,
					Timestamp:          time.Now().Unix(),
					Signature:          "",
					TypeTraceAddress:   differentShardNode.TypeTraceAddress,
					Tx:                 req.Tx,
					ProcessedMap:       req.ProcessedMap,
					VisitedShards:      req.VisitedShards,
				}
				requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
			} else {
				req.ProcessedMap[currentCallNode.TypeTraceAddress] = true
				destShardID := req.SourceShardID
				req.VisitedShards[destShardID] = true
				req.VisitedShards[prom.pbftNode.ShardID] = true

				response := &message.CrossShardFunctionResponse{
					OriginalSender:     req.OriginalSender,
					SourceShardID:      prom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             currentCallNode.Sender,
					Recipient:          currentCallNode.Recipient,
					Value:              currentCallNode.Value,
					RequestID:          req.RequestID, // 適切なRequestIDを設定
					StatusCode:         0,
					ResultData:         []byte(""), // 必要なら適切な結果データを設定
					Timestamp:          time.Now().Unix(),
					Signature:          "",                               // 必要なら署名を設定
					TypeTraceAddress:   currentCallNode.TypeTraceAddress, // 子のTypeTraceAddressをそのままコピー
					Tx:                 req.Tx,
					ProcessedMap:       req.ProcessedMap,
					VisitedShards:      req.VisitedShards,
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}
		}
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard)
	prom.sendResponses(responseByShard)
}

// 共通関数: レスポンスの処理
func (prom *ProposalRelayOutsideModule) processBatchResponses(responses []*message.CrossShardFunctionResponse) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)
	injectTxByShard := make(map[uint64][]*core.Transaction)

	for _, res := range responses {
		root := core.BuildExecutionCallTree(res.Tx, res.ProcessedMap)
		currentCallNode := root.FindParentNodeByTTA(res.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(res.TypeTraceAddress)
			log.Panic("processContractResponses: currentCallNode が nil です。")
		}

		differentShardNode, hasDiffShard, processedMap := prom.DFS(currentCallNode, false)
		for k, v := range processedMap {
			res.ProcessedMap[k] = v
		}

		if !hasDiffShard {
			// TODO: 親にレスポンスを返す。ルートの場合はトランザクションを発行
			destShardID := prom.pbftNode.CurChain.Get_PartitionMap(currentCallNode.Sender)
			res.ProcessedMap[currentCallNode.TypeTraceAddress] = true
			res.VisitedShards[destShardID] = true
			res.VisitedShards[prom.pbftNode.ShardID] = true
			if currentCallNode.CallType == "root" {
				// ルートの場合、トランザクションを発行
				if destShardID != prom.pbftNode.ShardID {
					response := &message.CrossShardFunctionResponse{
						OriginalSender:     res.OriginalSender,
						SourceShardID:      prom.pbftNode.ShardID,
						DestinationShardID: destShardID,
						Sender:             currentCallNode.Sender,
						Recipient:          currentCallNode.Recipient,
						Value:              currentCallNode.Value,
						RequestID:          res.RequestID,
						StatusCode:         0,
						ResultData:         []byte(""),
						Timestamp:          time.Now().Unix(),
						Signature:          "",
						TypeTraceAddress:   currentCallNode.TypeTraceAddress,
						Tx:                 res.Tx,
						ProcessedMap:       res.ProcessedMap,
						VisitedShards:      res.VisitedShards,
					}
					responseByShard[destShardID] = append(responseByShard[destShardID], response)
				} else {
					// txを発行する
					contractAddrByShard := make(map[uint64]map[string]struct{}) // シャードごとのコントラクトアドレスを格納

					// InternalTxsを処理してコントラクトアドレスを収集
					for _, internalTx := range res.Tx.InternalTxs {
						// Senderがコントラクトの場合
						if internalTx.SenderIsContract {
							ssid := prom.pbftNode.CurChain.Get_PartitionMap(internalTx.Sender)
							if contractAddrByShard[ssid] == nil {
								contractAddrByShard[ssid] = make(map[string]struct{})
							}
							contractAddrByShard[ssid][internalTx.Sender] = struct{}{}
						}

						// Recipientがコントラクトの場合
						if internalTx.RecipientIsContract {
							rsid := prom.pbftNode.CurChain.Get_PartitionMap(internalTx.Recipient)
							if contractAddrByShard[rsid] == nil {
								contractAddrByShard[rsid] = make(map[string]struct{})
							}
							contractAddrByShard[rsid][internalTx.Recipient] = struct{}{}
						}
					}

					// VisitedShardsを処理してトランザクションを作成・挿入
					for visitedShardID := range res.VisitedShards {
						tx := core.NewTransaction(res.OriginalSender, "0000000000000000000000000000000000000001", res.Tx.Value, 0, time.Now())
						tx.HasContract = true
						tx.IsCrossShardFuncCall = true

						// シャードに関連付けられたコントラクトアドレスを追加
						for contractAddr := range contractAddrByShard[visitedShardID] {
							tx.SmartContractAddress = append(tx.SmartContractAddress, contractAddr)
						}

						// トランザクションを対応するシャードに追加
						injectTxByShard[visitedShardID] = append(injectTxByShard[visitedShardID], tx)
					}
				}
			} else {
				// ルート以外の場合、親にレスポンスを返す
				response := &message.CrossShardFunctionResponse{
					OriginalSender:     res.OriginalSender,
					SourceShardID:      prom.pbftNode.ShardID,
					DestinationShardID: destShardID,
					Sender:             currentCallNode.Sender,
					Recipient:          currentCallNode.Recipient,
					Value:              currentCallNode.Value,
					RequestID:          res.RequestID, // 適切なRequestIDを設定
					StatusCode:         0,
					ResultData:         []byte(""), // 必要なら適切な結果データを設定
					Timestamp:          time.Now().Unix(),
					Signature:          "",                               // 必要なら署名を設定
					TypeTraceAddress:   currentCallNode.TypeTraceAddress, // 親のTypeTraceAddressをそのままコピー
					Tx:                 res.Tx,
					ProcessedMap:       res.ProcessedMap,
					VisitedShards:      res.VisitedShards,
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}

		} else {
			// 異なるシャードが見つかった場合、その子にリクエストを送信
			destShardID := prom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			request := &message.CrossShardFunctionRequest{
				OriginalSender:     res.OriginalSender,
				SourceShardID:      prom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             differentShardNode.Sender,
				Recepient:          differentShardNode.Recipient,
				Value:              differentShardNode.Value,
				MethodSignature:    "execute",
				Arguments:          []byte{0x01, 0x02, 0x03, 0x04},
				RequestID:          res.RequestID,
				Timestamp:          time.Now().Unix(),
				Signature:          "",
				TypeTraceAddress:   differentShardNode.TypeTraceAddress,
				Tx:                 res.Tx,
				ProcessedMap:       res.ProcessedMap,
				VisitedShards:      res.VisitedShards,
			}

			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		}
	}

	// シャードごとにリクエストを送信
	prom.sendRequests(requestsByShard)
	prom.sendResponses(responseByShard)
	prom.sendInjectTransactions(injectTxByShard)
}

// 共通関数: リクエストの送信
func (prom *ProposalRelayOutsideModule) sendRequests(requestsByShard map[uint64][]*message.CrossShardFunctionRequest) {
	for sid, requests := range requestsByShard {
		if sid == prom.pbftNode.ShardID {
			prom.pbftNode.pl.Plog.Println("自分自身のシャードには送信しません。DFSの処理が正しく行われていない可能性があります。")
			/* for _, req := range requests {
				fmt.Println(req)
			} */
			// continue
		}
		rByte, err := json.Marshal(requests)
		if err != nil {
			log.Panic("sendRequests: Unmarshal エラー", err)
		}
		msg_send := message.MergeMessage(message.CContractRequest, rByte)
		go networks.TcpDial(msg_send, prom.pbftNode.ip_nodeTable[sid][0])
		prom.pbftNode.pl.Plog.Printf("sendRequests(%s): Shard %d にリクエストを %d 件送信しました。\n", message.CContractRequest, sid, len(requests))
	}
}

func (prom *ProposalRelayOutsideModule) sendResponses(responsesByShard map[uint64][]*message.CrossShardFunctionResponse) {
	for sid, responses := range responsesByShard {
		/* if sid == prom.pbftNode.ShardID {
			prom.pbftNode.pl.Plog.Println("自分自身のシャードには送信しません。")
			continue
		} */
		rByte, err := json.Marshal(responses)
		if err != nil {
			log.Panic("sendRequests: Unmarshal エラー", err)
		}
		msg_send := message.MergeMessage(message.CContactResponse, rByte)
		go networks.TcpDial(msg_send, prom.pbftNode.ip_nodeTable[sid][0])
		prom.pbftNode.pl.Plog.Printf("sendRequests(%s): Shard %d にレスポンスを %d 件送信しました。\n", message.CContactResponse, sid, len(responses))
	}
}

// `CInject` 用のトランザクション送信
func (prom *ProposalRelayOutsideModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, prom.pbftNode.ip_nodeTable[sid][0])
		prom.pbftNode.pl.Plog.Printf("Shard %d に %d 件の Inject トランザクションを送信しました。\n", sid, len(txs))
	}
}

func (prom *ProposalRelayOutsideModule) StartBatchProcessing(batchSize int, interval time.Duration) {
	prom.pbftNode.pl.Plog.Println("StartBatchProcessing: 開始!!!")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		// リクエストのバッチ処理
		requestBatch := prom.cfcpm.GetAndClearRequests(batchSize)
		if len(requestBatch) > 0 {
			prom.processBatchRequests(requestBatch)
		}

		// レスポンスのバッチ処理
		responseBatch := prom.cfcpm.GetAndClearResponses(batchSize)
		if len(responseBatch) > 0 {
			prom.processBatchResponses(responseBatch)
		}
	}
}

// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
// 第2戻り値は異なるシャードが見つかった場合はtrue、見つからなかった場合はfalse
func (prom *ProposalRelayOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool, map[string]bool) {
	processedMap := make(map[string]bool) // 処理済みのTypeTraceAddressを格納するマップ

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
			processedMap[node.TypeTraceAddress] = true // TypeTraceAddressをマップに記録
		}

		// シャードが異なる部分が見つからなかった場合
		return nil, false
	}

	// checkSelf の値に応じて探索範囲を決定
	if checkSelf {
		if result, found := dfsHelper(root); found {
			return result, true, processedMap
		}
		return root, false, processedMap // シャードが異なる部分が見つからなかった場合に root を返す
	}

	// 子ノードのみを探索
	for _, child := range root.Children {
		result, found := dfsHelper(child)
		if found {
			return result, true, processedMap
		}
	}

	// シャードが異なる部分が見つからなかった場合に root を返す
	return root, false, processedMap
}
