package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// This module used in the blockChain using Broker mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type ProposalBrokerOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

func (pbom *ProposalBrokerOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CSeqIDinfo:
		pbom.handleSeqIDinfos(content)
	case message.CInject:
		pbom.handleInjectTx(content)

	// messages about CLPA
	case message.StartCLPA:
		pbom.handleStartCLPA()
	case message.CPartitionMsg:
		pbom.handlePartitionMsg(content)
	case message.CAccountTransferMsg_broker:
		pbom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		pbom.handlePartitionReady(content)

	// for smart contract
	case message.CContractInject:
		pbom.handleContractInject(content) // supervisorから受け取る
	case message.CContractRequest:
		pbom.cfcpm.HandleContractRequest(content) // ほかのshardから受け取る
	case message.CContactResponse:
		pbom.cfcpm.HandleContractResponse(content) // ほかのshardから受け取る(CContractRequestで追加されたtxがCommitされたときに呼び出される)

	default:
	}
	return true
}

// receive SeqIDinfo
func (pbom *ProposalBrokerOutsideModule) handleSeqIDinfos(content []byte) {
	sii := new(message.SeqIDinfo)
	err := json.Unmarshal(content, sii)
	if err != nil {
		log.Panic(err)
	}
	pbom.pbftNode.pl.Plog.Printf("S%dN%d : has received SeqIDinfo from shard %d, the senderSeq is %d\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID, sii.SenderShardID, sii.SenderSeq)
	pbom.pbftNode.seqMapLock.Lock()
	pbom.pbftNode.seqIDMap[sii.SenderShardID] = sii.SenderSeq
	pbom.pbftNode.seqMapLock.Unlock()
	pbom.pbftNode.pl.Plog.Printf("S%dN%d : has handled SeqIDinfo msg\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID)
}

func (pbom *ProposalBrokerOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	pbom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	pbom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID, len(it.Txs))
}

func (pbom *ProposalBrokerOutsideModule) handleStartCLPA() {
	fmt.Println("handleStartCLPAを受け取りました")
	pbom.pbftNode.IsStartCLPA = true
}

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (pbom *ProposalBrokerOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	pbom.cdm.ModifiedMap = append(pbom.cdm.ModifiedMap, pm.PartitionModified)
	// TODO: ここで、MergedContractを受け取りたい
	pbom.cdm.MergedContracts = append(pbom.cdm.MergedContracts, pm.MergedContracts)
	pbom.cdm.ReversedMergedContracts = append(pbom.cdm.ReversedMergedContracts, ReverseMap(pm.MergedContracts))
	pbom.pbftNode.pl.Plog.Printf("%d 個のMergedContractsを受け取りました。 \n", len(pm.MergedContracts))
	pbom.pbftNode.pl.Plog.Printf("S%dN%d : 次のProposeはPartitionブロックが生成されます\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID)
	pbom.pbftNode.IsStartCLPA = false
	pbom.cdm.PartitionOn = true
}

// wait for other shards' last rounds are over
func (pbom *ProposalBrokerOutsideModule) handlePartitionReady(content []byte) {
	pr := new(message.PartitionReady)
	err := json.Unmarshal(content, pr)
	if err != nil {
		log.Panic()
	}
	pbom.cdm.P_ReadyLock.Lock()
	pbom.cdm.PartitionReady[pr.FromShard] = true
	pbom.cdm.P_ReadyLock.Unlock()

	pbom.pbftNode.seqMapLock.Lock()
	pbom.cdm.ReadySeq[pr.FromShard] = pr.NowSeqID
	pbom.pbftNode.seqMapLock.Unlock()

	pbom.pbftNode.pl.Plog.Printf("シャード %d のPartition準備は完了しました, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (pbom *ProposalBrokerOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}

	pbom.cdm.AccountStateTxLock.Lock()
	pbom.cdm.AccountStateTx[at.FromShard] = at
	pbom.cdm.AccountStateTxLock.Unlock()

	pbom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID, at.FromShard)

	if len(pbom.cdm.AccountStateTx) == int(pbom.pbftNode.pbftChainConfig.ShardNums)-1 {
		pbom.cdm.CollectLock.Lock()
		pbom.cdm.CollectOver = true
		pbom.cdm.CollectLock.Unlock()
		pbom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID)
	}
}

// SupervisorからコントラクトTxsを受け取って、初回の処理する
func (pbom *ProposalBrokerOutsideModule) handleContractInject(content []byte) {
	ci := new(message.ContractInjectTxs)
	err := json.Unmarshal(content, ci)
	if err != nil {
		log.Panic("handleContractInject: Unmarshal エラー", err)
	}

	// シャードごとのリクエストとTxPoolに追加するリストを処理
	requestsByShard, innerTxList := pbom.processContractInject(ci.Txs)

	// シャードごとにリクエストを送信
	pbom.sendRequests(requestsByShard)

	// TxPoolにトランザクションを追加
	if len(innerTxList) > 0 {
		pbom.pbftNode.CurChain.Txpool.AddTxs2Front(innerTxList)
		pbom.pbftNode.pl.Plog.Printf("handleContractInject: %d 件のトランザクションをTxPoolに追加しました。\n", len(innerTxList))
	} else {
		pbom.pbftNode.pl.Plog.Println("handleContractInject: TxPoolに追加するトランザクションがありませんでした。")
	}
}

func (pbom *ProposalBrokerOutsideModule) processContractInject(txs []*core.Transaction) (map[uint64][]*message.CrossShardFunctionRequest, []*core.Transaction) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	innerTxList := make([]*core.Transaction, 0)

	for _, tx := range txs {
		// 初回のProcessedMapは空
		root := core.BuildExecutionCallTree(tx, make(map[string]bool))

		differentShardNode, hasDiffShard, processedMap := pbom.DFS(root, true)

		if hasDiffShard {
			destShardID := pbom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

			request := &message.CrossShardFunctionRequest{
				OriginalSender:     tx.Sender,
				SourceShardID:      pbom.pbftNode.ShardID,
				DestinationShardID: destShardID,
				Sender:             differentShardNode.Sender,
				Recepient:          differentShardNode.Recipient,
				Value:              differentShardNode.Value,
				MethodSignature:    "execute",
				Arguments:          []byte{0x01, 0x02, 0x03, 0x04},
				RequestID:          string(tx.TxHash),
				Timestamp:          time.Now().Unix(),
				Signature:          "",
				TypeTraceAddress:   differentShardNode.TypeTraceAddress,
				Tx:                 tx,
				ProcessedMap:       processedMap,
			}
			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		} else {
			tx.IsAllInner = true
			tx.DivisionCount = 1
			tx.StateChangeAccounts[tx.Sender] = &core.Account{AcAddress: tx.Sender, IsContract: false}
			tx.StateChangeAccounts[tx.Recipient] = &core.Account{AcAddress: tx.Recipient, IsContract: true}
			for _, itx := range tx.InternalTxs {
				switch itx.CallType {
				case "call", "create":
					tx.StateChangeAccounts[itx.Recipient] = &core.Account{AcAddress: itx.Recipient, IsContract: itx.RecipientIsContract}
				case "delegatecall":
					tx.StateChangeAccounts[itx.Sender] = &core.Account{AcAddress: itx.Sender, IsContract: itx.SenderIsContract}
				case "staticcall", "suicide":
					// 状態を変更しないので、何もしない
				default:
					fmt.Println("CallTypeが分類できません", itx.CallType)
				}
			}
			innerTxList = append(innerTxList, tx)
		}
	}
	return requestsByShard, innerTxList
}

// 共通関数: リクエストの処理
func (pbom *ProposalBrokerOutsideModule) processBatchRequests(requests []*message.CrossShardFunctionRequest) {
	requestsByShard := make(map[uint64][]*message.CrossShardFunctionRequest)
	responseByShard := make(map[uint64][]*message.CrossShardFunctionResponse)
	skipCount := 0

	for _, req := range requests {
		root := core.BuildExecutionCallTree(req.Tx, req.ProcessedMap)
		currentCallNode := root.FindNodeByTTA(req.TypeTraceAddress)

		if currentCallNode == nil {
			fmt.Println(req.TypeTraceAddress)
			log.Panic("processContractRequests: currentCallNode が nil です。")
		}

		if currentCallNode.IsLeaf {
			randomAccountInSC := GenerateRandomString(params.AccountNumInContract)
			isAccountLocked := pbom.cfcpm.SClock.IsAccountLocked(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
			if isAccountLocked {
				skipCount++
				pbom.cfcpm.AddRequest(req)
				fmt.Println("ロックされてます", currentCallNode.Recipient, randomAccountInSC, req.RequestID)
				continue
			}

			switch currentCallNode.CallType {
			case "call":
				err := pbom.cfcpm.SClock.LockAccount(currentCallNode.Recipient, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			case "staticcall":
				// 何かしらの処理
			case "delegatecall":
				err := pbom.cfcpm.SClock.LockAccount(currentCallNode.Sender, randomAccountInSC, req.RequestID)
				if err != nil {
					fmt.Println(err)
				}
			default:
				// 何かしらの処理
			}

			destShardID := req.SourceShardID
			req.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			response := &message.CrossShardFunctionResponse{
				OriginalSender:     req.OriginalSender,
				SourceShardID:      pbom.pbftNode.ShardID,
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
			}
			responseByShard[destShardID] = append(responseByShard[destShardID], response)
		} else {
			// Leafが見つからない場合、その子からDFSを開始
			differentShardNode, hasDiffShard, processedMap := pbom.DFS(currentCallNode, false)
			for k, v := range processedMap {
				req.ProcessedMap[k] = v
			}

			if hasDiffShard {
				destShardID := pbom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)

				request := &message.CrossShardFunctionRequest{
					OriginalSender:     req.OriginalSender,
					SourceShardID:      pbom.pbftNode.ShardID,
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
				}
				requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
			} else {
				req.ProcessedMap[currentCallNode.TypeTraceAddress] = true
				destShardID := req.SourceShardID

				response := &message.CrossShardFunctionResponse{
					OriginalSender:     req.OriginalSender,
					SourceShardID:      pbom.pbftNode.ShardID,
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
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}
		}
	}

	pbom.pbftNode.pl.Plog.Printf("processContractRequests: skipCount %d", skipCount)

	// シャードごとにリクエストを送信
	pbom.sendRequests(requestsByShard)
	pbom.sendResponses(responseByShard)
}

// 共通関数: レスポンスの処理
func (pbom *ProposalBrokerOutsideModule) processBatchResponses(responses []*message.CrossShardFunctionResponse) {
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

		differentShardNode, hasDiffShard, processedMap := pbom.DFS(currentCallNode, false)
		for k, v := range processedMap {
			res.ProcessedMap[k] = v
		}

		if !hasDiffShard {
			// TODO: 親にレスポンスを返す。ルートの場合はトランザクションを発行
			destShardID := pbom.pbftNode.CurChain.Get_PartitionMap(currentCallNode.Sender)
			res.ProcessedMap[currentCallNode.TypeTraceAddress] = true

			if currentCallNode.CallType == "root" {
				// ルートの場合、トランザクションを発行
				if destShardID != pbom.pbftNode.ShardID {
					response := &message.CrossShardFunctionResponse{
						OriginalSender:     res.OriginalSender,
						SourceShardID:      pbom.pbftNode.ShardID,
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
					}
					responseByShard[destShardID] = append(responseByShard[destShardID], response)
				} else {
					// txを発行する
					stateChangeAccountByShard := make(map[uint64][]*core.Account)
					ssid := pbom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Sender)
					rsid := pbom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Recipient)

					if stateChangeAccountByShard[ssid] == nil {
						stateChangeAccountByShard[ssid] = make([]*core.Account, 0)
					}
					stateChangeAccountByShard[ssid] = append(stateChangeAccountByShard[ssid], &core.Account{AcAddress: res.Tx.Sender, IsContract: false})

					if stateChangeAccountByShard[rsid] == nil {
						stateChangeAccountByShard[rsid] = make([]*core.Account, 0)
					}
					stateChangeAccountByShard[rsid] = append(stateChangeAccountByShard[rsid], &core.Account{AcAddress: res.Tx.Recipient, IsContract: true})

					for _, itx := range res.Tx.InternalTxs {
						switch itx.CallType {
						case "call", "create":
							rsid = pbom.pbftNode.CurChain.Get_PartitionMap(itx.Recipient)
							if stateChangeAccountByShard[rsid] == nil {
								stateChangeAccountByShard[rsid] = make([]*core.Account, 0)
							}
							stateChangeAccountByShard[rsid] = append(stateChangeAccountByShard[rsid], &core.Account{AcAddress: itx.Recipient, IsContract: itx.RecipientIsContract})
						case "delegatecall":
							ssid = pbom.pbftNode.CurChain.Get_PartitionMap(itx.Sender)
							if stateChangeAccountByShard[ssid] == nil {
								stateChangeAccountByShard[ssid] = make([]*core.Account, 0)
							}
							stateChangeAccountByShard[ssid] = append(stateChangeAccountByShard[ssid], &core.Account{AcAddress: itx.Sender, IsContract: itx.SenderIsContract})
						case "staticcall", "suicide":
							// 状態を変更しないので、何もしない
						default:
							fmt.Println("CallTypeが分類できません", itx.CallType)
						}
					}

					for shardID, accounts := range stateChangeAccountByShard {
						tx := core.NewTransaction(res.OriginalSender, res.Tx.Recipient, res.Tx.Value, 0, res.Tx.Time)
						tx.TxHash = res.Tx.TxHash
						tx.HasContract = true
						tx.RequestID = res.RequestID
						tx.DivisionCount = len(stateChangeAccountByShard)

						if len(stateChangeAccountByShard) == 1 {
							pbom.pbftNode.pl.Plog.Println("CrossShardFunctionCallがCLPAでInner TXになりました。")
							pbom.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)
							tx.InternalTxs = res.Tx.InternalTxs
							tx.IsAllInner = true
						} else {
							/* OriginalRecipientShardID := pbom.pbftNode.CurChain.Get_PartitionMap(res.Tx.Recipient)
							if shardID == OriginalRecipientShardID {
								tx.InternalTxs = res.Tx.InternalTxs
								tx.IsExecuteCLPA = true
							} */

							tx.InternalTxs = res.Tx.InternalTxs
							tx.IsExecuteCLPA = true

							tx.IsCrossShardFuncCall = true
						}

						// シャードに関連付けられたコントラクトアドレスを追加
						scAddrCount := 0
						acAddrCount := 0
						for _, account := range accounts {
							tx.StateChangeAccounts[account.AcAddress] = account
							if account.IsContract {
								scAddrCount++
							} else {
								acAddrCount++
							}
						}

						// トランザクションを対応するシャードに追加
						/* if len(res.Tx.InternalTxs) > 0 {
							txhash := res.Tx.InternalTxs[0].ParentTxHash
							fmt.Printf("txHash: %s, SCAddrNum: %d, AcAddrNum: %d, shard: %d\n", txhash, scAddrCount, acAddrCount, shardID)
						} else {
							fmt.Printf("internalTxなし reci %s, SCAddrNum: %d, AcAddrNum: %d, shard: %d\n", res.Tx.Recipient, scAddrCount, acAddrCount, shardID)
						}
						*/
						injectTxByShard[shardID] = append(injectTxByShard[shardID], tx)
					}
				}
			} else {
				// ルート以外の場合、親にレスポンスを返す
				response := &message.CrossShardFunctionResponse{
					OriginalSender:     res.OriginalSender,
					SourceShardID:      pbom.pbftNode.ShardID,
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
				}
				responseByShard[destShardID] = append(responseByShard[destShardID], response)
			}

		} else {
			// 異なるシャードが見つかった場合、その子にリクエストを送信
			destShardID := pbom.pbftNode.CurChain.Get_PartitionMap(differentShardNode.Recipient)
			request := &message.CrossShardFunctionRequest{
				OriginalSender:     res.OriginalSender,
				SourceShardID:      pbom.pbftNode.ShardID,
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
			}

			requestsByShard[destShardID] = append(requestsByShard[destShardID], request)
		}
	}

	// シャードごとにリクエストを送信
	pbom.sendRequests(requestsByShard)
	pbom.sendResponses(responseByShard)
	pbom.sendInjectTransactions(injectTxByShard)
}

// 共通関数: リクエストの送信
func (pbom *ProposalBrokerOutsideModule) sendRequests(requestsByShard map[uint64][]*message.CrossShardFunctionRequest) {
	for sid, requests := range requestsByShard {
		if sid == pbom.pbftNode.ShardID {
			pbom.pbftNode.pl.Plog.Println("自分自身のシャードには送信しません。DFSの処理が正しく行われていない可能性があります。")
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
		go networks.TcpDial(msg_send, pbom.pbftNode.ip_nodeTable[sid][0])
		// pbom.pbftNode.pl.Plog.Printf("sendRequests(%s): Shard %d にリクエストを %d 件送信しました。\n", message.CContractRequest, sid, len(requests))
	}
}

func (pbom *ProposalBrokerOutsideModule) sendResponses(responsesByShard map[uint64][]*message.CrossShardFunctionResponse) {
	for sid, responses := range responsesByShard {
		/* if sid == pbom.pbftNode.ShardID {
			pbom.pbftNode.pl.Plog.Println("自分自身のシャードには送信しません。")
			continue
		} */
		rByte, err := json.Marshal(responses)
		if err != nil {
			log.Panic("sendRequests: Unmarshal エラー", err)
		}
		msg_send := message.MergeMessage(message.CContactResponse, rByte)
		go networks.TcpDial(msg_send, pbom.pbftNode.ip_nodeTable[sid][0])
		// pbom.pbftNode.pl.Plog.Printf("sendRequests(%s): Shard %d にレスポンスを %d 件送信しました。\n", message.CContactResponse, sid, len(responses))
	}
}

// `CInject` 用のトランザクション送信
func (pbom *ProposalBrokerOutsideModule) sendInjectTransactions(sendToShard map[uint64][]*core.Transaction) {
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
		go networks.TcpDial(sendMsg, pbom.pbftNode.ip_nodeTable[sid][0])
	}
}

func (pbom *ProposalBrokerOutsideModule) StartBatchProcessing(batchSize int, interval time.Duration) {
	pbom.pbftNode.pl.Plog.Println("StartBatchProcessing: 開始!!!")

	// メインのバッチ処理用タイマー
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// CSVエクスポート用のタイマー (5秒間隔)
	exportTicker := time.NewTicker(time.Duration(params.Block_Interval) * time.Millisecond)
	defer exportTicker.Stop()

	// パーティション状態ログ出力用タイマー (1秒間隔)
	partitionLogTicker := time.NewTicker(1 * time.Second)
	defer partitionLogTicker.Stop()

	for {
		select {
		case <-ticker.C:
			// パーティションを変更するときは待機
			if pbom.cdm.PartitionOn {
				continue
			}

			// リクエストのバッチ処理
			requestBatch := pbom.cfcpm.GetAndClearRequests(batchSize)
			if len(requestBatch) > 0 {
				start := time.Now() // 計測開始
				pbom.processBatchRequests(requestBatch)
				elapsed := time.Since(start) // 経過時間を計測
				fmt.Printf("processBatchRequests: %v items processed in %v\n", len(requestBatch), elapsed)
			}

			// レスポンスのバッチ処理
			responseBatch := pbom.cfcpm.GetAndClearResponses(batchSize)
			if len(responseBatch) > 0 {
				start := time.Now() // 計測開始
				pbom.processBatchResponses(responseBatch)
				elapsed := time.Since(start) // 経過時間を計測
				fmt.Printf("processBatchResponses: %v items processed in %v\n", len(responseBatch), elapsed)
			}

		case <-exportTicker.C:
			// CSVファイルのエクスポート
			filePath := "expTest/contractPoolSize/S" + strconv.Itoa(int(pbom.pbftNode.ShardID)) + ".csv"
			pbom.cfcpm.ExportPoolSizesToCSV(filePath)

		case <-partitionLogTicker.C:
			// パーティション状態のログ出力
			if pbom.cdm.PartitionOn {
				pbom.pbftNode.pl.Plog.Println("パーティションを変更中なので、コントラクトバッチ処理を待機します。")
			}
		}
	}
}

// 戻り値は、異なるシャードが見つかった場合の子を返す
// A→Bを比較するとき、Bがかえって来る
// 戻り値は、異なるシャードが見つかった場合の子を返す
// 第2戻り値は異なるシャードが見つかった場合はtrue、見つからなかった場合はfalse
func (pbom *ProposalBrokerOutsideModule) DFS(root *core.CallNode, checkSelf bool) (*core.CallNode, bool, map[string]bool) {
	processedMap := make(map[string]bool) // 処理済みのTypeTraceAddressを格納するマップ

	// 内部関数で再帰処理を定義
	var dfsHelper func(node *core.CallNode) (*core.CallNode, bool)
	dfsHelper = func(node *core.CallNode) (*core.CallNode, bool) {
		if node == nil || node.IsProcessed { // IsProcessed が true のノードはスキップ
			return nil, false
		}

		// senderとrecipientのシャードを取得
		ssid := pbom.pbftNode.CurChain.Get_PartitionMap(node.Sender)
		rsid := pbom.pbftNode.CurChain.Get_PartitionMap(node.Recipient)

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
