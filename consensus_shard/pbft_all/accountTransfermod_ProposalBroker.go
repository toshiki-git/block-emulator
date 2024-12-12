// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) sendPartitionReady() {
	pbhm.cdm.P_ReadyLock.Lock()
	pbhm.cdm.PartitionReady[pbhm.pbftNode.ShardID] = true
	pbhm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: pbhm.pbftNode.ShardID,
		NowSeqID:  pbhm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(pbhm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	pbhm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) getPartitionReady() bool {
	pbhm.cdm.P_ReadyLock.Lock()
	defer pbhm.cdm.P_ReadyLock.Unlock()
	pbhm.pbftNode.seqMapLock.Lock()
	defer pbhm.pbftNode.seqMapLock.Unlock()
	pbhm.cdm.ReadySeqLock.Lock()
	defer pbhm.cdm.ReadySeqLock.Unlock()

	flag := true
	for sid, val := range pbhm.pbftNode.seqIDMap {
		if rval, ok := pbhm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(pbhm.cdm.PartitionReady) == int(pbhm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	start := time.Now()
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	lastMapid := len(pbhm.cdm.ModifiedMap) - 1 // 最初は0
	pbhm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): lastMapidは", lastMapid)
	txsBeCross := make([]*core.Transaction, 0)                      // the transactions which will be cross-shard tx because of re-partition
	for addr, newShardID := range pbhm.cdm.ModifiedMap[lastMapid] { //key is the address, val is the shardID
		// ModifiedMapがすべてのアカウントを含んでないので、そこでエラーになる可能性がある
		if originalAddrs, ok := pbhm.cdm.ReversedMergedContracts[lastMapid][partition.Vertex{Addr: addr}]; ok {
			// mergedVertexはaccountToFetchに含めない
			// mergedVertexのshardID(newShardID)と、originalAddrのshardIDが違う場合は移動
			// newShardID != pbhm.pbftNode.ShardIDがないのは、originalAddrのshardIDが初期値がバラバラだから
			for _, originalAddr := range originalAddrs {
				currentShard := pbhm.pbftNode.CurChain.Get_PartitionMap(originalAddr) // originalAddrの現在のシャード
				isDifferentShard := currentShard != newShardID                        // 移動先のシャードが異なるか
				isNewShardNotLocal := newShardID != pbhm.pbftNode.ShardID             // 移動先が自分自身じゃない
				isCurrentShardLocal := currentShard == pbhm.pbftNode.ShardID          // 対象アドレスが自分のシャードに所属している

				if isDifferentShard && isNewShardNotLocal && isCurrentShardLocal {
					accountToFetch = append(accountToFetch, originalAddr)
				}
			}
			continue
		}

		// ModifiedMapのアカウントが現在のシャードになくて、かつ自分のシャードに所属していることを確認
		// 基本的に相手に送るアカウントを考えている、自分のshardに来るものはほかのshardから送られてくる
		// 移動先が自分自身じゃない && 対象アドレスが自分のシャードに所属している
		if newShardID != pbhm.pbftNode.ShardID && pbhm.pbftNode.CurChain.Get_PartitionMap(addr) == pbhm.pbftNode.ShardID { // pbhm.pbftNode.CurChain.Get_PartitionMapはまだ更新されてない
			accountToFetch = append(accountToFetch, addr) // 移動するアカウントだけを抽出
		}
	}

	asFetched := pbhm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	// send the accounts to other shards
	pbhm.pbftNode.CurChain.Txpool.GetLocked()
	pbhm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): The size of tx pool is: ", len(pbhm.pbftNode.CurChain.Txpool.TxQueue))
	for destShardID := uint64(0); destShardID < pbhm.pbftNode.pbftChainConfig.ShardNums; destShardID++ {
		if destShardID == pbhm.pbftNode.ShardID {
			continue
		}

		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)

		for idx, originalAddr := range accountToFetch {
			// if the account is in the shard i, then send it
			//　shard iに移動するべきアカウントをピックアップ
			// mergedされたaddrの場合
			if mergedVertex, ok := pbhm.cdm.MergedContracts[lastMapid][originalAddr]; ok {
				if pbhm.cdm.ModifiedMap[lastMapid][mergedVertex.Addr] == destShardID { // ModifiedMapはmergedVertexのものしか参照できない
					addrSend = append(addrSend, originalAddr)
					addrSet[originalAddr] = true
					asSend = append(asSend, asFetched[idx])
				}
				continue
			}
			if pbhm.cdm.ModifiedMap[lastMapid][originalAddr] == destShardID {
				addrSend = append(addrSend, originalAddr)
				addrSet[originalAddr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(pbhm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := pbhm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
			// Internal TXじゃないとき
			if !ptx.HasContract {
				// whether should be transfer or not
				beSend := false
				beRemoved := false
				_, ok1 := addrSet[ptx.Sender]
				_, ok2 := addrSet[ptx.Recipient]
				if ptx.RawTxHash == nil { // if this tx is an inner-shard tx...
					if ptx.HasBroker {
						if ptx.SenderIsBroker {
							beSend = ok2
							beRemoved = ok2
						} else {
							beRemoved = ok1
							beSend = ok1
						}
					} else if ok1 || ok2 { // if the inner-shard tx should be transferred.
						txsBeCross = append(txsBeCross, ptx)
						beRemoved = true
					}
					// all inner-shard tx should not be added into the account transfer message
				} else if ptx.FinalRecipient == ptx.Recipient { //A→BのCTXを考えるとき。A→Broker, Broker→B
					beSend = ok2
					beRemoved = ok2
				} else if ptx.OriginalSender == ptx.Sender {
					beRemoved = ok1
					beSend = ok1
				}

				if beSend {
					txSend = append(txSend, ptx)
				}
				if !beRemoved {
					pbhm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = pbhm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
					firstPtr++
				}
			}

			// Internal TXのとき
			if ptx.HasContract {
				remainingAccount := make(map[string]*core.Account) // 残るAccountを保持
				sendAccount := make(map[string]*core.Account)      // 送信するAccountを保持
				isTxSend := false                                  // txSendに移動したかを追跡

				if ptx.IsCrossShardFuncCall {
					if len(ptx.StateChangeAccounts) > 0 {
						for _, account := range ptx.StateChangeAccounts {
							_, ok := addrSet[account.AcAddress]
							if ok {
								sendAccount[account.AcAddress] = account
								isTxSend = true
							} else {
								// 残るscAddrを保持
								remainingAccount[account.AcAddress] = account
							}

						}

						ptx.StateChangeAccounts = remainingAccount

						if len(remainingAccount) > 0 {
							pbhm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
							firstPtr++
						} else {
							ptx.IsDeleted = true
						}

						if isTxSend {
							ptx.StateChangeAccounts = sendAccount
							txSend = append(txSend, ptx)
						}
					} else {
						ptx.PrintTx()
						fmt.Println("[ERROR] 更新するstateがありません", ptx.Sender, ptx.Recipient)
					}
				}

				// ここが問題 PratitionによってAllInnerではなくなる可能性がある
				if ptx.IsAllInner {
					if len(ptx.StateChangeAccounts) > 0 {
						remainingAccount := make(map[string]*core.Account) // 残るAccountを保持
						sendAccount := make(map[string]*core.Account)      // 送信するAccountを保持
						isTxSend := false                                  // txSendに移動したかを追跡

						// addrSetに含まれているかどうかで仕分け
						for _, account := range ptx.StateChangeAccounts {
							if _, ok := addrSet[account.AcAddress]; ok {
								sendAccount[account.AcAddress] = account
								isTxSend = true
							} else {
								remainingAccount[account.AcAddress] = account
							}
						}

						// 部分的に移動が発生している場合
						if isTxSend && len(ptx.StateChangeAccounts) != len(sendAccount) && len(remainingAccount) > 0 {
							ptx.PrintTx()
							fmt.Printf("StateChangeAccounts: %d, sendAccount: %d, remainingAccount: %d\n", len(ptx.StateChangeAccounts), len(sendAccount), len(remainingAccount))
							ptx.IsAllInner = false
							ptx.IsCrossShardFuncCall = true
							ptx.DivisionCount++
							fmt.Println("IsAllInnerがCrossShardFuncCallに変更されました")
						}

						// 残るアカウントがある場合は ptx に反映して再キュー
						if len(remainingAccount) > 0 {
							ptx.StateChangeAccounts = remainingAccount
							pbhm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
							firstPtr++
						} else {
							// 残存アカウントがない場合は削除
							ptx.IsDeleted = true
						}

						// 送信用アカウントがある場合は ptx をコピーして送信リストへ
						if isTxSend {
							// ptxSendはptxのコピーを作成(参照を分離するため)
							ptxSend := *ptx
							ptxSend.StateChangeAccounts = sendAccount
							ptxSend.IsDeleted = false // コピーなので必要に応じて状態を調整
							txSend = append(txSend, &ptxSend)
						}

					} else {
						ptx.PrintTx()
						fmt.Println("[ERROR] 更新するstateがありません", ptx.Sender, ptx.Recipient)
					}
				}
			}
		}

		pbhm.pbftNode.CurChain.Txpool.TxQueue = pbhm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]
		pbhm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のトランザクションを送信\n", destShardID, len(txSend))

		// ここにShard iに移動するContract Request Poolを送信する処理を追加する
		requestSend := make([]*message.CrossShardFunctionRequest, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(pbhm.cfcpm.RequestPool); secondPtr++ {
			request := pbhm.cfcpm.RequestPool[secondPtr]

			_, ok := addrSet[request.Recepient]
			if ok {
				requestSend = append(requestSend, request)
			} else {
				pbhm.cfcpm.RequestPool[firstPtr] = request
				firstPtr++
			}
		}
		pbhm.cfcpm.RequestPool = pbhm.cfcpm.RequestPool[:firstPtr]
		pbhm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のCrossShardFuctionCallRequestを送信\n", destShardID, len(requestSend))

		// ここにShard iに移動するContract Request Poolを送信する処理を追加する
		responseSend := make([]*message.CrossShardFunctionResponse, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(pbhm.cfcpm.ResponsePool); secondPtr++ {
			response := pbhm.cfcpm.ResponsePool[secondPtr]

			_, ok := addrSet[response.Recipient]
			if ok {
				responseSend = append(responseSend, response)
			} else {
				pbhm.cfcpm.ResponsePool[firstPtr] = response
				firstPtr++
			}
		}
		pbhm.cfcpm.ResponsePool = pbhm.cfcpm.ResponsePool[:firstPtr]
		pbhm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のCrossShardFuctionCallResponseを送信\n", destShardID, len(responseSend))

		pbhm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", destShardID)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    pbhm.pbftNode.ShardID,
			Txs:          txSend,
			Requests:     requestSend,
			Responses:    responseSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.CAccountTransferMsg_broker, aByte)
		go networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[destShardID][0])
		pbhm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", destShardID)
	}
	pbhm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs():after sending, The size of tx pool is: ", len(pbhm.pbftNode.CurChain.Txpool.TxQueue))
	pbhm.pbftNode.CurChain.Txpool.GetUnlocked()
	pbhm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): かかった時間 %v\n", time.Since(start))

	// send these txs to supervisor
	i2ctx := message.InnerTx2CrossTx{
		Txs: txsBeCross,
	}
	icByte, err := json.Marshal(i2ctx)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CInner2CrossTx, icByte)
	go networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
}

// fetch collect infos
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) getCollectOver() bool {
	pbhm.cdm.CollectLock.Lock()
	defer pbhm.cdm.CollectLock.Unlock()
	return pbhm.cdm.CollectOver
}

// propose a partition message
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	start := time.Now()
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
	// add all data in pool into the set
	for shradID, at := range pbhm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			pbhm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		pbhm.cdm.ReceivedNewTx = append(pbhm.cdm.ReceivedNewTx, at.Txs...)

		// TODO: ReceivedNewTxみたいにエラーハンドリングが必要
		pbhm.cfcpm.AddRequests(at.Requests)
		pbhm.pbftNode.pl.Plog.Println("proposePartition():", len(at.Requests), "個のContract Request追加")
		pbhm.pbftNode.pl.Plog.Printf("proposePartition(): %d 個のContract Request追加 From %d\n", len(at.Requests), shradID)
		pbhm.cfcpm.AddResponses(at.Responses)
		pbhm.pbftNode.pl.Plog.Println("proposePartition():", len(at.Responses), "個のContract Response追加")
	}
	// propose, send all txs to other nodes in shard
	pbhm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(pbhm.cdm.ReceivedNewTx))

	filteredTxs := []*core.Transaction{}
	txHashMap := make(map[string]*core.Transaction)
	for _, rtx := range pbhm.cdm.ReceivedNewTx {
		//TODO: ここで発生しているエラーを解消する
		// Internal TXじゃないとき
		if !rtx.HasContract {
			// TODO: Brokerの場合のエラーハンドリング
			// 元のソースコードだとエラーハンドリングがない
		}

		if rtx.HasContract {
			// これはエラーハンドリング
			for addr, account := range rtx.StateChangeAccounts {
				if mergedVertex, ok := pbhm.cdm.MergedContracts[pbhm.cdm.AccountTransferRound][addr]; ok {
					addr = mergedVertex.Addr
				}

				shardID := pbhm.cdm.ModifiedMap[pbhm.cdm.AccountTransferRound][addr]

				if shardID != pbhm.pbftNode.ShardID {
					rtx.PrintTx()
					fmt.Printf("[ERROR] Addr: %s, modifiedMapShardID: %d, currentShardID: %d, IsContract: %t\n", addr, shardID, pbhm.pbftNode.ShardID, account.IsContract)
					log.Panic("error tx: StateChangeAccount is not in the shard ", addr, shardID, pbhm.pbftNode.ShardID, account.IsContract)
				}

			}
		}

		if existingTx, exists := txHashMap[string(rtx.TxHash)]; exists {
			for addr, account := range rtx.StateChangeAccounts {
				existingTx.StateChangeAccounts[addr] = account
			}
			existingTx.MergeCount++

			if rtx.InternalTxs != nil {
				fmt.Println("InternalTxsを受け渡しました")
				existingTx.InternalTxs = rtx.InternalTxs
				existingTx.IsExecuteCLPA = true
			}

			if rtx.DivisionCount == existingTx.MergeCount+1 {
				fmt.Println("AllInnerに変更されました!!!!!!!!")
				existingTx.IsAllInner = true
				existingTx.IsCrossShardFuncCall = false
			}
		} else {
			// 新しいTxHashの場合、マップに記録して結果リストに追加
			txHashMap[string(rtx.TxHash)] = rtx
			filteredTxs = append(filteredTxs, rtx)
		}
	}

	// 重複を除いた配列を反映
	pbhm.cdm.ReceivedNewTx = filteredTxs

	txHashToIdxMap := make(map[string]int) // key: RequestID, value: TxQueueのindex

	// TxQueueからRequestIDをキーにマッピングを作成
	for idx, tx := range pbhm.pbftNode.CurChain.Txpool.TxQueue {
		txHashToIdxMap[string(tx.TxHash)] = idx
	}

	// ReceivedNewTxをフィルタリングしつつ処理
	filteredNewTx := make([]*core.Transaction, 0) // 残るrtxを保持するための新しいスライス

	for _, rtx := range pbhm.cdm.ReceivedNewTx {
		if poolIdx, ok := txHashToIdxMap[string(rtx.TxHash)]; ok {
			// 新しく受け取ったStateを更新するaccountを反映させる
			for addr, account := range rtx.StateChangeAccounts {
				pbhm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].StateChangeAccounts[addr] = account
			}

			if rtx.IsDeleted {
				if rtx.IsCrossShardFuncCall && pbhm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].DivisionCount == rtx.MergeCount+1 {
					fmt.Println("AllInnerになったー")
					pbhm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsCrossShardFuncCall = false
					pbhm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsAllInner = true
				}
			}
		} else {
			// rtxを残す
			filteredNewTx = append(filteredNewTx, rtx)
		}
	}

	// ReceivedNewTxを更新（削除済みのrtxを反映）
	pbhm.cdm.ReceivedNewTx = filteredNewTx

	pbhm.pbftNode.pl.Plog.Println("ReceivedNewTxはこれだけになりました: ", len(pbhm.cdm.ReceivedNewTx))

	// pbhm.pbftNode.CurChain.Txpool.AddTxs2Pool(pbhm.cdm.ReceivedNewTx)
	pbhm.pbftNode.CurChain.Txpool.AddTxs2Front(pbhm.cdm.ReceivedNewTx)
	pbhm.pbftNode.pl.Plog.Println("proposePartition(): The size of txpool: ", len(pbhm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range pbhm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:     pbhm.cdm.ModifiedMap[pbhm.cdm.AccountTransferRound],
		MergedContracts: pbhm.cdm.MergedContracts[pbhm.cdm.AccountTransferRound],
		Addrs:           atmaddr,
		AccountState:    atmAs,
		ATid:            uint64(len(pbhm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}

	pbhm.pbftNode.pl.Plog.Printf("proposePartition()の時間: %v", time.Since(start))

	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		pbhm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}

	// TODO: mergedContractを更新する
	cnt1 := 0
	for key, val := range atm.MergedContracts {
		cnt1++
		pbhm.pbftNode.CurChain.Update_MergedContracts(key, val)
	}
	pbhm.pbftNode.pl.Plog.Printf("%d MergedContractsが更新されました\n", cnt1)

	pbhm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	pbhm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	pbhm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	pbhm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, pbhm.pbftNode.view.Load())

	if uint64(len(pbhm.cdm.ModifiedMap)) != atm.ATid {
		pbhm.pbftNode.pl.Plog.Println("ModifiedMapのInsideExtraHandleMod同期を行っています")
		pbhm.cdm.ModifiedMap = append(pbhm.cdm.ModifiedMap, atm.ModifiedMap)
		pbhm.cdm.MergedContracts = append(pbhm.cdm.MergedContracts, atm.MergedContracts)
		pbhm.cdm.ReversedMergedContracts = append(pbhm.cdm.ReversedMergedContracts, ReverseMap(atm.MergedContracts))
	}
	pbhm.cdm.AccountTransferRound = atm.ATid //最初のpartition proposeの時は1になる
	pbhm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	pbhm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	pbhm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	pbhm.cdm.PartitionOn = false

	pbhm.cdm.CollectLock.Lock()
	pbhm.cdm.CollectOver = false
	pbhm.cdm.CollectLock.Unlock()

	pbhm.cdm.P_ReadyLock.Lock()
	pbhm.cdm.PartitionReady = make(map[uint64]bool)
	pbhm.cdm.P_ReadyLock.Unlock()

	pbhm.pbftNode.CurChain.PrintBlockChain()
}
