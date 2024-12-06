// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/partition"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this message used in propose stage, so it will be invoked by InsidePBFT_Module
func (cphm *ProposalPbftInsideExtraHandleMod) sendPartitionReady() {
	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady[cphm.pbftNode.ShardID] = true
	cphm.cdm.P_ReadyLock.Unlock()

	pr := message.PartitionReady{
		FromShard: cphm.pbftNode.ShardID,
		NowSeqID:  cphm.pbftNode.sequenceID,
	}
	pByte, err := json.Marshal(pr)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionReady, pByte)
	for sid := 0; sid < int(cphm.pbftNode.pbftChainConfig.ShardNums); sid++ {
		if sid != int(pr.FromShard) {
			go networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[uint64(sid)][0])
		}
	}
	cphm.pbftNode.pl.Plog.Print("Ready for partition\n")
}

// get whether all shards is ready, it will be invoked by InsidePBFT_Module
func (cphm *ProposalPbftInsideExtraHandleMod) getPartitionReady() bool {
	cphm.cdm.P_ReadyLock.Lock()
	defer cphm.cdm.P_ReadyLock.Unlock()
	cphm.pbftNode.seqMapLock.Lock()
	defer cphm.pbftNode.seqMapLock.Unlock()
	cphm.cdm.ReadySeqLock.Lock()
	defer cphm.cdm.ReadySeqLock.Unlock()

	// TODO: 理解する
	flag := true
	for sid, val := range cphm.pbftNode.seqIDMap {
		if rval, ok := cphm.cdm.ReadySeq[sid]; !ok || (rval-1 != val) {
			flag = false
		}
	}
	return len(cphm.cdm.PartitionReady) == int(cphm.pbftNode.pbftChainConfig.ShardNums) && flag
}

// send the transactions and the accountState to other leaders
func (cphm *ProposalPbftInsideExtraHandleMod) sendAccounts_and_Txs() {
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)        // mergedVertexは含めないようにする
	lastMapid := len(cphm.cdm.ModifiedMap) - 1 // 最初は0
	cphm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): lastMapidは", lastMapid)
	for addr, newShardID := range cphm.cdm.ModifiedMap[lastMapid] { // key is the address, val is the shardID
		// ModifiedMapがすべてのアカウントを含んでないので、そこでエラーになる可能性がある
		if originalAddrs, ok := cphm.cdm.ReversedMergedContracts[lastMapid][partition.Vertex{Addr: addr}]; ok {
			// mergedVertexはaccountToFetchに含めない
			// mergedVertexのshardID(newShardID)と、originalAddrのshardIDが違う場合は移動
			// newShardID != cphm.pbftNode.ShardIDがないのは、originalAddrのshardIDが初期値がバラバラだから
			for _, originalAddr := range originalAddrs {
				currentShard := cphm.pbftNode.CurChain.Get_PartitionMap(originalAddr) // originalAddrの現在のシャード
				isDifferentShard := currentShard != newShardID                        // 移動先のシャードが異なるか
				isNewShardNotLocal := newShardID != cphm.pbftNode.ShardID             // 移動先が自分自身じゃない
				isCurrentShardLocal := currentShard == cphm.pbftNode.ShardID          // 対象アドレスが自分のシャードに所属している

				if isDifferentShard && isNewShardNotLocal && isCurrentShardLocal {
					accountToFetch = append(accountToFetch, originalAddr)
				}
			}
			continue
		}

		// ModifiedMapのアカウントが現在のシャードになくて、かつ自分のシャードに所属していることを確認
		// 基本的に相手に送るアカウントを考えている、自分のshardに来るものはほかのshardから送られてくる
		// 移動先が自分自身じゃない && 対象アドレスが自分のシャードに所属している
		if newShardID != cphm.pbftNode.ShardID && cphm.pbftNode.CurChain.Get_PartitionMap(addr) == cphm.pbftNode.ShardID { // cphm.pbftNode.CurChain.Get_PartitionMapはまだ更新されてない
			accountToFetch = append(accountToFetch, addr) // 移動するアカウントだけを抽出
		}
	}

	asFetched := cphm.pbftNode.CurChain.FetchAccounts(accountToFetch)

	// send the accounts to other shards
	cphm.pbftNode.CurChain.Txpool.GetLocked()
	cphm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs(): The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	for destShardID := uint64(0); destShardID < cphm.pbftNode.pbftChainConfig.ShardNums; destShardID++ {
		if destShardID == cphm.pbftNode.ShardID {
			continue
		}

		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)

		for idx, originalAddr := range accountToFetch {
			// if the account is in the shard i, then send it
			//　shard iに移動するべきアカウントをピックアップ
			// mergedされたaddrの場合
			if mergedVertex, ok := cphm.cdm.MergedContracts[lastMapid][originalAddr]; ok {
				if cphm.cdm.ModifiedMap[lastMapid][mergedVertex.Addr] == destShardID { // ModifiedMapはmergedVertexのものしか参照できない
					addrSend = append(addrSend, originalAddr)
					addrSet[originalAddr] = true
					asSend = append(asSend, asFetched[idx])
				}
				continue
			}
			if cphm.cdm.ModifiedMap[lastMapid][originalAddr] == destShardID {
				addrSend = append(addrSend, originalAddr)
				addrSet[originalAddr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		// TxPoolの中から移動するべきTXをピックアップ
		for secondPtr := 0; secondPtr < len(cphm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := cphm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]

			// Internal TXじゃないとき
			if !ptx.HasContract {
				// if this is a normal transaction or ctx1 before re-sharding && the addr is correspond
				// まだRelayedされてないときはSender側のシャードにTXを移動
				_, ok1 := addrSet[ptx.Sender]
				condition1 := ok1 && !ptx.Relayed
				// if this tx is ctx2
				// RelayedされているときはRecipient側のシャードにTXを移動
				_, ok2 := addrSet[ptx.Recipient]
				condition2 := ok2 && ptx.Relayed
				if condition1 || condition2 {
					txSend = append(txSend, ptx)
				} else {
					cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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
							cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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

						// 全部移動はしない場合
						if len(ptx.StateChangeAccounts) != len(sendAccount) {
							ptx.IsAllInner = false
							ptx.IsCrossShardFuncCall = true
							ptx.DivisionCount++
							fmt.Println("AllIneerがCrossShardFuncCallに変更されました")
						}

						ptx.StateChangeAccounts = remainingAccount

						if len(remainingAccount) > 0 {
							cphm.pbftNode.CurChain.Txpool.TxQueue[firstPtr] = ptx
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
			}
		}

		cphm.pbftNode.CurChain.Txpool.TxQueue = cphm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]
		cphm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のトランザクションを送信\n", destShardID, len(txSend))

		// ここにShard iに移動するContract Request Poolを送信する処理を追加する
		requestSend := make([]*message.CrossShardFunctionRequest, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(cphm.cfcpm.RequestPool); secondPtr++ {
			request := cphm.cfcpm.RequestPool[secondPtr]

			_, ok := addrSet[request.Recepient]
			if ok {
				requestSend = append(requestSend, request)
			} else {
				cphm.cfcpm.RequestPool[firstPtr] = request
				firstPtr++
			}
		}
		cphm.cfcpm.RequestPool = cphm.cfcpm.RequestPool[:firstPtr]
		cphm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のCrossShardFuctionCallRequestを送信\n", destShardID, len(requestSend))

		// ここにShard iに移動するContract Request Poolを送信する処理を追加する
		responseSend := make([]*message.CrossShardFunctionResponse, 0)
		firstPtr = 0
		for secondPtr := 0; secondPtr < len(cphm.cfcpm.ResponsePool); secondPtr++ {
			response := cphm.cfcpm.ResponsePool[secondPtr]

			_, ok := addrSet[response.Recipient]
			if ok {
				responseSend = append(responseSend, response)
			} else {
				cphm.cfcpm.ResponsePool[firstPtr] = response
				firstPtr++
			}
		}
		cphm.cfcpm.ResponsePool = cphm.cfcpm.ResponsePool[:firstPtr]
		cphm.pbftNode.pl.Plog.Printf("sendAccounts_and_Txs(): シャード %d に %d 個のCrossShardFuctionCallResponseを送信\n", destShardID, len(responseSend))

		cphm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", destShardID)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    cphm.pbftNode.ShardID,
			Txs:          txSend,
			Requests:     requestSend,
			Responses:    responseSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.AccountState_and_TX, aByte)
		networks.TcpDial(send_msg, cphm.pbftNode.ip_nodeTable[destShardID][0])
		cphm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", destShardID)
	}
	cphm.pbftNode.pl.Plog.Println("sendAccounts_and_Txs():after sending, The size of tx pool is: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))
	cphm.pbftNode.CurChain.Txpool.GetUnlocked()
}

// fetch collect infos
func (cphm *ProposalPbftInsideExtraHandleMod) getCollectOver() bool {
	cphm.cdm.CollectLock.Lock()
	defer cphm.cdm.CollectLock.Unlock()
	return cphm.cdm.CollectOver
}

// propose a partition message
func (cphm *ProposalPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	cphm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", cphm.pbftNode.ShardID, cphm.pbftNode.NodeID)
	// add all data in pool into the set
	for shradID, at := range cphm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			cphm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		cphm.cdm.ReceivedNewTx = append(cphm.cdm.ReceivedNewTx, at.Txs...)

		// TODO: ReceivedNewTxみたいにエラーハンドリングが必要
		cphm.cfcpm.AddRequests(at.Requests)
		cphm.pbftNode.pl.Plog.Println("proposePartition():", len(at.Requests), "個のContract Request追加")
		cphm.pbftNode.pl.Plog.Printf("proposePartition(): %d 個のContract Request追加 From %d\n", len(at.Requests), shradID)
		cphm.cfcpm.AddResponses(at.Responses)
		cphm.pbftNode.pl.Plog.Println("proposePartition():", len(at.Responses), "個のContract Response追加")
	}
	// propose, send all txs to other nodes in shard
	cphm.pbftNode.pl.Plog.Println("The number of ReceivedNewTx: ", len(cphm.cdm.ReceivedNewTx))

	filteredTxs := []*core.Transaction{}
	txHashMap := make(map[string]*core.Transaction)
	for _, rtx := range cphm.cdm.ReceivedNewTx {
		//TODO: ここで発生しているエラーを解消する
		// Internal TXじゃないとき
		if !rtx.HasContract {
			sender := rtx.Sender
			recipient := rtx.Recipient
			if mergedVertex, ok := cphm.cdm.MergedContracts[cphm.cdm.AccountTransferRound][rtx.Sender]; ok {
				sender = mergedVertex.Addr
			}
			if mergedVertex, ok := cphm.cdm.MergedContracts[cphm.cdm.AccountTransferRound][rtx.Recipient]; ok {
				recipient = mergedVertex.Addr
			}

			if !rtx.Relayed && cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][sender] != cphm.pbftNode.ShardID {
				fmt.Printf("[ERROR] Sender: %s, Recipient: %s modifiedMapShardID: %d, currentShardID: %d\n", rtx.Sender, rtx.Recipient, cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][rtx.Sender], cphm.pbftNode.ShardID)
				// log.Panic("error tx: sender is not in the shard")
			}
			if rtx.Relayed && cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][recipient] != cphm.pbftNode.ShardID {
				fmt.Printf("[ERROR] Sender: %s, Recipient: %s, modifiedMapShardID: %d, currentShardID: %d\n", rtx.Sender, rtx.Recipient, cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][rtx.Recipient], cphm.pbftNode.ShardID)
				// log.Panic("error tx: recipient is not in the shard")
			}
		}

		if rtx.HasContract {
			// これはエラーハンドリング
			for addr, account := range rtx.StateChangeAccounts {
				if mergedVertex, ok := cphm.cdm.MergedContracts[cphm.cdm.AccountTransferRound][addr]; ok {
					addr = mergedVertex.Addr
				}

				shardID := cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound][addr]

				if shardID != cphm.pbftNode.ShardID {
					rtx.PrintTx()
					fmt.Printf("[ERROR] Addr: %s, modifiedMapShardID: %d, currentShardID: %d, IsContract: %t\n", addr, shardID, cphm.pbftNode.ShardID, account.IsContract)
					log.Panic("error tx: StateChangeAccount is not in the shard ", addr, shardID, cphm.pbftNode.ShardID, account.IsContract)
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
	cphm.cdm.ReceivedNewTx = filteredTxs

	txHashToIdxMap := make(map[string]int) // key: RequestID, value: TxQueueのindex

	// TxQueueからRequestIDをキーにマッピングを作成
	for idx, tx := range cphm.pbftNode.CurChain.Txpool.TxQueue {
		txHashToIdxMap[string(tx.TxHash)] = idx
	}

	// ReceivedNewTxをフィルタリングしつつ処理
	filteredNewTx := make([]*core.Transaction, 0) // 残るrtxを保持するための新しいスライス

	for _, rtx := range cphm.cdm.ReceivedNewTx {
		if poolIdx, ok := txHashToIdxMap[string(rtx.TxHash)]; ok {
			// 新しく受け取ったStateを更新するaccountを反映させる
			for addr, account := range rtx.StateChangeAccounts {
				cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].StateChangeAccounts[addr] = account
			}

			if rtx.IsDeleted {
				if rtx.IsCrossShardFuncCall && cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].DivisionCount == rtx.MergeCount+1 {
					fmt.Println("AllInnerになったー")
					cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsCrossShardFuncCall = false
					cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsAllInner = true
				}
				if rtx.InternalTxs != nil {
					fmt.Println("InternalTxsを受け渡しました")
					cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].InternalTxs = rtx.InternalTxs
					cphm.pbftNode.CurChain.Txpool.TxQueue[poolIdx].IsExecuteCLPA = true
				}
			}
		} else {
			// rtxを残す
			filteredNewTx = append(filteredNewTx, rtx)
		}
	}

	// ReceivedNewTxを更新（削除済みのrtxを反映）
	cphm.cdm.ReceivedNewTx = filteredNewTx

	cphm.pbftNode.pl.Plog.Println("ReceivedNewTxはこれだけになりました: ", len(cphm.cdm.ReceivedNewTx))

	cphm.pbftNode.CurChain.Txpool.AddTxs2Pool(cphm.cdm.ReceivedNewTx)
	cphm.pbftNode.pl.Plog.Println("proposePartition(): The size of txpool: ", len(cphm.pbftNode.CurChain.Txpool.TxQueue))

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range cphm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	// これにat.Requestsとresponseを追加する
	atm := message.AccountTransferMsg{
		ModifiedMap:     cphm.cdm.ModifiedMap[cphm.cdm.AccountTransferRound],
		MergedContracts: cphm.cdm.MergedContracts[cphm.cdm.AccountTransferRound],
		Addrs:           atmaddr,
		AccountState:    atmAs,
		ATid:            uint64(len(cphm.cdm.ModifiedMap)),
	}
	atmbyte := atm.Encode()
	r := &message.Request{
		RequestType: message.PartitionReq,
		Msg: message.RawMessage{
			Content: atmbyte,
		},
		ReqTime: time.Now(),
	}
	return true, r
}

// all nodes in a shard will do accout Transfer, to sync the state trie
// パーティションブロックがコミットされたときに呼び出される
func (cphm *ProposalPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		cphm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}

	// TODO: mergedContractを更新する
	cnt1 := 0
	for key, val := range atm.MergedContracts {
		cnt1++
		cphm.pbftNode.CurChain.Update_MergedContracts(key, val)
	}
	cphm.pbftNode.pl.Plog.Printf("%d MergedContractsが更新されました\n", cnt1)

	cphm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	cphm.pbftNode.pl.Plog.Printf("%d addrs to add\n", len(atm.Addrs))
	cphm.pbftNode.pl.Plog.Printf("%d accountstates to add\n", len(atm.AccountState))
	cphm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, cphm.pbftNode.view.Load())

	// Insideモジュールに追加で、Leaderは実行されずに、ほかのノードのため
	if uint64(len(cphm.cdm.ModifiedMap)) != atm.ATid {
		cphm.pbftNode.pl.Plog.Println("ModifiedMapのInsideExtraHandleMod同期を行っています")
		cphm.cdm.ModifiedMap = append(cphm.cdm.ModifiedMap, atm.ModifiedMap)
		cphm.cdm.MergedContracts = append(cphm.cdm.MergedContracts, atm.MergedContracts)
		cphm.cdm.ReversedMergedContracts = append(cphm.cdm.ReversedMergedContracts, ReverseMap(atm.MergedContracts))
	}
	cphm.cdm.AccountTransferRound = atm.ATid //最初のpartition proposeの時は1になる
	cphm.cdm.AccountStateTx = make(map[uint64]*message.AccountStateAndTx)
	cphm.cdm.ReceivedNewAccountState = make(map[string]*core.AccountState)
	cphm.cdm.ReceivedNewTx = make([]*core.Transaction, 0)
	cphm.cdm.PartitionOn = false

	cphm.cdm.CollectLock.Lock()
	cphm.cdm.CollectOver = false
	cphm.cdm.CollectLock.Unlock()

	cphm.cdm.P_ReadyLock.Lock()
	cphm.cdm.PartitionReady = make(map[uint64]bool)
	cphm.cdm.P_ReadyLock.Unlock()

	cphm.pbftNode.CurChain.PrintBlockChain()
}
