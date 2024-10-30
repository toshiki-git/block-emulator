// account transfer happens when the leader received the re-partition message.
// leaders send the infos about the accounts to be transferred to other leaders, and
// handle them.

package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
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
			networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[uint64(sid)][0])
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
	// generate accout transfer and txs message
	accountToFetch := make([]string, 0)
	txsBeCross := make([]*core.Transaction, 0) // the transactions which will be cross-shard tx because of re-partition
	lastMapid := len(pbhm.cdm.ModifiedMap) - 1
	for key, val := range pbhm.cdm.ModifiedMap[lastMapid] { //key is the address, val is the shardID
		if val != pbhm.pbftNode.ShardID && pbhm.pbftNode.CurChain.Get_PartitionMap(key) == pbhm.pbftNode.ShardID {
			accountToFetch = append(accountToFetch, key) //移動するアカウントだけを抽出
		}
	}
	asFetched := pbhm.pbftNode.CurChain.FetchAccounts(accountToFetch)
	// send the accounts to other shards
	pbhm.pbftNode.CurChain.Txpool.GetLocked()
	for i := uint64(0); i < pbhm.pbftNode.pbftChainConfig.ShardNums; i++ {
		if i == pbhm.pbftNode.ShardID {
			continue
		}
		addrSend := make([]string, 0)
		addrSet := make(map[string]bool)
		asSend := make([]*core.AccountState, 0)
		for idx, addr := range accountToFetch {
			// if the account is in the shard i, then send it
			if pbhm.cdm.ModifiedMap[lastMapid][addr] == i {
				addrSend = append(addrSend, addr)
				addrSet[addr] = true
				asSend = append(asSend, asFetched[idx])
			}
		}
		// fetch transactions to it, after the transactions is fetched, delete it in the pool
		txSend := make([]*core.Transaction, 0)
		firstPtr := 0
		for secondPtr := 0; secondPtr < len(pbhm.pbftNode.CurChain.Txpool.TxQueue); secondPtr++ {
			ptx := pbhm.pbftNode.CurChain.Txpool.TxQueue[secondPtr]
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
		pbhm.pbftNode.CurChain.Txpool.TxQueue = pbhm.pbftNode.CurChain.Txpool.TxQueue[:firstPtr]

		pbhm.pbftNode.pl.Plog.Printf("The txSend to shard %d is generated \n", i)
		ast := message.AccountStateAndTx{
			Addrs:        addrSend,
			AccountState: asSend,
			FromShard:    pbhm.pbftNode.ShardID,
			Txs:          txSend,
		}
		aByte, err := json.Marshal(ast)
		if err != nil {
			log.Panic()
		}
		send_msg := message.MergeMessage(message.CAccountTransferMsg_broker, aByte)
		networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[i][0])
		pbhm.pbftNode.pl.Plog.Printf("The message to shard %d is sent\n", i)
	}
	pbhm.pbftNode.CurChain.Txpool.GetUnlocked()

	// send these txs to supervisor
	i2ctx := message.InnerTx2CrossTx{
		Txs: txsBeCross,
	}
	icByte, err := json.Marshal(i2ctx)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CInner2CrossTx, icByte)
	networks.TcpDial(send_msg, pbhm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
}

// fetch collect infos
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) getCollectOver() bool {
	pbhm.cdm.CollectLock.Lock()
	defer pbhm.cdm.CollectLock.Unlock()
	return pbhm.cdm.CollectOver
}

// propose a partition message
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) proposePartition() (bool, *message.Request) {
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : begin partition proposing\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
	// add all data in pool into the set
	for _, at := range pbhm.cdm.AccountStateTx {
		for i, addr := range at.Addrs {
			pbhm.cdm.ReceivedNewAccountState[addr] = at.AccountState[i]
		}
		pbhm.cdm.ReceivedNewTx = append(pbhm.cdm.ReceivedNewTx, at.Txs...)
	}
	// propose, send all txs to other nodes in shard
	pbhm.pbftNode.CurChain.Txpool.AddTxs2Pool(pbhm.cdm.ReceivedNewTx)

	atmaddr := make([]string, 0)
	atmAs := make([]*core.AccountState, 0)
	for key, val := range pbhm.cdm.ReceivedNewAccountState {
		atmaddr = append(atmaddr, key)
		atmAs = append(atmAs, val)
	}
	atm := message.AccountTransferMsg{
		ModifiedMap:  pbhm.cdm.ModifiedMap[pbhm.cdm.AccountTransferRound],
		Addrs:        atmaddr,
		AccountState: atmAs,
		ATid:         uint64(len(pbhm.cdm.ModifiedMap)),
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
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) accountTransfer_do(atm *message.AccountTransferMsg) {
	// change the partition Map
	cnt := 0
	for key, val := range atm.ModifiedMap {
		cnt++
		pbhm.pbftNode.CurChain.Update_PartitionMap(key, val)
	}
	pbhm.pbftNode.pl.Plog.Printf("%d key-vals are updated\n", cnt)
	// add the account into the state trie
	pbhm.pbftNode.CurChain.AddAccounts(atm.Addrs, atm.AccountState, pbhm.pbftNode.view.Load())

	if uint64(len(pbhm.cdm.ModifiedMap)) != atm.ATid {
		pbhm.cdm.ModifiedMap = append(pbhm.cdm.ModifiedMap, atm.ModifiedMap)
	}
	pbhm.cdm.AccountTransferRound = atm.ATid
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
