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

type ProposalPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

// propose request with different types
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if pphm.cdm.PartitionOn { // handlePartitionMsg()でPartitionOnがtrueになる SupervisorからすべてのリーダにCPartitionMsgを受け取り実行する
		pphm.pbftNode.pl.Plog.Println("パーティションブロックのProposeを行います。")
		pphm.sendPartitionReady() // Leader to Other Shard Leaders
		for !pphm.getPartitionReady() {
			pphm.pbftNode.pl.Plog.Println("各シャードのPartitionReadyがすべてtrueになるまでgetPartitionReady()で待機します。")
			time.Sleep(time.Second)
		}
		pphm.pbftNode.pl.Plog.Println("各シャードのPartitionReadyがすべてtrueになりました。")
		// send accounts and txs
		pphm.sendAccounts_and_Txs() // Leader to Other Shard Leaders
		// propose a partition
		for !pphm.getCollectOver() {
			time.Sleep(time.Second)
			pphm.pbftNode.pl.Plog.Println("各シャードのCollectOverがすべてtrueになるまでgetCollectOver()で待機します。")
		}
		return pphm.proposePartition()
	}

	// ELSE: propose a block
	// TODO: ここでSCの実行とtraceの決定
	pphm.pbftNode.pl.Plog.Println("TXブロックのProposeを行います。")
	pphm.pbftNode.pl.Plog.Println("TxPool Size Before PackTX: ", len(pphm.pbftNode.CurChain.Txpool.TxQueue))
	block := pphm.pbftNode.CurChain.GenerateBlock(int32(pphm.pbftNode.NodeID))

	pphm.pbftNode.pl.Plog.Println("TxPool Size After PackTX: ", len(pphm.pbftNode.CurChain.Txpool.TxQueue))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		// after some checking
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	} else {
		// the request is a block
		if pphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			pphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
			return false
		}
	}
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	pphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (pphm *ProposalPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := pphm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		pphm.accountTransfer_do(atm)
		pphm.pbftNode.pl.Plog.Printf("accountTransfer_doが完了しました。\n")
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number, pphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	pphm.pbftNode.CurChain.AddBlock(block)
	pphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number)
	pphm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if pphm.pbftNode.NodeID == uint64(pphm.pbftNode.view.Load()) {
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		pphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		innerShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		crossShardFunctionCall := make([]*core.Transaction, 0)
		innerSCTxs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if !tx.HasContract {
				if !tx.Relayed && ssid != pphm.pbftNode.ShardID {
					pphm.pbftNode.pl.Plog.Printf(
						"[ERROR] Transaction relay status mismatch: expected ShardID=%d, got ssid=%d, rsid=%d. Sender=%s, Recipient=%s, Relayed=%t",
						pphm.pbftNode.ShardID, ssid, rsid, tx.Sender, tx.Recipient, tx.Relayed,
					)
					log.Panic("incorrect tx1: Relayed is false but shard IDs do not match.")
				}
				if tx.Relayed && rsid != pphm.pbftNode.ShardID {
					pphm.pbftNode.pl.Plog.Printf(
						"[ERROR] Transaction relay shard ID mismatch: expected ShardID=%d, got ssid=%d, rsid=%d. Sender=%s, Recipient=%s, Relayed=%t",
						pphm.pbftNode.ShardID, ssid, rsid, tx.Sender, tx.Recipient, tx.Relayed,
					)
					log.Panic("incorrect tx2: Relayed is true but recipient shard ID does not match.")
				}
				if rsid != pphm.pbftNode.ShardID {
					relay1Txs = append(relay1Txs, tx)
					tx.Relayed = true
					// RelayPoolに追加
					pphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
				} else {
					if tx.Relayed {
						relay2Txs = append(relay2Txs, tx)
						tx.IsTxProcessed = true
					} else {
						innerShardTxs = append(innerShardTxs, tx)
						tx.IsTxProcessed = true
					}
				}
			}

			//TODO: ここにInternal TXを持っている場合は追加の処理を書く
			if tx.HasContract {
				pphm.pbftNode.pl.Plog.Println("HasContractの処理を行います。")
				if tx.IsCrossShardFuncCall {
					pphm.pbftNode.pl.Plog.Println("CrossShardFunctionCallの処理を行います。")
					ssid = pphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
					crossShardFunctionCall = append(crossShardFunctionCall, tx)
					// TODO: ssidが正しいか検証
					pphm.pbftNode.CurChain.Txpool.AddCrossShardFuncTx(tx, ssid)
				} else {
					// Internal TXを持つがすべて同じshard内で完結(txも含め)する場合
					pphm.pbftNode.pl.Plog.Println("InnerSCの処理を行います。")
					innerSCTxs = append(innerSCTxs, tx)
				}
			}

		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			pphm.pbftNode.RelayWithProofSend(block)
		} else {
			// RelayPoolを他のシャードに送信して、RelayPoolをクリア
			// ここでInternal TXも送信する
			pphm.pbftNode.RelayMsgSend()
		}

		// CContactResponse Msgを送信
		pphm.pbftNode.CrossShardFunctionResponseMsgSend()

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Epoch:           int(pphm.cdm.AccountTransferRound),

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: pphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, pphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)

		pphm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",

			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",

			"Cross Shard Function Call Txs",
			"Inner SC Txs",

			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(pphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),

			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),

			strconv.Itoa(len(crossShardFunctionCall)),
			strconv.Itoa(len(innerSCTxs)),

			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		pphm.pbftNode.writeCSVline(metricName, metricVal)
		pphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (pphm *ProposalPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (pphm *ProposalPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		pphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", pphm.pbftNode.ShardID, pphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				pphm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				pphm.accountTransfer_do(atm)
			}
		}
		pphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		pphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
