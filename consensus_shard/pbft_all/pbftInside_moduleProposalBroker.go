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

	"github.com/ethereum/go-ethereum/common"
)

type ProposalBrokerPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
	cfcpm    *dataSupport.CrossFunctionCallPoolManager
}

// propose request with different types
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if pbhm.cdm.PartitionOn { // handlePartitionMsg()でPartitionOnがtrueになる SupervisorからすべてのリーダにCPartitionMsgを受け取り実行する
		pbhm.pbftNode.pl.Plog.Println("パーティションブロックのProposeを行います。")
		pbhm.sendPartitionReady() // Leader to Other Shard Leaders
		for !pbhm.getPartitionReady() {
			unReadyShard := make([]uint64, 0)
			pbhm.cdm.P_ReadyLock.Lock()
			for k := 0; k < params.ShardNum; k++ {
				_, exists := pbhm.cdm.PartitionReady[uint64(k)]
				if !exists {
					unReadyShard = append(unReadyShard, uint64(k))
				}
			}
			pbhm.cdm.P_ReadyLock.Unlock()
			pbhm.pbftNode.pl.Plog.Printf("待機中: PartitionReadyが未完了のシャード: %v", unReadyShard)
			time.Sleep(10 * time.Second)
		}
		pbhm.pbftNode.pl.Plog.Println("各シャードのPartitionReadyがすべてtrueになりました。")
		// send accounts and txs
		pbhm.sendAccounts_and_Txs() // Leader to Other Shard Leaders
		// propose a partition
		for !pbhm.getCollectOver() {
			time.Sleep(time.Second)
			pbhm.pbftNode.pl.Plog.Println("各シャードのCollectOverがすべてtrueになるまでgetCollectOver()で待機します。")
		}
		return pbhm.proposePartition()
	}

	// ELSE: propose a block
	// TODO: ここでSCの実行とtraceの決定
	pbhm.pbftNode.pl.Plog.Println("TXブロックのProposeを行います。")
	pbhm.pbftNode.pl.Plog.Println("TxPool Size Before PackTX: ", len(pbhm.pbftNode.CurChain.Txpool.TxQueue))
	block := pbhm.pbftNode.CurChain.GenerateBlock(int32(pbhm.pbftNode.NodeID))

	pbhm.pbftNode.pl.Plog.Println("TxPool Size After PackTX: ", len(pbhm.pbftNode.CurChain.Txpool.TxQueue))
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	r.Msg.Content = block.Encode()
	return true, r

}

// the diy operation in preprepare
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// judge whether it is a partitionRequest or not
	isPartitionReq := ppmsg.RequestMsg.RequestType == message.PartitionReq

	if isPartitionReq {
		// after some checking
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : a partition block\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
	} else {
		// the request is a block
		if pbhm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
			pbhm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
			return false
		}
	}
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
	pbhm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	start := time.Now()
	r := pbhm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		pbhm.accountTransfer_do(atm)
		pbhm.pbftNode.pl.Plog.Printf("accountTransfer_doが完了しました。\n")
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number, pbhm.pbftNode.CurChain.CurrentBlock.Header.Number)
	fmt.Printf("State Root in HandleinCommit(): %s\n", common.BytesToHash(pbhm.pbftNode.CurChain.CurrentBlock.Header.StateRoot))
	startAddBlock := time.Now()
	pbhm.pbftNode.CurChain.AddBlock(block)
	pbhm.pbftNode.pl.Plog.Printf("[DEBUG] AddBlock完了: 所要時間=%s", time.Since(startAddBlock))
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number)
	pbhm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if pbhm.pbftNode.NodeID == uint64(pbhm.pbftNode.view.Load()) {
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send broker confirm txs at height = %d \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number)
		// generate brokertxs and collect txs excuted
		innerShardTxs := make([]*core.Transaction, 0)
		broker1Txs := make([]*core.Transaction, 0)
		broker2Txs := make([]*core.Transaction, 0)

		crossShardFunctionCall := make([]*core.Transaction, 0)
		innerSCTxs := make([]*core.Transaction, 0)

		loopTime := time.Now()
		for _, tx := range block.Body {
			if !tx.HasContract {
				isBroker1Tx := tx.Sender == tx.OriginalSender
				isBroker2Tx := tx.Recipient == tx.FinalRecipient

				senderIsInshard := pbhm.pbftNode.CurChain.Get_PartitionMap(tx.Sender) == pbhm.pbftNode.ShardID
				recipientIsInshard := pbhm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient) == pbhm.pbftNode.ShardID
				if isBroker1Tx && !senderIsInshard {
					tx.PrintTx()
					fmt.Println("[ERROR] Err tx1")
					// log.Panic("Err tx1")
				}
				if isBroker2Tx && !recipientIsInshard {
					tx.PrintTx()
					fmt.Println("[ERROR] Err tx2")
					// log.Panic("Err tx2")
				}
				if tx.RawTxHash == nil {
					if tx.HasBroker {
						if tx.SenderIsBroker && !recipientIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx 1 - recipient")
							// log.Panic("err tx 1 - recipient")
						}
						if !tx.SenderIsBroker && !senderIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx 1 - sender")
							// log.Panic("err tx 1 - sender")
						}
					} else {
						if !senderIsInshard || !recipientIsInshard {
							tx.PrintTx()
							fmt.Println("[ERROR] err tx - without broker")
							// log.Panic("err tx - without broker")
						}
					}
				}

				if isBroker2Tx {
					broker2Txs = append(broker2Txs, tx)
				} else if isBroker1Tx {
					broker1Txs = append(broker1Txs, tx)
				} else {
					innerShardTxs = append(innerShardTxs, tx)
				}
			}

			//TODO: ここにInternal TXを持っている場合は追加の処理を書く
			if tx.HasContract {
				if tx.IsCrossShardFuncCall {
					crossShardFunctionCall = append(crossShardFunctionCall, tx)
					err := pbhm.cfcpm.SClock.UnlockAllByRequestID(tx.RequestID)

					if err != nil {
						// TODO: Unlockするのは1シャードだけにする
						// fmt.Println(err)
					}
				} else if tx.IsAllInner {
					// Internal TXを持つが、すべて同じshard内で完結(txも含め)する場合
					innerSCTxs = append(innerSCTxs, tx)
				} else {
					fmt.Println("分類できないInternal TXがあります。")
				}
			}

		}
		pbhm.pbftNode.pl.Plog.Printf("[DEBUG] loopTime: %s", time.Since(loopTime))

		// send seqID
		// TODO: これが必要かどうか検討　relayのpphm.pbftNode.RelayMsgSend()これに対応するもの
		for sid := uint64(0); sid < pbhm.pbftNode.pbftChainConfig.ShardNums; sid++ {
			if sid == pbhm.pbftNode.ShardID {
				continue
			}
			sii := message.SeqIDinfo{
				SenderShardID: pbhm.pbftNode.ShardID,
				SenderSeq:     pbhm.pbftNode.sequenceID,
			}
			sByte, err := json.Marshal(sii)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CSeqIDinfo, sByte)
			go networks.TcpDial(msg_send, pbhm.pbftNode.ip_nodeTable[sid][0])
			pbhm.pbftNode.pl.Plog.Printf("S%dN%d : sended sequence ids to %d\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, sid)
		}
		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Epoch:           int(pbhm.cdm.AccountTransferRound),

			Broker1Txs: broker1Txs,
			Broker2Txs: broker2Txs,

			CrossShardFunctionCall: crossShardFunctionCall,
			InnerSCTxs:             innerSCTxs,

			SenderShardID: pbhm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, pbhm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)

		pbhm.pbftNode.CurChain.Txpool.GetLocked()

		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",

			"# of Inner Account Txs in this block",
			"# of Broker1 (ctx) Txs in this block",
			"# of Broker2 (ctx) Txs in this block",

			"# of Cross Shard Function Call Txs in this block",
			"# of Inner SC Txs in this block",

			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Broker1 Txs) (Duration: Broker1 proposed -> Broker1 Commit)",
			"SUM of confirm latency (ms, Broker2 Txs) (Duration: Broker2 proposed -> Broker2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(pbhm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),

			strconv.Itoa(len(innerShardTxs)),
			strconv.Itoa(len(broker1Txs)),
			strconv.Itoa(len(broker2Txs)),

			strconv.Itoa(len(crossShardFunctionCall)),
			strconv.Itoa(len(innerSCTxs)),

			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker2Txs, bim.CommitTime), 10),
		}
		pbhm.pbftNode.writeCSVline(metricName, metricVal)
		pbhm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	elapsed := time.Since(start)
	pbhm.pbftNode.pl.Plog.Printf("HandleinCommitにかかった時間は %s \n", elapsed)
	return true
}

func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				pbhm.pbftNode.CurChain.AddBlock(b)
			} else {
				atm := message.DecodeAccountTransferMsg(r.Msg.Content)
				pbhm.accountTransfer_do(atm)
			}
		}
		pbhm.pbftNode.sequenceID = som.SeqEndHeight + 1
		pbhm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
