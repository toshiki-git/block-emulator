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

type ProposalBrokerPbftInsideExtraHandleMod struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

// propose request with different types
func (pbhm *ProposalBrokerPbftInsideExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	if pbhm.cdm.PartitionOn {
		pbhm.sendPartitionReady()
		for !pbhm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		// send accounts and txs
		pbhm.sendAccounts_and_Txs()
		// propose a partition
		for !pbhm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return pbhm.proposePartition()
	}

	// ELSE: propose a block
	block := pbhm.pbftNode.CurChain.GenerateBlock(int32(pbhm.pbftNode.NodeID))
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
	r := pbhm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	if r.RequestType == message.PartitionReq {
		// if a partition Requst ...
		atm := message.DecodeAccountTransferMsg(r.Msg.Content)
		pbhm.accountTransfer_do(atm)
		return true
	}
	// if a block request ...
	block := core.DecodeB(r.Msg.Content)
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number, pbhm.pbftNode.CurChain.CurrentBlock.Header.Number)
	pbhm.pbftNode.CurChain.AddBlock(block)
	pbhm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number)
	pbhm.pbftNode.CurChain.PrintBlockChain()

	// now try to relay txs to other shards (for main nodes)
	if pbhm.pbftNode.NodeID == uint64(pbhm.pbftNode.view.Load()) {
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send broker confirm txs at height = %d \n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, block.Header.Number)
		// generate brokertxs and collect txs excuted
		innerShardTxs := make([]*core.Transaction, 0)
		broker1Txs := make([]*core.Transaction, 0)
		broker2Txs := make([]*core.Transaction, 0)

		// generate block infos
		for _, tx := range block.Body {
			isBroker1Tx := tx.Sender == tx.OriginalSender
			isBroker2Tx := tx.Recipient == tx.FinalRecipient

			senderIsInshard := pbhm.pbftNode.CurChain.Get_PartitionMap(tx.Sender) == pbhm.pbftNode.ShardID
			recipientIsInshard := pbhm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient) == pbhm.pbftNode.ShardID
			if isBroker1Tx && !senderIsInshard {
				log.Panic("Err tx1")
			}
			if isBroker2Tx && !recipientIsInshard {
				log.Panic("Err tx2")
			}
			if tx.RawTxHash == nil {
				if tx.HasBroker {
					if tx.SenderIsBroker && !recipientIsInshard {
						log.Panic("err tx 1 - recipient")
					}
					if !tx.SenderIsBroker && !senderIsInshard {
						log.Panic("err tx 1 - sender")
					}
				} else {
					if !senderIsInshard || !recipientIsInshard {
						log.Panic("err tx - without broker")
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
		// send seqID
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
			networks.TcpDial(msg_send, pbhm.pbftNode.ip_nodeTable[sid][0])
			pbhm.pbftNode.pl.Plog.Printf("S%dN%d : sended sequence ids to %d\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID, sid)
		}
		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   innerShardTxs,
			Broker1Txs:      broker1Txs,
			Broker2Txs:      broker2Txs,
			Epoch:           int(pbhm.cdm.AccountTransferRound),
			SenderShardID:   pbhm.pbftNode.ShardID,
			ProposeTime:     r.ReqTime,
			CommitTime:      time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		networks.TcpDial(msg_send, pbhm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		pbhm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", pbhm.pbftNode.ShardID, pbhm.pbftNode.NodeID)
		pbhm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Broker1 Txs in this block",
			"# of Broker2 Txs in this block",
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
			strconv.Itoa(len(broker1Txs)),
			strconv.Itoa(len(broker2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(broker2Txs, bim.CommitTime), 10),
		}
		pbhm.pbftNode.writeCSVline(metricName, metricVal)
		pbhm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
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
