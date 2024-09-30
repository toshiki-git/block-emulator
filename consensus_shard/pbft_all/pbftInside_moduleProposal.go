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
	if pphm.cdm.PartitionOn {
		pphm.sendPartitionReady()
		for !pphm.getPartitionReady() {
			time.Sleep(time.Second)
		}
		// send accounts and txs
		pphm.sendAccounts_and_Txs()
		// propose a partition
		for !pphm.getCollectOver() {
			time.Sleep(time.Second)
		}
		return pphm.proposePartition()
	}

	// ELSE: propose a block
	block := pphm.pbftNode.CurChain.GenerateBlock(int32(pphm.pbftNode.NodeID))
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
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)

		for _, tx := range block.Body {
			ssid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := pphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if !tx.Relayed && ssid != pphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if tx.Relayed && rsid != pphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if rsid != pphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				pphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			pphm.pbftNode.RelayWithProofSend(block)
		} else {
			pphm.pbftNode.RelayMsgSend()
		}

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
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