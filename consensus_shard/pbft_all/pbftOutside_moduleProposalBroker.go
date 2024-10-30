package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

// This module used in the blockChain using Broker mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type ProposalBrokerOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
}

func (pbom *ProposalBrokerOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CSeqIDinfo:
		pbom.handleSeqIDinfos(content)
	case message.CInject:
		pbom.handleInjectTx(content)

	// messages about CLPA
	case message.CPartitionMsg:
		pbom.handlePartitionMsg(content)
	case message.CAccountTransferMsg_broker:
		pbom.handleAccountStateAndTxMsg(content)
	case message.CPartitionReady:
		pbom.handlePartitionReady(content)
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

// the leader received the partition message from listener/decider,
// it init the local variant and send the accout message to other leaders.
func (pbom *ProposalBrokerOutsideModule) handlePartitionMsg(content []byte) {
	pm := new(message.PartitionModifiedMap)
	err := json.Unmarshal(content, pm)
	if err != nil {
		log.Panic()
	}
	pbom.cdm.ModifiedMap = append(pbom.cdm.ModifiedMap, pm.PartitionModified)
	pbom.pbftNode.pl.Plog.Printf("S%dN%d : has received partition message\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID)
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

	pbom.pbftNode.pl.Plog.Printf("ready message from shard %d, seqid is %d\n", pr.FromShard, pr.NowSeqID)
}

// when the message from other shard arriving, it should be added into the message pool
func (pbom *ProposalBrokerOutsideModule) handleAccountStateAndTxMsg(content []byte) {
	at := new(message.AccountStateAndTx)
	err := json.Unmarshal(content, at)
	if err != nil {
		log.Panic()
	}
	pbom.cdm.AccountStateTx[at.FromShard] = at
	pbom.pbftNode.pl.Plog.Printf("S%dN%d has added the accoutStateandTx from %d to pool\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID, at.FromShard)

	if len(pbom.cdm.AccountStateTx) == int(pbom.pbftNode.pbftChainConfig.ShardNums)-1 {
		pbom.cdm.CollectLock.Lock()
		pbom.cdm.CollectOver = true
		pbom.cdm.CollectLock.Unlock()
		pbom.pbftNode.pl.Plog.Printf("S%dN%d has added all accoutStateandTx~~~\n", pbom.pbftNode.ShardID, pbom.pbftNode.NodeID)
	}
}
