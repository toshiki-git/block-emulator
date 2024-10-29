package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/message"
	"encoding/json"
	"log"
)

// This module used in the blockChain using transaction relaying mechanism.
// "CLPA" means that the blockChain use Account State Transfer protocal by clpa.
type ProposalRelayOutsideModule struct {
	cdm      *dataSupport.Data_supportCLPA
	pbftNode *PbftConsensusNode
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
	prom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", prom.pbftNode.ShardID, prom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
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
