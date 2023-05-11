// The pbft consensus process

package pbft_clpa

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/shard"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	db       ethdb.Database    // to save the mpt

	// the global config about pbft
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // denote the ip of the specific node
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N
	view            uint64                       // denote the view of this pbft, the main node can be inferred from this variant

	// the control message and message checking utils in pbft
	sequenceID        uint64                          // the message sequence id of the pbft
	stop              bool                            // send stop signal
	pStop             chan uint64                     // channle for stopping consensus
	requestPool       map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast  map[string]bool                 // denote whether the commit is broadcast
	isReply           map[string]bool                 // denote whether the message is reply
	height2Digest     map[uint64]string               // sequence (block height) -> request, fast read

	// locks about pbft
	sequenceLock sync.Mutex // the lock of sequence
	lock         sync.Mutex // lock the stage
	askForLock   sync.Mutex // lock for asking for a serise of requests

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbftlog

	// tcp control
	tcpln net.Listener

	// to handle the message in the pbft
	ihm PbftInsideExtraHandleMod

	// to handle the message outside of pbft
	ohm PbftOutsideHandleMod
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := "./record/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic("cannot get the level db")
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stop = false
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3
	p.view = 0

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	default:
		ncdm := NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	}

	return p
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		p.handlePrePrepare(content)
	case message.CPrepare:
		p.handlePrepare(content)
	case message.CCommit:
		p.handleCommit(content)
	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)
	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

// listen to the request
func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	p.pl.plog.Printf("S%dN%d begins listening：%s\n", p.ShardID, p.NodeID, p.RunningNode.IPaddr)

	for {
		if p.stop {
			p.closePbft()
			return
		}
		conn, err := p.tcpln.Accept()
		if err != nil {
			log.Panic(err)
		}
		b, err := io.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		p.handleMessage(b)
	}
}

// when received stop
func (p *PbftConsensusNode) WaitToStop() {
	fmt.Println("handling stop message")
	p.stop = true
	if p.NodeID == p.view {
		p.pStop <- 1
	}
	p.tcpln.Close()
	fmt.Println("handled stop message")
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	p.CurChain.CloseBlockChain()
}