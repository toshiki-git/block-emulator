package committee

import (
	"blockEmulator/broker"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// CLPA committee operations
type ProposalBrokerCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.CLPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int

	//broker related  attributes avatar
	broker             *broker.Broker
	brokerConfirm1Pool map[string]*message.Mag1Confirm
	brokerConfirm2Pool map[string]*message.Mag2Confirm
	brokerTxPool       []*core.Transaction
	brokerModuleLock   sync.Mutex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string
}

func NewProposalBrokerCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *ProposalBrokerCommitteeModule {
	cg := new(partition.CLPAState)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)

	broker := new(broker.Broker)
	broker.NewBroker(nil)

	return &ProposalBrokerCommitteeModule{
		csvPath:             csvFilePath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		brokerConfirm1Pool:  make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool:  make(map[string]*message.Mag2Confirm),
		brokerTxPool:        make([]*core.Transaction, 0),
		broker:              broker,
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
	}
}

// for CLPA_Broker committee, it only handle the extra CInner2CrossTx message.
func (pbcm *ProposalBrokerCommitteeModule) HandleOtherMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	if msgType != message.CInner2CrossTx {
		return
	}
	itct := new(message.InnerTx2CrossTx)
	err := json.Unmarshal(content, itct)
	if err != nil {
		log.Panic()
	}
	itxs := pbcm.dealTxByBroker(itct.Txs)
	pbcm.txSending(itxs)
}

// get shard id by address
func (pbcm *ProposalBrokerCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := pbcm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (pbcm *ProposalBrokerCommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, pbcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		pbcm.clpaLock.Lock()
		sendersid := pbcm.fetchModifiedMap(tx.Sender)

		if pbcm.broker.IsBroker(tx.Sender) {
			sendersid = pbcm.fetchModifiedMap(tx.Recipient)
		}

		pbcm.clpaLock.Unlock()
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (pbcm *ProposalBrokerCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(pbcm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)
	clpaCnt := 0

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(pbcm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			pbcm.nowDataNum++
		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(pbcm.batchDataNum) || pbcm.nowDataNum == pbcm.dataTotalNum {
			// set the algorithm timer begins
			if pbcm.clpaLastRunningTime.IsZero() {
				pbcm.clpaLastRunningTime = time.Now()
			}

			itx := pbcm.dealTxByBroker(txlist)

			pbcm.txSending(itx)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			pbcm.Ss.StopGap_Reset()
		}

		if params.ShardNum > 1 && !pbcm.clpaLastRunningTime.IsZero() && time.Since(pbcm.clpaLastRunningTime) >= time.Duration(pbcm.clpaFreq)*time.Second {
			pbcm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := pbcm.clpaGraph.CLPA_Partition()

			pbcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pbcm.modifiedMap[key] = val
			}
			pbcm.clpaReset()
			pbcm.clpaLock.Unlock()

			for atomic.LoadInt32(&pbcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pbcm.clpaLastRunningTime = time.Now()
			pbcm.sl.Slog.Println("Next CLPA epoch begins. ")
		}

		if pbcm.nowDataNum == pbcm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !pbcm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if time.Since(pbcm.clpaLastRunningTime) >= time.Duration(pbcm.clpaFreq)*time.Second {
			pbcm.clpaLock.Lock()
			clpaCnt++
			mmap, _ := pbcm.clpaGraph.CLPA_Partition()

			pbcm.clpaMapSend(mmap)
			for key, val := range mmap {
				pbcm.modifiedMap[key] = val
			}
			pbcm.clpaReset()
			pbcm.clpaLock.Unlock()

			for atomic.LoadInt32(&pbcm.curEpoch) != int32(clpaCnt) {
				time.Sleep(time.Second)
			}
			pbcm.clpaLastRunningTime = time.Now()
			pbcm.sl.Slog.Println("Next CLPA epoch begins. ")
		}
	}
}

func (pbcm *ProposalBrokerCommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, pbcm.IpNodeTable[i][0])
	}
	pbcm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (pbcm *ProposalBrokerCommitteeModule) clpaReset() {
	pbcm.clpaGraph = new(partition.CLPAState)
	pbcm.clpaGraph.Init_CLPAState(0.5, 100, params.ShardNum)
	for key, val := range pbcm.modifiedMap {
		pbcm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

// handle block information when received CBlockInfo message(pbft node commited)pbftノードがコミットしたとき
func (pbcm *ProposalBrokerCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	pbcm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&pbcm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		pbcm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}

	// add createConfirm
	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	pbcm.createConfirm(txs)

	pbcm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		if tx.HasBroker {
			continue
		}
		pbcm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
	}
	for _, b1tx := range b.Broker1Txs {
		pbcm.clpaGraph.AddEdge(partition.Vertex{Addr: b1tx.OriginalSender}, partition.Vertex{Addr: b1tx.FinalRecipient})
	}
	pbcm.clpaLock.Unlock()
}

func (pbcm *ProposalBrokerCommitteeModule) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.Mag1Confirm, 0)
	confirm2s := make([]*message.Mag2Confirm, 0)
	pbcm.brokerModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := pbcm.brokerConfirm1Pool[string(tx.TxHash)]; ok {
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := pbcm.brokerConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	pbcm.brokerModuleLock.Unlock()

	if len(confirm1s) != 0 {
		pbcm.handleTx1ConfirmMag(confirm1s)
	}

	if len(confirm2s) != 0 {
		pbcm.handleTx2ConfirmMag(confirm2s)
	}
}

func (pbcm *ProposalBrokerCommitteeModule) dealTxByBroker(txs []*core.Transaction) (itxs []*core.Transaction) {
	itxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
	for _, tx := range txs {
		pbcm.clpaLock.Lock()
		rSid := pbcm.fetchModifiedMap(tx.Recipient)
		sSid := pbcm.fetchModifiedMap(tx.Sender)
		pbcm.clpaLock.Unlock()
		//　どっちもブローカーじゃないし、受信者と送信者が違うシャード場合
		if rSid != sSid && !pbcm.broker.IsBroker(tx.Recipient) && !pbcm.broker.IsBroker(tx.Sender) {
			// Cross shard transaction. BrokerChain context
			brokerRawMeg := &message.BrokerRawMeg{
				Tx:     tx,
				Broker: pbcm.broker.BrokerAddress[0],
			}
			brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
		} else {
			// Inner shard transaction. BrokerChain context
			if pbcm.broker.IsBroker(tx.Recipient) || pbcm.broker.IsBroker(tx.Sender) {
				tx.HasBroker = true
				tx.SenderIsBroker = pbcm.broker.IsBroker(tx.Sender)
			}
			itxs = append(itxs, tx)
		}
	}
	if len(brokerRawMegs) != 0 {
		pbcm.handleBrokerRawMag(brokerRawMegs)
	}
	return itxs
}

func (pbcm *ProposalBrokerCommitteeModule) handleBrokerType1Mes(brokerType1Megs []*message.BrokerType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range brokerType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce, time.Now())
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient
		tx1.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx1.RawTxHash, ctx.TxHash)
		tx1s = append(tx1s, tx1)
		confirm1 := &message.Mag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash,
		}
		pbcm.brokerModuleLock.Lock()
		pbcm.brokerConfirm1Pool[string(tx1.TxHash)] = confirm1
		pbcm.brokerModuleLock.Unlock()
	}
	pbcm.txSending(tx1s)
	fmt.Println("BrokerType1Mes received by shard,  add brokerTx1 len ", len(tx1s))
}

func (pbcm *ProposalBrokerCommitteeModule) handleBrokerType2Mes(brokerType2Megs []*message.BrokerType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce, time.Now())
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		copy(tx2.RawTxHash, ctx.TxHash)
		tx2s = append(tx2s, tx2)

		confirm2 := &message.Mag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		pbcm.brokerModuleLock.Lock()
		pbcm.brokerConfirm2Pool[string(tx2.TxHash)] = confirm2
		pbcm.brokerModuleLock.Unlock()
	}
	pbcm.txSending(tx2s)
	fmt.Println("broker tx2 add to pool len ", len(tx2s))
}

// get the digest of rawMeg
func (pbcm *ProposalBrokerCommitteeModule) getBrokerRawMagDigest(r *message.BrokerRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

// handle broker raw message(Cross shard transaction)
func (pbcm *ProposalBrokerCommitteeModule) handleBrokerRawMag(brokerRawMags []*message.BrokerRawMeg) {
	b := pbcm.broker
	brokerType1Mags := make([]*message.BrokerType1Meg, 0)
	fmt.Println("broker receive ctx ", len(brokerRawMags))
	pbcm.brokerModuleLock.Lock()
	for _, meg := range brokerRawMags {
		b.BrokerRawMegs[string(pbcm.getBrokerRawMagDigest(meg))] = meg

		brokerType1Mag := &message.BrokerType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	pbcm.brokerModuleLock.Unlock()
	pbcm.handleBrokerType1Mes(brokerType1Mags)
}

func (pbcm *ProposalBrokerCommitteeModule) handleTx1ConfirmMag(mag1confirms []*message.Mag1Confirm) {
	brokerType2Mags := make([]*message.BrokerType2Meg, 0)
	b := pbcm.broker

	fmt.Println("receive confirm  brokerTx1 len ", len(mag1confirms))
	pbcm.brokerModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.BrokerRawMegs[string(pbcm.getBrokerRawMagDigest(RawMeg))]
		if !ok {
			fmt.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))

		brokerType2Mag := &message.BrokerType2Meg{
			Broker: pbcm.broker.BrokerAddress[0],
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	pbcm.brokerModuleLock.Unlock()
	pbcm.handleBrokerType2Mes(brokerType2Mags)
}

func (pbcm *ProposalBrokerCommitteeModule) handleTx2ConfirmMag(mag2confirms []*message.Mag2Confirm) {
	b := pbcm.broker
	fmt.Println("receive confirm  brokerTx2 len ", len(mag2confirms))
	num := 0
	pbcm.brokerModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag2confirm.Tx2Hash))
		if len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]) == 2 {
			num++
		} else {
			fmt.Println(len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]))
		}
	}
	pbcm.brokerModuleLock.Unlock()
	fmt.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
}
