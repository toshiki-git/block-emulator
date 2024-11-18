package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// set 2d map, only for pbft maps, if the first parameter is true, then set the cntPrepareConfirm map,
// otherwise, cntCommitConfirm map will be set
func (p *PbftConsensusNode) set2DMap(isPrePareConfirm bool, key string, val *shard.Node) {
	if isPrePareConfirm {
		if _, ok := p.cntPrepareConfirm[key]; !ok {
			p.cntPrepareConfirm[key] = make(map[*shard.Node]bool)
		}
		p.cntPrepareConfirm[key][val] = true
	} else {
		if _, ok := p.cntCommitConfirm[key]; !ok {
			p.cntCommitConfirm[key] = make(map[*shard.Node]bool)
		}
		p.cntCommitConfirm[key][val] = true
	}
}

// get neighbor nodes in a shard
func (p *PbftConsensusNode) getNeighborNodes() []string {
	receiverNodes := make([]string, 0)
	for _, ip := range p.ip_nodeTable[p.ShardID] {
		receiverNodes = append(receiverNodes, ip)
	}
	return receiverNodes
}

// get node ips of shard id=shardID
func (p *PbftConsensusNode) getNodeIpsWithinShard(shardID uint64) []string {
	receiverNodes := make([]string, 0)
	for _, ip := range p.ip_nodeTable[shardID] {
		receiverNodes = append(receiverNodes, ip)
	}
	return receiverNodes
}

func (p *PbftConsensusNode) writeCSVline(metricName []string, metricVal []string) {
	// Construct directory path
	dirpath := params.DataWrite_path + "pbft_shardNum=" + strconv.Itoa(int(p.pbftChainConfig.ShardNums))
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	// Construct target file path
	targetPath := fmt.Sprintf("%s/Shard%d%d.csv", dirpath, p.ShardID, p.pbftChainConfig.ShardNums)

	// Open file, create if it does not exist
	file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)

	// Write header if the file is newly created
	fileInfo, err := file.Stat()
	if err != nil {
		log.Panic(err)
	}
	if fileInfo.Size() == 0 {
		if err := writer.Write(metricName); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}

	// Write data
	if err := writer.Write(metricVal); err != nil {
		log.Panic(err)
	}
	writer.Flush()
}

// get the digest of request
func getDigest(r *message.Request) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

// calculate TCL
func computeTCL(txs []*core.Transaction, commitTS time.Time) int64 {
	ret := int64(0)
	for _, tx := range txs {
		ret += commitTS.Sub(tx.Time).Milliseconds()
	}
	return ret
}

// help to send Relay message to other shards.

// help to send Relay message to other shards.
func (p *PbftConsensusNode) RelayMsgSend() {
	if params.RelayWithMerkleProof != 0 {
		log.Panicf("Parameter Error: RelayWithMerkleProof should be 0, but RelayWithMerkleProof=%d", params.RelayWithMerkleProof)
	}

	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		if sid == p.ShardID {
			continue
		}
		relay := message.Relay{
			Txs:           p.CurChain.Txpool.RelayPool[sid],
			SenderShardID: p.ShardID,
			SenderSeq:     p.sequenceID,
		}
		rByte, err := json.Marshal(relay)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CRelay, rByte)
		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0])
		p.pl.Plog.Printf("S%dN%d : sended relay txs to %d\n", p.ShardID, p.NodeID, sid)
	}
	p.CurChain.Txpool.ClearRelayPool()
}

func (p *PbftConsensusNode) CrossShardFunctionResponseMsgSend() {
	// シャードごとにCrossShardFunctionResponseを格納するマップ
	csfresByShard := make(map[uint64][]message.CrossShardFunctionResponse)

	if p.CurChain == nil || p.CurChain.Txpool == nil {
		log.Panic("CurChain or Txpool is nil")
	}

	// CrossShardFuncPoolからデータを取り出し、シャードごとに分割
	for sid, txs := range p.CurChain.Txpool.CrossShardFuncPool {
		if txs == nil {
			log.Printf("CrossShardFuncPool: sid=%d has nil transactions", sid)
			continue
		}

		for _, tx := range txs {
			if tx == nil || tx.RootCallNode == nil {
				log.Printf("Transaction or RootCallNode is nil for sid=%d", sid)
				continue
			}

			currentCallNode := tx.RootCallNode.FindNodeByTTA(tx.TypeTraceAddress)
			if currentCallNode == nil {
				log.Printf("FindNodeByTTA: Node not found for TypeTraceAddress=%s", tx.TypeTraceAddress)
				continue
			}
			// 処理済みフラグを立てる
			currentCallNode.IsProcessed = true

			fmt.Println("Commitされた後の結果")
			fmt.Println("TypeTraceAddress: ", tx.TypeTraceAddress)
			fmt.Printf("sender: %s, recipient: %s \n", tx.Sender, tx.Recipient)
			fmt.Println("sid: ", sid)
			tx.RootCallNode.PrintTree(0)

			//子から親に結果を返す
			csfres := message.CrossShardFunctionResponse{
				SourceShardID:      p.ShardID,
				DestinationShardID: sid,
				Sender:             tx.Sender,
				Recipient:          tx.Recipient,
				Value:              tx.Value,
				RequestID:          "0x12345678901", // 適切なRequestIDを設定
				StatusCode:         0,
				ResultData:         []byte(""), // 必要なら適切な結果データを設定
				Timestamp:          time.Now().Unix(),
				Signature:          "", // 必要なら署名を設定
				RootCallNode:       tx.RootCallNode,
				TypeTraceAddress:   tx.TypeTraceAddress, // 子のTypeTraceAddressをそのままコピー
			}
			csfresByShard[sid] = append(csfresByShard[sid], csfres)
		}
	}

	// 各シャードにメッセージを送信
	for sid, responses := range csfresByShard {
		if sid == p.ShardID {
			continue // 自分自身のシャードには送信しない
		}

		if p.ip_nodeTable[sid] == nil || len(p.ip_nodeTable[sid]) == 0 {
			log.Printf("ip_nodeTable: No nodes found for shard %d", sid)
			continue
		}

		// メッセージをエンコード
		rByte, err := json.Marshal(responses)
		if err != nil {
			log.Panic(err) // エラーがあれば停止
		}

		// メッセージを送信
		msg_send := message.MergeMessage(message.CContactResponse, rByte)
		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0]) // 非同期で送信
		p.pl.Plog.Printf("S%dN%d : CrossShardFunctionResponseの結果を返す shard %d に送信\n", p.ShardID, p.NodeID, sid)
	}

	// CrossShardFuncPoolをクリア
	p.CurChain.Txpool.ClearCrossShardFuncPool()
}

// help to send RelayWithProof message to other shards.
func (p *PbftConsensusNode) RelayWithProofSend(block *core.Block) {
	if params.RelayWithMerkleProof != 1 {
		log.Panicf("Parameter Error: RelayWithMerkleProof should be 1, but RelayWithMerkleProof=%d", params.RelayWithMerkleProof)
	}
	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		if sid == p.ShardID {
			continue
		}

		txHashes := make([][]byte, len(p.CurChain.Txpool.RelayPool[sid]))
		for i, tx := range p.CurChain.Txpool.RelayPool[sid] {
			txHashes[i] = tx.TxHash[:]
		}
		txProofs := chain.TxProofBatchGenerateOnBlock(txHashes, block)

		rwp := message.RelayWithProof{
			Txs:           p.CurChain.Txpool.RelayPool[sid],
			TxProofs:      txProofs,
			SenderShardID: p.ShardID,
			SenderSeq:     p.sequenceID,
		}
		rByte, err := json.Marshal(rwp)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CRelayWithProof, rByte)

		go networks.TcpDial(msg_send, p.ip_nodeTable[sid][0])
		p.pl.Plog.Printf("S%dN%d : sended relay txs & proofs to %d\n", p.ShardID, p.NodeID, sid)
	}
	p.CurChain.Txpool.ClearRelayPool()
}

// delete the txs in blocks. This list should be locked before calling this func.
func DeleteElementsInList(list []*core.Transaction, elements []*core.Transaction) []*core.Transaction {
	elementHashMap := make(map[string]bool)
	for _, element := range elements {
		elementHashMap[string(element.TxHash)] = true
	}

	removedCnt := 0
	for left, right := 0, 0; right < len(list); right++ {
		// if this tx should be deleted.
		if _, ok := elementHashMap[string(list[right].TxHash)]; ok {
			removedCnt++
		} else {
			list[left] = list[right]
			left++
		}
	}
	return list[:-removedCnt]
}

// ReverseMap はジェネリックを使用して、任意の型のキーと値を持つマップのキーと値を入れ替えます。
func ReverseMap[K comparable, V comparable](m map[K]V) map[V][]K {
	// 入れ替え後のマップを作成
	reversedMap := make(map[V][]K)

	// 元のマップのキーと値をループして、入れ替えた結果を reversedMap に格納
	for key, value := range m {
		reversedMap[value] = append(reversedMap[value], key)
	}

	return reversedMap
}
