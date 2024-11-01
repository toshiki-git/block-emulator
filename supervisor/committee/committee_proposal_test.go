package committee

import (
	"blockEmulator/core"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLoadInternalTxsFromCSV(t *testing.T) {
	// テスト対象のCSVファイルのパスを指定
	internalTxCsvPath := "../../selectedInternalTxs_1000K.csv"

	// ProposalCommitteeModule のインスタンスを作成
	pcm := ProposalCommitteeModule{
		csvPath:           "",
		internalTxCsvPath: internalTxCsvPath,
		dataTotalNum:      0,
		nowDataNum:        0,
		batchDataNum:      0,

		// additional variants
		curEpoch:            0,
		clpaLock:            sync.Mutex{},
		ClpaGraph:           nil,
		modifiedMap:         make(map[string]uint64),
		clpaLastRunningTime: time.Time{},
		clpaFreq:            0,

		// logger module
		sl: nil,

		// control components
		Ss:          nil, // to control the stop message sending
		IpNodeTable: make(map[uint64]map[uint64]string),

		// smart contract internal transaction
		internalTxMap: make(map[string][]*core.InternalTransaction),
	}

	// テスト対象の関数を呼び出し
	internalTxMap := LoadInternalTxsFromCSV(pcm.internalTxCsvPath)

	// テストの検証（例として2つのトランザクションをチェック）
	// ここでは仮に存在する親トランザクションハッシュ "0x123" を検証しています。
	if len(internalTxMap) == 0 {
		t.Fatalf("Expected transactions, but got none")
	}

	fmt.Println("The number of internal transactions: ", len(internalTxMap))
}
