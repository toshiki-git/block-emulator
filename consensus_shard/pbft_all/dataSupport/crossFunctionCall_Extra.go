package dataSupport

import (
	"blockEmulator/message"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type CrossFunctionCallPoolManager struct {
	RequestPool        []*message.CrossShardFunctionRequest
	ResponsePool       []*message.CrossShardFunctionResponse
	ReceivedNewRequest map[string]bool
	SClock             *SmartContractLockManager
	mutex              sync.Mutex
}

// PoolManagerの初期化
func NewCrossFunctionCallPoolManager() *CrossFunctionCallPoolManager {
	return &CrossFunctionCallPoolManager{
		RequestPool:  make([]*message.CrossShardFunctionRequest, 0),
		ResponsePool: make([]*message.CrossShardFunctionResponse, 0),
		SClock:       NewSmartContractLockManager(),
	}
}

func (pm *CrossFunctionCallPoolManager) HandleContractRequest(content []byte) {
	requests := []*message.CrossShardFunctionRequest{}
	err := json.Unmarshal(content, &requests)
	if err != nil {
		log.Panic("handleContractRequest: Unmarshal エラー", err)
	}

	pm.AddRequests(requests)

	fmt.Printf("handleContractRequest: %d 件のリクエストをPoolに追加しました。\n", len(requests))
}

func (pm *CrossFunctionCallPoolManager) HandleContractResponse(content []byte) {
	responses := []*message.CrossShardFunctionResponse{}
	err := json.Unmarshal(content, &responses)
	if err != nil {
		log.Panic("handleContractResponse: Unmarshal エラー", err)
	}

	pm.AddResponses(responses)

	fmt.Printf("HandleContractResponse: %d 件のレスポンスをPoolに追加しました。\n", len(responses))
}

// Poolにデータを追加する関数
func (pm *CrossFunctionCallPoolManager) AddRequest(req *message.CrossShardFunctionRequest) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.RequestPool = append(pm.RequestPool, req)
}

// Poolに複数のリクエストを追加する関数
func (pm *CrossFunctionCallPoolManager) AddRequests(reqs []*message.CrossShardFunctionRequest) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.RequestPool = append(pm.RequestPool, reqs...)
}

func (pm *CrossFunctionCallPoolManager) AddResponse(res *message.CrossShardFunctionResponse) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.ResponsePool = append(pm.ResponsePool, res)
}

// Poolに複数のレスポンスを追加する関数
func (pm *CrossFunctionCallPoolManager) AddResponses(resps []*message.CrossShardFunctionResponse) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.ResponsePool = append(pm.ResponsePool, resps...)
}

// Poolからバッチを取得してクリアする関数
func (pm *CrossFunctionCallPoolManager) GetAndClearRequests(batchSize int) []*message.CrossShardFunctionRequest {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	count := min(batchSize, len(pm.RequestPool))
	batch := pm.RequestPool[:count]
	pm.RequestPool = pm.RequestPool[count:]
	return batch
}

func (pm *CrossFunctionCallPoolManager) GetAndClearResponses(batchSize int) []*message.CrossShardFunctionResponse {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	count := min(batchSize, len(pm.ResponsePool))
	batch := pm.ResponsePool[:count]
	pm.ResponsePool = pm.ResponsePool[count:]
	return batch
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// プールサイズをCSVに出力する関数
func (pm *CrossFunctionCallPoolManager) ExportPoolSizesToCSV(filename string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// ディレクトリを作成（存在しない場合のみ）
	dir := filepath.Dir(filename) // ディレクトリ部分を取得
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("ディレクトリ作成エラー: %v", err)
	}

	// ファイルを開く（新規作成時の判定用にos.Statを使う）
	isNewFile := false
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		isNewFile = true
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("CSVファイルのオープンエラー: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 新しいファイルの場合はヘッダーを書き込む
	if isNewFile {
		header := []string{"Timestamp", "RequestPoolSize", "ResponsePoolSize"}
		if err := writer.Write(header); err != nil {
			log.Fatalf("CSVへのヘッダー書き込みエラー: %v", err)
		}
	}

	// 現在時刻とプールサイズを取得
	currentTime := time.Now().Format(time.RFC3339)
	requestPoolSize := len(pm.RequestPool)
	responsePoolSize := len(pm.ResponsePool)

	// データを書き込む
	record := []string{currentTime, fmt.Sprintf("%d", requestPoolSize), fmt.Sprintf("%d", responsePoolSize)}
	if err := writer.Write(record); err != nil {
		log.Fatalf("CSVへの書き込みエラー: %v", err)
	}
}
