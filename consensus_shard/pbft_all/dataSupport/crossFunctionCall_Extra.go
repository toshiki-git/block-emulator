package dataSupport

import (
	"blockEmulator/message"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

type CrossFunctionCallPoolManager struct {
	requestPool  []*message.CrossShardFunctionRequest
	responsePool []*message.CrossShardFunctionResponse
	mutex        sync.Mutex
}

// PoolManagerの初期化
func NewCrossFunctionCallPoolManager() *CrossFunctionCallPoolManager {
	return &CrossFunctionCallPoolManager{
		requestPool:  make([]*message.CrossShardFunctionRequest, 0),
		responsePool: make([]*message.CrossShardFunctionResponse, 0),
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
		log.Panic("andleContractResponse: Unmarshal エラー", err)
	}

	pm.AddResponses(responses)

	fmt.Printf("HandleContractResponse: %d 件のレスポンスをPoolに追加しました。\n", len(responses))
}

// Poolにデータを追加する関数
func (pm *CrossFunctionCallPoolManager) AddRequest(req *message.CrossShardFunctionRequest) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.requestPool = append(pm.requestPool, req)
}

// Poolに複数のリクエストを追加する関数
func (pm *CrossFunctionCallPoolManager) AddRequests(reqs []*message.CrossShardFunctionRequest) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.requestPool = append(pm.requestPool, reqs...)
}

func (pm *CrossFunctionCallPoolManager) AddResponse(res *message.CrossShardFunctionResponse) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.responsePool = append(pm.responsePool, res)
}

// Poolに複数のレスポンスを追加する関数
func (pm *CrossFunctionCallPoolManager) AddResponses(resps []*message.CrossShardFunctionResponse) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.responsePool = append(pm.responsePool, resps...)
}

// Poolからバッチを取得してクリアする関数
func (pm *CrossFunctionCallPoolManager) GetAndClearRequests(batchSize int) []*message.CrossShardFunctionRequest {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	count := min(batchSize, len(pm.requestPool))
	batch := pm.requestPool[:count]
	pm.requestPool = pm.requestPool[count:]
	return batch
}

func (pm *CrossFunctionCallPoolManager) GetAndClearResponses(batchSize int) []*message.CrossShardFunctionResponse {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	count := min(batchSize, len(pm.responsePool))
	batch := pm.responsePool[:count]
	pm.responsePool = pm.responsePool[count:]
	return batch
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
