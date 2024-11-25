package message

import (
	"blockEmulator/core"
	"math/big"
)

type CrossShardFunctionRequest struct {
	OriginalSender     string
	SourceShardID      uint64
	DestinationShardID uint64
	Sender             string   // リクエスト発信元のアドレス
	Recepient          string   // リクエストの宛先となるコントラクトのアドレス
	Value              *big.Int // 送金額
	MethodSignature    string   // 呼び出すメソッドのシグネチャ
	Arguments          []byte   // メソッドに渡す引数
	RequestID          string   // リクエストを識別するためのID
	Timestamp          int64    // リクエストが発生したタイムスタンプ
	Signature          string   // リクエスト発信元の署名
	TypeTraceAddress   string
	Tx                 *core.Transaction
	ProcessedMap       map[string]bool // key: TypeTraceAddress, value: 処理済みかどうか
	VisitedShards      map[uint64]bool // 過去に通過したシャードIDを記録
}

type CrossShardFunctionResponse struct {
	OriginalSender     string
	SourceShardID      uint64   // 応答を送信するシャードID
	DestinationShardID uint64   // 応答の宛先となるシャードID（リクエストの発信元シャード）
	Sender             string   // 応答発信元のアドレス
	Recipient          string   // 応答の宛先となるコントラクトのアドレス
	Value              *big.Int // 送金額
	RequestID          string   // リクエストに対応するID
	StatusCode         int      // 処理結果のステータスコード
	ResultData         []byte   // 処理結果のデータ
	Timestamp          int64    // 応答が発生したタイムスタンプ
	Signature          string   // 応答内容の署名
	TypeTraceAddress   string
	Tx                 *core.Transaction
	ProcessedMap       map[string]bool
	VisitedShards      map[uint64]bool // 過去に通過したシャードIDを記録
}
