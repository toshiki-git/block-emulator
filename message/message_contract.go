package message

import (
	"blockEmulator/core"
	"math/big"
)

type CrossShardFunctionRequest struct {
	SourceShardID      uint64
	DestinationShardID uint64
	Sender             string   // リクエスト発信元のアドレス
	Recepient          string   // リクエストの宛先となるコントラクトのアドレス
	Value              *big.Int // 送金額
	ContractAddress    string   // 呼び出すコントラクトのアドレス
	MethodSignature    string   // 呼び出すメソッドのシグネチャ
	Arguments          []byte   // メソッドに渡す引数
	RequestID          string   // リクエストを識別するためのID
	Timestamp          int64    // リクエストが発生したタイムスタンプ
	Signature          string   // リクエスト発信元の署名
	CurrentCallNode    *core.CallNode
}

type CrossShardFunctionResponse struct {
	SourceShardID      uint64 // 応答を送信するシャードID
	DestinationShardID uint64 // 応答の宛先となるシャードID（リクエストの発信元シャード）
	RequestID          string // リクエストに対応するID
	StatusCode         int    // 処理結果のステータスコード
	ResultData         []byte // 処理結果のデータ
	Timestamp          int64  // 応答が発生したタイムスタンプ
	Signature          string // 応答内容の署名
	CurrentCallNode    *core.CallNode
}
