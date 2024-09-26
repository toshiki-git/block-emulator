package committee

import "blockEmulator/message"

type CommitteeModule interface {
	HandleBlockInfo(*message.BlockInfoMsg) // handle block information when received CBlockInfo message(pbft node commited)pbftノードがコミットしたとき
	MsgSendingControl()
	HandleOtherMessage([]byte)
}
