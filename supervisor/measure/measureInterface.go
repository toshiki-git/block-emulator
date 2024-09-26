package measure

import "blockEmulator/message"

type MeasureModule interface {
	UpdateMeasureRecord(*message.BlockInfoMsg) // Called when a commit is made
	HandleExtraMessage([]byte)                 // Auxiliary Interface
	OutputMetricName() string                  // Called when CloseSupervisor()
	OutputRecord() ([]float64, float64)        // Called when CloseSupervisor()
}
