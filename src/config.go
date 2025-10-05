package src

import (
	"driftgo/connection"
	"driftgo/drift/config"
	"driftgo/priorityFee"
)

type DebugOption struct {
	TargetTrace       bool   `yaml:"targetTrace"`
	TargetMarketIndex uint16 `yaml:"targetMarketIndex"`
}

type GlobalConfig = struct {
	DriftEnv                         config.DriftEnv               `yaml:"driftEnv"`
	EndPoints                        map[string]connection.Config  `yaml:"endPoints"`
	PriorityFeeMethod                priorityFee.PriorityFeeMethod `yaml:"priorityFeeMethod"`
	MaxPriorityFeeMicroLamports      uint64                        `yaml:"maxPriorityFeeMicroLamports"`
	ResubTimeoutMs                   int64                         `yaml:"resubTimeoutMs"`
	PriorityFeeMultiplier            float64                       `yaml:"priorityFeeMultiplier"`
	KeeperPrivateKey                 string                        `yaml:"keeperPrivateKey"`
	EventSubscriber                  bool                          `yaml:"eventSubscriber"`
	EventSubscriberConnectionId      string                        `yaml:"eventSubscriberConnectionId"`
	Debug                            bool                          `yaml:"debug"`
	DebugOption                      DebugOption                   `yaml:"debugOption"`
	SubAccounts                      []uint16                      `yaml:"subAccounts"`
	EventSubscriberPollingInterval   int64                         `yaml:"eventSubscriberPollingInterval"`
	BulkAccountLoaderPollingInterval int64                         `yaml:"bulkAccountLoaderPollingInterval"`
	SlotSubscriberConnection         string                        `yaml:"slotSubscriberConnection"`
	PriorityFeeSubscriberConnection  string                        `yaml:"priorityFeeSubscriberConnection"`
	UserMapConnection                string                        `yaml:"userMapConnection"`
	BlockhashSubscriberConnection    string                        `yaml:"blockhashSubscriberConnection"`
}

type FillerConfig struct {
	BotId                   string `yaml:"botId"`
	DryRun                  bool   `yaml:"dryRun"`
	FillerPollingInterval   int64  `yaml:"fillerPollingInterval"`
	RevertOnFailure         bool   `yaml:"revertOnFailure"`
	SimulateTxForCUEstimate bool   `yaml:"simulateTxForCUEstimate"`

	RebalanceFilter              bool    `yaml:"rebalanceFilter"`
	RebalanceSettledPnlThreshold float64 `yaml:"rebalanceSettledPnlThreshold"`
	MinGasBalanceToFill          float64 `yaml:"minGasBalanceToFill"`
	ProcessWaitTimeoutMs         int64   `yaml:"processWaitTimeoutMs"`
	MinOrderUpdateTimeMs         int64   `yaml:"minOrderUpdateTimeMs"`
	TryOnOrderUpdate             bool    `yaml:"tryOnOrderUpdate"`
	MinIntervalFillMs            int64   `yaml:"minIntervalFillMs"`
	FilterTryMuchCount           int64   `yaml:"filterTryMuchCount"`
}

type Bot interface {
	Init()
	Reset()
	StartIntervalLoop(intervalMs int64)
	HealthCheck() bool
}
