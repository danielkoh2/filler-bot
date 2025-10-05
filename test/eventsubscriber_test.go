package main

import (
	go_drift "driftgo"
	"driftgo/anchor"
	"driftgo/anchor/types"
	"driftgo/connection"
	"driftgo/drift/config"
	"driftgo/events"
	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jinzhu/configor"
	"keeper-bots-2025/src"
	"testing"
	"time"
)

var configm = struct {
	Global     src.GlobalConfig
	BotConfigs map[string]src.FillerConfig `yaml:"botConfigs"`
}{}

func loadConfig(configFile string) {
	err := configor.New(&configor.Config{
		Silent:               true,
		ErrorOnUnmatchedKeys: true,
	}).Load(&configm, configFile)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}
}

func createConnectionManager() *connection.Manager {
	connectionManager := connection.CreateManager()
	for key, connectionConfig := range configm.Global.EndPoints {
		connectionManager.AddConfig(connectionConfig, key)
	}
	return connectionManager
}
func createProgram(manager *connection.Manager) types.IProgram {
	wallet, err := src.GetWallet(configm.Global.KeeperPrivateKey)
	if err != nil {
		panic(err.Error())
	}
	provider := anchor.CreateAnchorProvider(wallet, wallet.GetPublicKey(), go_drift.ConfirmOptions{Commitment: rpc.CommitmentConfirmed}, manager)
	return anchor.CreateProgram(solana.MPK(config.DRIFT_PROGRAM_ID), provider)
}

// go test --run TestEventSubscriber
func TestEventSubscriber(t *testing.T) {
	loadConfig("../config.yaml")
	connectionManager := createConnectionManager()
	program := createProgram(connectionManager)
	eventSubscriber := events.CreateEventSubscriber(
		connectionManager.GetGeyser(),
		program,
		nil,
	)
	go_drift.EventEmitter().On("newEvent", func(data ...interface{}) {
		eventList := data[0].([]*events.WrappedEvent)
		for _, event := range eventList {
			spew.Dump(event.EventType, "--------", event.Data)
			//switch event.EventType {
			//case events.EventTypeOrderRecord:
			//	record := event.Data.(*drift.OrderRecord)
			//	spew.Dump("Ordered", record)
			//case events.EventTypeOrderActionRecord:
			//	record := event.Data.(*drift.OrderActionRecord)
			//	if record.Action == drift.OrderActionFill {
			//		spew.Dump("Filled", record)
			//	}
			//}
			//if event.EventType == events.EventTypeOrderActionRecord {
			//	record := event.Data.(*drift.OrderActionRecord)
			//	if record.Action == drift.OrderActionFill {
			//		spew.Dump("Filled........", record)
			//	}
			//}
		}
	})
	eventSubscriber.Subscribe()
	time.Sleep(time.Second * 100)
}
