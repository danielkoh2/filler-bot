package main

import (
	go_drift "driftgo"
	"driftgo/accounts"
	"driftgo/blockhashSubscriber"
	"driftgo/connection"
	"driftgo/dlob"
	dloblib "driftgo/dlob/types"
	"driftgo/drift"
	"driftgo/drift/types"
	"driftgo/events"
	oracles "driftgo/oracles/types"
	"driftgo/tx"
	userMap2 "driftgo/userMap"
	types2 "driftgo/userMap/types"
	"driftgo/utils"
	"flag"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jinzhu/configor"
	"keeper-bots-2025/src"
	bots2 "keeper-bots-2025/src/bots"
	"keeper-bots-2025/src/log"
	"sync"
)

var Config = struct {
	Global     src.GlobalConfig
	BotConfigs map[string]src.FillerConfig `yaml:"botConfigs"`
}{}

func configHasBot(botName string) bool {
	_, exists := Config.BotConfigs[botName]
	return exists
}

func init() {
	log.SetupLogger(log.LoggerConfig{
		Channels:     map[string]log.LoggerLevel{},
		DefaultLevel: log.LevelError,
	})
}

var configFile = flag.String("configFile", "../config.yaml", "Path to config file")

func loadConfig(configFile string) {
	err := configor.New(&configor.Config{
		Silent:               true,
		ErrorOnUnmatchedKeys: true,
	}).Load(&Config, configFile)
	if err != nil {
		panic("error loading config: " + err.Error())
	}
}

func main() {

	loadConfig(*configFile)

	var bots []src.Bot
	var userMap *userMap2.UserMap
	var bhSubscriber *blockhashSubscriber.BlockHashSubscriber

	runBot := func() {
		fmt.Println("Filler Bot is running, and loading wallet keypair.")
		privateKey := Config.Global.KeeperPrivateKey
		wallet, err := src.GetWallet(privateKey)
		if err != nil {
			panic("Could not obtain Wallet")
		}
		fmt.Println("Wallet loaded successfully", wallet.GetPublicKey())

		sdkConfig := drift.Initialize(Config.Global.DriftEnv, nil)
		driftPublicKey := solana.MPK(sdkConfig.DRIFT_PROGRAM_ID)
		fmt.Println("Drift public key successfully obtained.", driftPublicKey.String())

		connectionManager := connection.CreateManager()
		for id, config := range Config.Global.EndPoints {
			connectionManager.AddConfig(config, id)
		}

		bulkAccountLoader := accounts.CreateBulkAccountLoader(
			connectionManager.GetRpc("account"),
			rpc.CommitmentProcessed,
			Config.Global.BulkAccountLoaderPollingInterval,
		)

		opts := go_drift.ConfirmOptions{
			TransactionOpts: rpc.TransactionOpts{
				SkipPreflight:       false,
				PreflightCommitment: rpc.CommitmentFinalized,
			},
			Commitment: rpc.CommitmentProcessed,
		}
		txSender := tx.CreateBaseTxSender(
			connectionManager.GetRpc("submit"),
			solana.Wallet{PrivateKey: wallet.PrivateKey},
			&opts,
			30_000,
		)
		/**
		 * Creating and subscribing to the drift client
		 */

		driftClientConfig := types.DriftClientConfig{
			Connection: connectionManager,
			Wallet:     wallet,
			Env:        Config.Global.DriftEnv,
			ProgramId:  driftPublicKey,
			AccountSubscription: types.DriftClientSubscriptionConfig{
				ResubTimeoutMs: &Config.Global.ResubTimeoutMs,
			},
			Opts: go_drift.ConfirmOptions{
				TransactionOpts: rpc.TransactionOpts{
					PreflightCommitment: rpc.CommitmentFinalized,
				},
				Commitment: rpc.CommitmentProcessed,
			},
			TxSender: txSender,
			SubAccountIds: utils.TT(Config.Global.SubAccounts != nil && len(Config.Global.SubAccounts) > 0,
				Config.Global.SubAccounts,
				[]uint16{},
			),
			ActiveSubAccountId: utils.TTM[uint16](Config.Global.SubAccounts != nil && len(Config.Global.SubAccounts) > 0,
				func() uint16 { return Config.Global.SubAccounts[0] },
				uint16(0),
			),
			PerpMarketIndexes: []uint16{},
			SpotMarketIndexes: []uint16{},
			MarketLookupTable: solana.PublicKey{},
			OracleInfos:       []oracles.OracleInfo{},
			UserStats:         true,
			Authority:         solana.PublicKey{},
		}
		driftClient := drift.CreateDriftClient(driftClientConfig)
		driftClient.EventEmitter.On("error", func(obj ...interface{}) {
			log.Info("clearing house error")
			log.Error(spew.Sdump(obj))
		})

		var eventSubscriber *events.EventSubscriber
		if Config.Global.EventSubscriber {
			eventSubscriber = events.CreateEventSubscriber(
				connectionManager.GetGeyser(Config.Global.EventSubscriberConnectionId),
				driftClient.GetProgram(),
				nil,
			)
		}

		userMap = userMap2.CreateUserMap(types2.UserMapConfig{
			DriftClient: driftClient,
			Connection:  connectionManager.GetGeyser(Config.Global.UserMapConnection),
			SubscriptionConfig: types2.UserMapSubscriptionConfig{
				ResubTimeoutMs: 30_000,
				Commitment:     rpc.CommitmentProcessed,
			},
			SkipInitialLoad:                  true,
			DisableSyncOnTotalAccountsChange: true,
			IncludeIdle:                      false,
		})

		//priorityFeeMethod := Config.Global.PriorityFeeMethod
		//if priorityFeeMethod == "" {
		//	priorityFeeMethod = priorityFee.PriorityFeeMethodSolana
		//}
		//var priorityFeeSubscribers []*priorityFee.PriorityFeeSubscriber

		bhSubscriber = blockhashSubscriber.CreateBlockHashSubscriber(blockhashSubscriber.BlockHashSubscriberConfig{
			Program:        driftClient.GetProgram(),
			ConnectionId:   Config.Global.BlockhashSubscriberConnection,
			ResubTimeoutMs: nil,
			Commitment:     nil,
		})

		dlobSubscriber := dlob.CreateDLOBSubscriber(dloblib.DLOBSubscriptionConfig{
			DriftClient:     driftClient,
			DlobSource:      userMap,
			SlotSource:      bhSubscriber,
			UpdateFrequency: 60_000,
		})
		// sync userstats once
		userStatsLoader := accounts.CreateBulkAccountLoader(
			driftClient.GetProgram().GetProvider().GetConnection(),
			rpc.CommitmentConfirmed,
			0,
		)
		userStatsMap := userMap2.CreateUserStatsMap(driftClient, userStatsLoader)

		if configHasBot("filler") {
			fmt.Println("filler creating")

			fillerConfig, _ := Config.BotConfigs["filler"]
			bots = append(bots, bots2.CreateFillerBot(
				bhSubscriber,
				bulkAccountLoader,
				driftClient,
				userMap,
				userStatsMap,
				Config.Global,
				fillerConfig,
				dlobSubscriber,
			))
		}

		userMap.Sync()
		userMap.Subscribe()

		bhSubscriber.Subscribe()

		if eventSubscriber != nil {
			eventSubscriber.Subscribe()
		}
		dlobSubscriber.Subscribe()

		// Initialize bots
		for _, bot := range bots {
			fmt.Printf("run bot : %v\n", bot)
			bot.Init()
			bot.StartIntervalLoop(1000)
		}
	}

	var wait sync.WaitGroup
	wait.Add(1)
	recursiveTryCatch(bots, runBot)
	wait.Wait()

}

func recursiveTryCatch(bots []src.Bot, f func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("recursive catch error:", err)
		}
	}()
	f()
}
