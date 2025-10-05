package bots

import (
	"context"
	go_drift "driftgo"
	"driftgo/accounts"
	"driftgo/addresses"
	"driftgo/blockhashSubscriber"
	"driftgo/dlob"
	dloblib "driftgo/dlob/types"
	"driftgo/drift"
	types2 "driftgo/drift/types"
	"driftgo/events"
	drift2 "driftgo/lib/drift"
	"driftgo/math"
	"driftgo/types"
	"driftgo/userMap"
	"driftgo/utils"
	"fmt"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	addresslookuptable "github.com/gagliardetto/solana-go/programs/address-lookup-table"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	rpc2 "github.com/gagliardetto/solana-go/rpc"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/shettyh/tlock"
	"keeper-bots-2025/src"
	"keeper-bots-2025/src/bots/common"
	"keeper-bots-2025/src/helper"
	"keeper-bots-2025/src/lib"
	"keeper-bots-2025/src/log"
	math2 "math"
	"math/rand"
	"net/rpc"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type TxType bin.BorshEnum

// todo change string to int

const (
	TxTypeFill TxType = iota
	TxTypeTrigger
	TxTypeSettlePnl
)

func (value TxType) String() string {
	switch value {
	case TxTypeFill:
		return "fill"
	case TxTypeTrigger:
		return "trigger"
	case TxTypeSettlePnl:
		return "settlePnl"
	default:
		return ""
	}
}

type PendingTxSigs struct {
	Ts         int64
	NodeFilled []*dloblib.NodeToFill
	FillTxId   int64
	TxType     TxType
}

type FillerBot struct {
	src.Bot
	name              string
	dryRun            bool
	defaultIntervalMs int64

	bulkAccountLoader              *accounts.BulkAccountLoader
	userStatsMapSubscriptionConfig types2.UserSubscriptionConfig
	driftClient                    *drift.DriftClient
	/// Connection to use specifically for confirming transactions
	txConfirmationConnection *rpc.Client
	pollingIntervalMs        int64
	revertOnFailure          bool
	simulateTxForCUEstimate  bool
	lookupTableAccount       *addresslookuptable.KeyedAddressLookupTable
	// bundleSender *BundleSender
	fillerConfig src.FillerConfig
	globalConfig src.GlobalConfig

	bhSubscriber   *blockhashSubscriber.BlockHashSubscriber
	dlobSubscriber *dlob.DLOBSubscriber

	userMap      *userMap.UserMap
	userStatsMap *userMap.UserStatsMap

	periodicTaskMutex tlock.Lock

	throttledNodes  map[string]int64
	fillingNodes    map[string]int64
	triggeringNodes map[string]int64

	useBurstCULimit    bool
	fillTxSinceBurstCU int64
	fillTxId           int64

	lastSettlePnl int64

	pendingTxSigsToconfirm *expirable.LRU[string, *PendingTxSigs]
	expiredNodesSet        *expirable.LRU[string, bool]
	loopCancels            []func()
	processWaitTimeoutMs   int64
	tryInOrderUpdate       bool
	minIntervalFillMs      int64
	lastFilledAt           int64
	// confirmLoopRunning bool
	// confirmLoopRateLimitTs = Date.now() - CONFIRM_TX_RATE_LIMIT_BACKOFF_MS;

	hasEnoughSolToFill           bool
	rebalanceFiller              bool
	minGasBalacenToFill          uint64
	rebalanceSettledPnlThreshold uint64

	// Mutexes
	mxThrottleNodes   *sync.RWMutex
	mxFillingNodes    *sync.RWMutex
	mxTriggeringNodes *sync.RWMutex
}

func CreateFillerBot(
	bhSubscriber *blockhashSubscriber.BlockHashSubscriber,
	bulkAccountLoader *accounts.BulkAccountLoader,
	driftClient *drift.DriftClient,
	userMap *userMap.UserMap,
	userStastMap *userMap.UserStatsMap,
	globalConfig src.GlobalConfig,
	fillerConfig src.FillerConfig,
	dlobSubscriber *dlob.DLOBSubscriber,
) *FillerBot {
	p := &FillerBot{
		globalConfig:                   globalConfig,
		fillerConfig:                   fillerConfig,
		name:                           fillerConfig.BotId,
		dryRun:                         fillerConfig.DryRun,
		bhSubscriber:                   bhSubscriber,
		driftClient:                    driftClient,
		bulkAccountLoader:              bulkAccountLoader,
		userStatsMapSubscriptionConfig: driftClient.UserAccountSubscriptionConfig,
		pollingIntervalMs:              utils.TT(fillerConfig.FillerPollingInterval == 0, FillerPollingIntervalDefault, fillerConfig.FillerPollingInterval),
		userMap:                        userMap,
		userStatsMap:                   userStastMap,
		revertOnFailure:                fillerConfig.RevertOnFailure,
		simulateTxForCUEstimate:        fillerConfig.SimulateTxForCUEstimate,
		dlobSubscriber:                 dlobSubscriber,
		pendingTxSigsToconfirm:         expirable.NewLRU[string, *PendingTxSigs](CacheLimitSize, nil, TX_TIMEOUT_THRESHOLD_MS), //todo question? LRU
		expiredNodesSet:                expirable.NewLRU[string, bool](CacheLimitSize, nil, TX_TIMEOUT_THRESHOLD_MS),
		tryInOrderUpdate:               fillerConfig.TryOnOrderUpdate,
		minIntervalFillMs:              max(MIN_INTERVAL_FILL_MS_DEFAULT, fillerConfig.MinIntervalFillMs),
		processWaitTimeoutMs:           fillerConfig.ProcessWaitTimeoutMs,
		rebalanceFiller:                fillerConfig.RebalanceFilter,
		minGasBalacenToFill: utils.TT(
			!src.ValidMinimumGasAmount(fillerConfig.MinGasBalanceToFill),
			solana.LAMPORTS_PER_SOL/5, uint64(float64(solana.LAMPORTS_PER_SOL)*fillerConfig.MinGasBalanceToFill),
		),
		rebalanceSettledPnlThreshold: utils.TT(
			!src.ValidRebalanceSettledPnlThreshold(fillerConfig.RebalanceSettledPnlThreshold),
			REBALANCE_SETTLED_PNL_THRESHOLD_DEFAULT,
			uint64(fillerConfig.RebalanceSettledPnlThreshold),
		),
		periodicTaskMutex: tlock.New(),
		throttledNodes:    make(map[string]int64),
		fillingNodes:      make(map[string]int64),
		triggeringNodes:   make(map[string]int64),

		lastSettlePnl:     time.Now().Unix() - SETTLE_POSITIVE_PNL_COOLDOWN_MS,
		mxThrottleNodes:   new(sync.RWMutex),
		mxFillingNodes:    new(sync.RWMutex),
		mxTriggeringNodes: new(sync.RWMutex),
	}
	return p
}

func (p *FillerBot) log() *log.LogChannel {
	return log.Channel("filler")
}

func (p *FillerBot) Init() {
	p.baseInit()
	if p.dlobSubscriber != nil {
		p.dlobSubscriber = dlob.CreateDLOBSubscriber(dloblib.DLOBSubscriptionConfig{
			DriftClient:     p.driftClient,
			DlobSource:      p.userMap,
			SlotSource:      p.bhSubscriber,
			UpdateFrequency: max(DLOB_UPDATE_FREQUENCY_DEFAULT, p.pollingIntervalMs-500), //todo question? 500
		})
	}
	p.dlobSubscriber.Subscribe()
}

func (p *FillerBot) baseInit() {
	if p.userStatsMap == nil {
		startInitUserStasMap := src.NowMs()
		p.log().Info("Initializing user stats map")

		userStasLoader := accounts.CreateBulkAccountLoader(
			p.driftClient.GetProgram().GetProvider().GetConnection(),
			rpc2.CommitmentConfirmed,
			0,
		)
		p.userStatsMap = userMap.CreateUserStatsMap(p.driftClient, userStasLoader)

		p.log().Info("User stats map: %d initialized in %d ms", p.userStatsMap.Size(), src.NowMs()-startInitUserStasMap)
	}
	p.lookupTableAccount = p.driftClient.FetchMarketLookupTableAccount()
}

func (p *FillerBot) StartIntervalLoop(intervalMs int64) {
	if p.tryInOrderUpdate {
		go_drift.EventEmitter().On(go_drift.EventLibUserOrderUpdated, func(data ...interface{}) {
			if p.lastFilledAt == 0 || time.Now().Unix()-p.lastFilledAt > p.minIntervalFillMs {
				go p.tryFill()
			}
			userAccountKey := data[0].(string)
			slot := data[2].(uint64)
			txnHash := data[3].(string)
			p.log().Info("User order updated: %s, slot: %d, txnHash: %s", userAccountKey, slot, txnHash)
		})
		go_drift.EventEmitter().On(go_drift.EventLibNewOrderRecords, func(data ...interface{}) {
			if p.lastFilledAt == 0 || time.Now().UnixMilli()-p.lastFilledAt > p.minIntervalFillMs {
				go p.tryFill()
			}
		})
		go_drift.EventEmitter().On(go_drift.EventLibUserAccountUpdated, func(data ...interface{}) {
			userAccountKey := data[0].(string)
			slot := data[2].(uint64)
			txnHash := data[3].(string)
			p.log().Info("User account updated: %s, slot: %d, txnHash: %s", userAccountKey, slot, txnHash)
		})
		go_drift.EventEmitter().On(go_drift.EventLibOrderEvent, func(data ...interface{}) {
			event := data[0].(*events.WrappedEvent)
			slot := event.Slot
			txSig := event.TxSig.String()
			if event.EventType == events.EventTypeOrderRecord {
				record := event.Data.(*drift2.OrderRecord)
				if p.IsDebugTargetMarket(record.Order.MarketIndex) {
					helper.DumpOrderRecord(record, slot, txSig)
				}
			} else if event.EventType == events.EventTypeOrderActionRecord {
				record := event.Data.(*drift2.OrderActionRecord)
				if p.IsDebugTargetMarket(record.MarketIndex) {
					helper.DumpOrderActionRecord(record, slot, txSig)
				}
			}
		})
	}

	go_drift.EventEmitter().On("newTransactionLogs", func(data ...interface{}) {
		txSig := data[0].(solana.Signature)
		logs := data[2].([]string)
		pendingTxQueue, exists := p.pendingTxSigsToconfirm.Get(txSig.String())
		if exists && pendingTxQueue != nil {
			p.log().Debug("Transaction: %s, Duration: %d ms", txSig.String(), src.NowMs()-pendingTxQueue.Ts)
			p.pendingTxSigsToconfirm.Remove(txSig.String())
			p.handleTransactionLogs(pendingTxQueue.NodeFilled, logs)
		}
	})
	p.log().Info("%s bot started", p.name)
}

func (p *FillerBot) IsDebugTargetMarket(marketIndex uint16) bool {
	return p.globalConfig.Debug && p.globalConfig.DebugOption.TargetTrace && p.globalConfig.DebugOption.TargetMarketIndex == marketIndex
}

func (p *FillerBot) handleTransactionLogs(nodesFilled []*dloblib.NodeToFill, logs []string) (int, bool) {
	if len(logs) == 0 {
		return 0, false
	}
	inFillIx := false
	errorThisFillIx := false
	ixIdx := -1
	successCount := 0
	burstedCU := false
	for _, line := range logs {
		if len(line) == 0 {
			continue
		}
		if strings.Contains(line, "exceeded maximum number of instructions allowed") {
			p.log().Warning("Using bursted CU limit")
			p.useBurstCULimit = true
			p.fillTxSinceBurstCU = 0
			burstedCU = true
			continue
		}
		if common.IsEndIxLog(p.driftClient.GetProgram().GetProgramId().String(), line) {
			if !errorThisFillIx {
				successCount++
			}
			inFillIx = false
			errorThisFillIx = false
			continue
		}
		if common.IsIxLog(line) {
			if common.IsFillIxLog(line) {
				inFillIx = true
				errorThisFillIx = false
				ixIdx++
			} else {
				inFillIx = false
			}
			continue
		}
		if !inFillIx {
			// this is not a log for a fill instruction
			continue
		}

		orderIdDoesNotExist, _ := common.IsOrderDoesNotExistLog(line)
		if orderIdDoesNotExist {
			if len(nodesFilled) > ixIdx {
				filledNode := nodesFilled[ixIdx]
				isExpired := math.IsOrderExpired(
					filledNode.Node.GetOrder(),
					src.Now(),
					true,
				)

				p.log().Error(
					"assoc node (ixIdx: %d): %s, %d; does not exist (filled by someone else); %s, expired: %s, orderTs: %d, now: %d",
					ixIdx,
					filledNode.Node.GetUserAccount(),
					line,
					utils.TT(isExpired, "Yes", "No"),
					filledNode.Node.GetOrder().MaxTs,
					src.Now(),
				)
				if isExpired {
					sig := src.GetNodeToFillSignature(filledNode)
					p.expiredNodesSet.Add(sig, true)
				}
				//p.setThrottledNode(src.GetNodeToFillSignature(filledNode))
			}
			errorThisFillIx = true
			continue
		}

		makerBreachedMaintenanceMargin := common.IsMakerBreachedMaintenanceMarginLog(line)
		if len(makerBreachedMaintenanceMargin) > 0 {
			p.log().Error("Throttling maker breached maintenance margin: %s", makerBreachedMaintenanceMargin)
			p.setThrottledNode(makerBreachedMaintenanceMargin)
			errorThisFillIx = true

			makerUser := p.getUserAccountAndSlotFromMap(makerBreachedMaintenanceMargin)
			if makerUser != nil {
				txSig, err := p.driftClient.ForceCancelOrders(
					solana.MPK(makerBreachedMaintenanceMargin),
					makerUser.Data,
					nil,
					nil,
				)
				if err != nil {
					p.log().Info("Force cancelled orders for makers due to breach of maintenance margin. Tx: %s", txSig.String())
				} else {
					p.log().Error("Failed to send ForceCancelOrder Tx (error %s), maker (%s) breach maint margin:", err.Error(), makerBreachedMaintenanceMargin)
				}
			}
			break
		}

		takerBreachedMaintenanceMargin := common.IsTakerBreachedMaintenanceMarginLog(line)
		if takerBreachedMaintenanceMargin && len(nodesFilled) > ixIdx {
			filledNode := nodesFilled[ixIdx]
			takerNodeSignature := filledNode.Node.GetUserAccount()
			p.log().Error("taker breach maint. margin, assoc node (ixIdx: %d): %s, %d; (throttling %s and force cancelling orders); %s",
				ixIdx,
				filledNode.Node.GetUserAccount(),
				filledNode.Node.GetOrder().OrderId,
				takerNodeSignature,
				line,
			)
			p.setThrottledNode(takerNodeSignature)
			errorThisFillIx = true

			takerUser := p.getUserAccountAndSlotFromMap(filledNode.Node.GetUserAccount())
			if takerUser != nil {
				txSig, err := p.driftClient.ForceCancelOrders(
					solana.MPK(filledNode.Node.GetUserAccount()),
					takerUser.Data,
					nil,
					nil,
				)
				if err != nil {
					p.log().Info("Force cancelled orders for user %s due to breach of maintenance margin. Tx: %s", filledNode.Node.GetUserAccount(), txSig.String())
				} else {
					p.log().Error("Failed to send ForceCancelOrder Tx (error %s), taker (%s - %d) breach maint margin:", err.Error(), filledNode.Node.GetUserAccount(), filledNode.Node.GetOrder().OrderId)
				}
			}
			continue
		}

		orderId, userAcc, errFillingLog := common.IsErrFillingLog(line)
		if errFillingLog {
			extractedSig := src.GetFillSignatureFromUserAccountAndOrderId(
				userAcc,
				orderId,
			)
			p.setThrottledNode(extractedSig)

			if len(nodesFilled) > ixIdx {
				filledNode := nodesFilled[ixIdx]
				assocNodeSig := src.GetNodeToFillSignature(filledNode)
				p.log().Warning(
					"Throttling node due to fill error. extractedSig: %s, assocNodeSig: %s, assocNodeIdx: %d",
					extractedSig, assocNodeSig, ixIdx)
				errorThisFillIx = true
				continue
			}
		}

		if common.IsErrStaleOracle(line) {
			p.log().Error("Stale oracle error: %s", line)
			errorThisFillIx = true
			continue
		}
	}

	if !burstedCU {
		if p.fillTxSinceBurstCU > TX_COUNT_COOLDOWN_ON_BURST {
			p.useBurstCULimit = false
		}
		p.fillTxSinceBurstCU += 1
	}

	if len(logs) > 0 {
		if strings.Contains(logs[len(logs)-1], "exceeded CUs meter at BPF instruction") {
			return successCount, true
		}
	}
	return successCount, false
}

func (p *FillerBot) setThrottledNode(signature string) {
	defer p.mxThrottleNodes.Unlock()
	p.mxThrottleNodes.Lock()
	p.throttledNodes[signature] = src.NowMs()
}

func (p *FillerBot) removeFillingNodes(nodes []*dloblib.NodeToFill) {
	defer p.mxFillingNodes.Unlock()
	p.mxFillingNodes.Lock()
	for _, node := range nodes {
		delete(p.fillingNodes, src.GetNodeToFillSignature(node))
	}
}

func (p *FillerBot) getUserAccountAndSlotFromMap(key string) *accounts.DataAndSlot[*drift2.User] {
	user := p.userMap.MustGetWithSlot(key, &p.driftClient.UserAccountSubscriptionConfig)
	return &accounts.DataAndSlot[*drift2.User]{
		Data: user.Data.GetUserAccount(),
		Slot: user.Slot,
	}
}

func (p *FillerBot) tryFill() {
	// todo confirm with typescript sdk
	if !p.periodicTaskMutex.TryLockWithTimeout(time.Millisecond * time.Duration(p.processWaitTimeoutMs)) {
		return
	}
	defer p.periodicTaskMutex.Unlock()
	p.lastFilledAt = time.Now().UnixMilli()
	dlob := p.getDLOB()
	p.pruneThrottleNode()
	fillableNodes, triggerableNodes := p.getFillableNodes(dlob)
	if len(fillableNodes) == 0 && len(triggerableNodes) == 0 {
		return
	}
	var wait sync.WaitGroup
	wait.Add(2)

	go func() {
		p.executeFillablePerpNodesForMarket(fillableNodes, false)
		wait.Done()
	}()
	go func() {
		p.executeTriggerablePerpNodesForMarket(triggerableNodes, false)
		wait.Done()
	}()
	wait.Wait()
}

func (p *FillerBot) getDLOB() dloblib.IDLOB {
	return p.dlobSubscriber.GetDLOB()
}

func (p *FillerBot) pruneThrottleNode() {
	defer p.mxThrottleNodes.Unlock()
	p.mxThrottleNodes.Lock()
	if len(p.throttledNodes) > THROTTLED_NODE_SIZE_TO_PRUNE {
		for key, value := range p.throttledNodes {
			if value+2*FILL_ORDER_THROTTLE_BACKOFF > src.NowMs() {
				delete(p.throttledNodes, key)
			}
		}
	}
}

func (p *FillerBot) getFillableNodes(
	dlob dloblib.IDLOB,
) (
	[]*dloblib.NodeToFill,
	[]*dloblib.NodeToTrigger,
) {
	var fillableNodes []*dloblib.NodeToFill
	var triggerableNodes []*dloblib.NodeToTrigger
	fmt.Println("Getting fillable nodes")
	for _, market := range p.driftClient.GetPerpMarketAccounts() {
		nodesToFill, nodesToTrigger := p.getPerpNodesForMarket(market, dlob)
		if len(nodesToFill) > 0 {
			fillableNodes = append(fillableNodes, nodesToFill...)
		}
		if len(nodesToTrigger) > 0 {
			triggerableNodes = append(triggerableNodes, nodesToTrigger...)
		}
	}
	return p.filterPerpNodesForMarket(fillableNodes, triggerableNodes)
}

func (p *FillerBot) getPerpNodesForMarket(
	market *drift2.PerpMarket,
	dlob dloblib.IDLOB,
) ([]*dloblib.NodeToFill, []*dloblib.NodeToTrigger) {
	marketIndex := market.MarketIndex

	oraclePriceData := p.driftClient.GetOracleDataForPerpMarket(marketIndex)
	if oraclePriceData == nil || oraclePriceData.Price == nil {
		fmt.Printf("oracle price for market %d is nil\n", marketIndex)
		return []*dloblib.NodeToFill{}, []*dloblib.NodeToTrigger{}
	}
	vAsk := math.CalculateAskPrice(market, oraclePriceData)
	vBid := math.CalculateBidPrice(market, oraclePriceData)

	fillSlot := p.getMaxSlot()

	return dlob.FindNodesToFill(
			marketIndex,
			vBid,
			vAsk,
			fillSlot,
			src.Now(),
			drift2.MarketType_Perp,
			oraclePriceData,
			p.driftClient.GetStateAccount(),
			&types.MarketAccount{
				PerpMarketAccount: p.driftClient.GetPerpMarketAccount(marketIndex),
			},
		), dlob.FindNodesToTrigger(
			marketIndex,
			fillSlot,
			oraclePriceData.Price,
			drift2.MarketType_Perp,
			p.driftClient.GetStateAccount(),
		)
}

func (p *FillerBot) getMaxSlot() uint64 {
	return max(p.bhSubscriber.GetSlot(), p.userMap.GetSlot())
}

func (p *FillerBot) filterPerpNodesForMarket(
	fillableNodes []*dloblib.NodeToFill,
	triggerableNodes []*dloblib.NodeToTrigger,
) ([]*dloblib.NodeToFill, []*dloblib.NodeToTrigger) {
	seenFillableNodes := make(map[string]bool)
	filteredFillableNodes := make([]*dloblib.NodeToFill, 0)

	for _, node := range fillableNodes {
		sig := src.GetNodeToFillSignature(node)
		_, exists := seenFillableNodes[sig]
		if exists {
			continue
		}
		seenFillableNodes[sig] = true
		if p.filterFillableNodes(node) {
			filteredFillableNodes = append(filteredFillableNodes, node)
		}
	}

	seenTriggerableNodes := make(map[string]bool)
	filteredTriggerableNodes := make([]*dloblib.NodeToTrigger, 0)

	for _, node := range triggerableNodes {
		sig := src.GetNodeToTriggerSignature(node)
		_, exists := seenTriggerableNodes[sig]
		if exists {
			continue
		}
		seenTriggerableNodes[sig] = true
		if p.filterTriggerableNodes(node) {
			filteredTriggerableNodes = append(filteredTriggerableNodes, node)
		}
	}
	return filteredFillableNodes, filteredTriggerableNodes
}

func (p *FillerBot) filterFillableNodes(nodeToFill *dloblib.NodeToFill) bool {
	if nodeToFill.Node.GetOrder() == nil {
		return false
	}

	if nodeToFill.Node.IsVammNode() {
		//p.log().Warning(
		//	"filtered out a vAMM node on market %d for user %s - %d",
		//	nodeToFill.Node.GetOrder().MarketIndex,
		//	nodeToFill.Node.GetUserAccount(),
		//	nodeToFill.Node.GetOrder().OrderId,
		//)
		return false
	}

	if nodeToFill.Node.IsHaveFilled() || nodeToFill.Node.IsBaseFilled() {
		//p.log().Warning(
		//	"filtered out filled node on market %d for user %s-%d",
		//	nodeToFill.Node.GetOrder().MarketIndex,
		//	nodeToFill.Node.GetUserAccount(),
		//	nodeToFill.Node.GetOrder().OrderId,
		//)
		return false
	}

	if p.checkIfNodeUserIsBot(nodeToFill.Node) {
		//fmt.Println("Bot detected : ", nodeToFill.Node.GetUserAccount())
		return false
	}

	if p.checkIfOrderProperlyFilled(nodeToFill.Node) {
		//fmt.Printf("Node : fully filled (orderId : %d), %d - %d\n",
		//	nodeToFill.Node.GetOrder().OrderId,
		//	nodeToFill.Node.GetOrder().BaseAssetAmountFilled,
		//	nodeToFill.Node.GetOrder().BaseAssetAmount,
		//)
		return false
	}
	if p.checkIfNodeTriedTooMuch(nodeToFill.Node) {
		return false
	}

	now := src.NowMs()
	nodeToFillSignature := src.GetNodeToFillSignature(nodeToFill)

	timeStartedToFillNode, exists := p.checkFillingNode(nodeToFillSignature)
	if exists {
		if timeStartedToFillNode+FILL_ORDER_THROTTLE_BACKOFF > now {
			// still cooling down on this node, filter it out
			return false
		}
	}

	// expired orders that we previously tried to fill
	if p.expiredNodesSet.Contains(nodeToFillSignature) {
		return false
	}

	// check if taker node is throttled
	if p.isDLOBNodeThrottled(nodeToFill.Node) {
		return false
	}

	marketIndex := nodeToFill.Node.GetOrder().MarketIndex
	oraclePriceData := p.driftClient.GetOracleDataForPerpMarket(marketIndex)

	if math.IsOrderExpired(nodeToFill.Node.GetOrder(), src.Now(), true) {
		if nodeToFill.Node.GetOrder().OrderType == drift2.OrderType_Limit {
			// do not try to fill (expire) limit orders b/c they will auto expire when filled against
			// or the user places a new order
			return false
		}
		return true
	}

	if len(nodeToFill.MakerNodes) == 0 &&
		nodeToFill.Node.GetOrder().MarketType == drift2.MarketType_Perp &&
		!math.IsFillableByVAMM(
			nodeToFill.Node.GetOrder(),
			p.driftClient.GetPerpMarketAccount(
				nodeToFill.Node.GetOrder().MarketIndex,
			),
			oraclePriceData,
			p.getMaxSlot(),
			src.Now(),
			int(p.driftClient.GetStateAccount().MinPerpAuctionDuration),
		) {
		//p.log().Warning("filtered out unfillable node on market %d for user %s-%d (order type : %d, trigger condition: )",
		//	nodeToFill.Node.GetOrder().MarketIndex,
		//	nodeToFill.Node.GetUserAccount(),
		//	nodeToFill.Node.GetOrder().OrderId,
		//	nodeToFill.Node.GetOrder().OrderType,
		//	nodeToFill.Node.GetOrder().TriggerCondition)
		//p.log().Warning(" . no maker node: %d", len(nodeToFill.MakerNodes))
		//p.log().Warning(" . is perp: %d", nodeToFill.Node.GetOrder().MarketType)
		//p.log().Warning(" . is not fillable by vamm: false")
		//p.log().Warning(" .     calculateBaseAssetAmountForAmmToFulfill: %s", math.CalculateBaseAssetAmountForAmmToFulfill(
		//	nodeToFill.Node.GetOrder(),
		//	p.driftClient.GetPerpMarketAccount(nodeToFill.Node.GetOrder().MarketIndex),
		//	oraclePriceData,
		//	p.getMaxSlot(),
		//).String(),
		//)
		return false
	}

	perpMarket := p.driftClient.GetPerpMarketAccount(
		nodeToFill.Node.GetOrder().MarketIndex,
	)
	// if making with vAMM, ensure valid oracle
	if len(nodeToFill.MakerNodes) == 0 &&
		perpMarket.Amm.OracleSource != drift2.OracleSource_Prelaunch {
		oracleIsValid := math.IsOracleValid(
			perpMarket,
			oraclePriceData,
			&p.driftClient.GetStateAccount().OracleGuardRails,
			p.getMaxSlot(),
		)
		if !oracleIsValid {
			p.log().Error("Oracle is not valid for market %d, skipping fill with vAMM, oraclePrice : %s", marketIndex, oraclePriceData.Price.String())
			return false
		}
	}
	return true
}

func (p *FillerBot) filterTriggerableNodes(nodeToTrigger *dloblib.NodeToTrigger) bool {
	if nodeToTrigger.Node.IsHaveTrigger() {
		return true
	}
	if p.checkIfNodeUserIsBot(nodeToTrigger.Node) {
		return false
	}

	if p.checkIfNodeTriedTooMuch(nodeToTrigger.Node) {
		return false
	}

	now := src.NowMs()
	nodeToFillSignature := src.GetNodeToTriggerSignature(nodeToTrigger)
	timeStartedToTriggerNode, exists := p.checkTriggeringNode(nodeToFillSignature)
	if exists && timeStartedToTriggerNode > 0 {
		if timeStartedToTriggerNode+TRIGGER_ORDER_COOLDOWN_MS > now {
			return false
		}
	}
	return true
}

func (p *FillerBot) checkIfNodeUserIsBot(node dloblib.IDLOBNode) bool {
	if p.fillerConfig.MinOrderUpdateTimeMs <= 0 {
		return false
	}

	userStatus := p.userMap.GetUserStatus(node.GetUserAccount())
	if userStatus == nil {
		return false
	}
	if userStatus.UpdatedCount < 50 {
		return false
	}
	if userStatus.UpdateRate >= p.fillerConfig.MinOrderUpdateTimeMs {
		//fmt.Println("Bot detected : ", node.GetUserAccount())
		return false
	}
	return true
}

func (p *FillerBot) checkIfNodeTriedTooMuch(node dloblib.IDLOBNode) bool {
	if p.fillerConfig.FilterTryMuchCount == 0 {
		return false
	}
	return node.GetTryFill() > p.fillerConfig.FilterTryMuchCount
}

func (p *FillerBot) checkIfOrderProperlyFilled(node dloblib.IDLOBNode) bool {
	return false
	//order := node.GetOrder()
	//filledPercent := order.BaseAssetAmountFilled * 100 / order.BaseAssetAmount
	//if filledPercent > 50 {
	//	return true
	//}
	//return false
}

func (p *FillerBot) checkFillingNode(nodeSig string) (int64, bool) {
	defer p.mxFillingNodes.RUnlock()
	p.mxFillingNodes.RLock()
	ts, exists := p.fillingNodes[nodeSig]
	return ts, exists
}

func (p *FillerBot) setTriggeringNodes(nodeSig string) {
	defer p.mxTriggeringNodes.Unlock()
	p.mxTriggeringNodes.Lock()
	p.triggeringNodes[nodeSig] = time.Now().UnixMilli()
}

func (p *FillerBot) checkTriggeringNode(nodeSig string) (int64, bool) {
	defer p.mxTriggeringNodes.RUnlock()
	p.mxTriggeringNodes.RLock()
	ts, exists := p.triggeringNodes[nodeSig]
	return ts, exists
}

func (p *FillerBot) removeTriggeringNodes(node *dloblib.NodeToTrigger) {
	defer p.mxTriggeringNodes.Unlock()
	p.mxTriggeringNodes.Lock()
	delete(p.triggeringNodes, src.GetNodeToTriggerSignature(node))
}

func (p *FillerBot) isDLOBNodeThrottled(dlobNode dloblib.IDLOBNode) bool {
	if dlobNode.GetUserAccount() == "" || dlobNode.GetOrder() == nil {
		return false
	}
	defer p.mxThrottleNodes.RUnlock()
	p.mxThrottleNodes.RLock()

	// first check if the userAccount itself is throttled
	userAccountPubkey := dlobNode.GetUserAccount()
	_, exists := p.throttledNodes[userAccountPubkey]
	if exists {
		if p.isThrottledNodeStillThrottled(userAccountPubkey) {
			return true
		} else {
			return false
		}
	}

	// then check if the specific order is throttled
	orderSignature := src.GetFillSignatureFromUserAccountAndOrderId(
		dlobNode.GetUserAccount(),
		dlobNode.GetOrder().OrderId,
	)
	_, exists = p.throttledNodes[orderSignature]
	if exists {
		if p.isThrottledNodeStillThrottled(orderSignature) {
			return true
		} else {
			return false
		}
	}

	return false
}

func (p *FillerBot) isThrottledNodeStillThrottled(throttleKey string) bool {
	lastFillAttempt, exists := p.throttledNodes[throttleKey]
	if exists {
		if lastFillAttempt+FILL_ORDER_THROTTLE_BACKOFF > src.NowMs() {
			return true
		} else {
			p.clearThrottledNode(throttleKey)
			return false
		}
	} else {
		return false
	}
}

func (p *FillerBot) clearThrottledNode(throttleKey string) {
	delete(p.throttledNodes, throttleKey)
}

func (p *FillerBot) executeFillablePerpNodesForMarket(
	fillableNodes []*dloblib.NodeToFill,
	buildForBundle bool,
) {
	p.tryBulkFillPerpNodes(fillableNodes, buildForBundle)
}

func (p *FillerBot) executeTriggerablePerpNodesForMarket(
	triggerableNodes []*dloblib.NodeToTrigger,
	buildForBundle bool,
) {
	for _, nodeToTrigger := range triggerableNodes {
		p.tryTriggerPerNodeForMarket(nodeToTrigger, buildForBundle)
	}
}

func (p *FillerBot) tryBulkFillPerpNodes(
	nodesToFill []*dloblib.NodeToFill,
	buildForBundle bool,
) int {
	nodesSent := 0
	marketNodeMap := make(map[uint16][]*dloblib.NodeToFill)

	for _, nodeToFill := range nodesToFill {
		if nodeToFill.Node.GetOrder() == nil {
			continue
		}
		marketIndex := nodeToFill.Node.GetOrder().MarketIndex
		_, exists := marketNodeMap[marketIndex]
		if !exists {
			marketNodeMap[marketIndex] = make([]*dloblib.NodeToFill, 0)

		}
		marketNodeMap[marketIndex] = append(marketNodeMap[marketIndex], nodeToFill)
	}
	if testing.Testing() {
		//spew.Dump(marketNodeMap)
	}
	mutex := new(sync.RWMutex)
	wait := new(sync.WaitGroup)
	wait.Add(len(marketNodeMap))
	p.log().Debug("tryBulkFillPerpNodes: %d", len(marketNodeMap))
	for marketIndex, nodesToFillForMarket := range marketNodeMap {
		//nodesSent += p.tryBulkFillPerpNodesForMarket(nodesToFillForMarket)
		nodestofillformarketTemp := nodesToFillForMarket
		go func() {
			sent := p.tryBulkFillPerpNodesForMarket(marketIndex, nodestofillformarketTemp, buildForBundle)
			mutex.Lock()
			nodesSent += sent
			mutex.Unlock()
			wait.Done()
		}()
	}
	wait.Wait()
	return nodesSent
}

func (p *FillerBot) tryBulkFillPerpNodesForMarket(
	marketIndex uint16,
	nodesToFill []*dloblib.NodeToFill,
	buildForBundle bool,
) int {
	var ixs []solana.Instruction
	ixs = append(ixs, computebudget.NewSetComputeUnitLimitInstructionBuilder().SetUnits(COMPUTE_UNIT_LIMIT).Build())
	minLamports := uint64(MIN_LAMPORT_DEFAULT)
	ixs = append(ixs, computebudget.NewSetComputeUnitPriceInstructionBuilder().SetMicroLamports(minLamports).Build())

	/**
	 * At all times, the running Tx size is:
	 * - signatures (compact-u16 array, 64 bytes per elem)
	 * - message header (3 bytes)
	 * - affected accounts (compact-u16 array, 32 bytes per elem)
	 * - previous block hash (32 bytes)
	 * - message instructions (
	 * 		- programIdIdx (1 byte)
	 * 		- accountsIdx (compact-u16, 1 byte per elem)
	 *		- instruction data (compact-u16, 1 byte per elem)
	 */
	runningTxSize := uint64(0)
	runningCUUsed := uint64(0)

	uniqueAccounts := make(map[string]bool)
	uniqueAccounts[p.driftClient.Wallet.GetPublicKey().String()] = true

	computeBudgetIx := ixs[0]
	for _, accountMeta := range computeBudgetIx.Accounts() {
		uniqueAccounts[accountMeta.PublicKey.String()] = true
	}
	uniqueAccounts[computeBudgetIx.ProgramID().String()] = true
	// initialize the barebones transaction
	// signatures
	runningTxSize += p.calcCompactU16EncodedSizeX(uint64(1), 64)
	// message header
	runningTxSize += 3
	// accounts
	runningTxSize += p.calcCompactU16EncodedSizeX(uint64(len(uniqueAccounts)), 32)
	// block hash
	runningTxSize += 32
	runningTxSize += p.calcIxEncodedSize(computeBudgetIx)

	nodesSent := make([]*dloblib.NodeToFill, 0)
	idxUsed := uint64(0)
	startingIxSize := uint64(len(ixs))
	p.fillTxId++
	fillTxId := p.fillTxId

	if p.IsDebugTargetMarket(marketIndex) {
		for _, nodeToFill := range nodesToFill {
			makerInfos, takerUserPubkey, _, _, _, _ :=
				p.getNodeFillInfo(nodeToFill)

			if makerInfos != nil && len(makerInfos) > 0 {
				makerOrders := ""
				for _, makerInfo := range makerInfos {
					if makerInfo != nil && makerInfo.Data.Order != nil {
						if len(makerOrders) > 0 {
							makerOrders = fmt.Sprintf("%s,%d", makerOrders, makerInfo.Data.Order.OrderId)
						} else {
							makerOrders = fmt.Sprintf("%d", makerInfo.Data.Order.OrderId)
						}
					}
				}
				p.log().Info(
					fmt.Sprintf("Filler TXN%d: market: %d taker: %s Order%d, maker: %s",
						fillTxId, marketIndex,
						takerUserPubkey, nodeToFill.Node.GetOrder().OrderId,
						makerOrders,
					))
			} else {
				p.log().Info(
					fmt.Sprintf(
						"Filler TXN%d: market: %d taker: %s Order%d, maker null",
						fillTxId, marketIndex,
						takerUserPubkey, nodeToFill.Node.GetOrder().OrderId,
					))
			}
		}
	}

	for _, nodeToFill := range nodesToFill {
		// do multi maker fills in a separate tx since they're larger
		if len(nodeToFill.MakerNodes) > 1 {
			p.tryFillMultiMakerPerpNodes(nodeToFill, buildForBundle)
			nodesSent = append(nodesSent, nodeToFill)
			continue
		}
		// otherwise pack fill ixs until est. tx size or CU limit is hit
		makerInfos, takerUserPubkey, takerUser, _, referrerInfo, marketType :=
			p.getNodeFillInfo(nodeToFill)
		//p.logMessageForNodeToFill
		p.logSlots()

		if marketType != drift2.MarketType_Perp {
			continue
		}
		if nodeToFill.Node.GetOrder() == nil {
			continue
		}

		ix := p.driftClient.GetFillPerpOrderIx(
			addresses.GetUserAccountPublicKey(
				p.driftClient.Program.GetProgramId(),
				takerUser.Authority,
				takerUser.SubAccountId,
			),
			takerUser,
			nodeToFill.Node.GetOrder(),
			utils.ValuesFunc(makerInfos, func(t *accounts.DataAndSlot[*go_drift.MakerInfo]) *go_drift.MakerInfo {
				return t.Data
			}),
			referrerInfo,
			nil,
		)
		if ix == nil {
			p.log().Error("failed to generate an ix")
			break
		}

		p.setFillingNode(src.GetNodeToFillSignature(nodeToFill))

		// first estimate new tx size with this additional ix and new accounts
		var ixKeys []solana.PublicKey
		for _, accountMeta := range ix.Accounts() {
			ixKeys = append(ixKeys, accountMeta.PublicKey)
		}
		newAccounts := slices.DeleteFunc(append(ixKeys, ix.ProgramID()), func(key solana.PublicKey) bool {
			_, exists := uniqueAccounts[key.String()]
			return !exists
		})
		newIxCost := p.calcIxEncodedSize(ix)
		additionalAccountsCost := uint64(0)
		if len(newAccounts) > 0 {
			additionalAccountsCost = p.calcCompactU16EncodedSizeX(uint64(len(newAccounts)), 32) - 1
		}
		// We have to use MAX_TX_PACK_SIZE because it appears we cannot send tx with a size of exactly 1232 bytes.
		// Also, some logs may get truncated near the end of the tx, so we need to leave some room for that.
		cuToUsePerFill := CU_PER_FILL
		if p.useBurstCULimit {
			cuToUsePerFill = BURST_CU_PER_FILL
		}
		//fmt.Println("txCost : ", runningTxSize+newIxCost+additionalAccountsCost)
		if (runningTxSize+newIxCost+additionalAccountsCost >=
			MAX_TX_PACK_SIZE ||
			runningCUUsed+uint64(cuToUsePerFill) >= MAX_CU_PER_TX) &&
			uint64(len(ixs)) > startingIxSize+1 {
			p.log().Info("Fully packed fill tx (ixs: %d): est. tx size %d, max: %d, est. CU used: expected %d, max: %d, (fillTxId: %d)",
				len(ixs),
				runningTxSize+newIxCost+additionalAccountsCost,
				MAX_TX_PACK_SIZE,
				runningCUUsed+uint64(cuToUsePerFill),
				MAX_CU_PER_TX,
				fillTxId,
			)
			break
		}
		// add to tx
		p.log().Info("including taker %s (fillTxId: %d)", takerUserPubkey, fillTxId)
		ixs = append(ixs, ix)
		runningTxSize += newIxCost + additionalAccountsCost
		runningCUUsed += uint64(cuToUsePerFill)
		for _, newAccount := range newAccounts {
			uniqueAccounts[newAccount.String()] = true
		}
		idxUsed++
		nodesSent = append(nodesSent, nodeToFill)
	}

	if idxUsed == 0 {
		return len(nodesSent)
	}

	if len(nodesSent) == 0 {
		return 0
	}

	if p.revertOnFailure {
		ixs = append(ixs, p.driftClient.GetRevertFillIx(nil))
	}

	simResult, err := lib.SimulateAndGetTxWithCUsX(lib.SimulateAndGetTxWithCUsParams{
		Connection:          p.driftClient.Rpc,
		Payer:               p.driftClient.Wallet.GetWallet(),
		LookupTableAccounts: []addresslookuptable.KeyedAddressLookupTable{*p.lookupTableAccount},
		Ixs:                 ixs,
		CuLimitMultiplier:   utils.NewPtr(SIM_CU_ESTIMATE_MULTIPLIER),
		DoSimulation:        utils.NewPtr(p.simulateTxForCUEstimate),
		RecentBlockhash:     utils.NewPtr(p.getBlockhashForTx()),
		DumpTx:              utils.NewPtr(DUMP_TXS_IN_SIM),
	})
	//simResult, err := lib.SimulateAndGetTxWithCUs(
	//	ixs,
	//	p.driftClient.Rpc,
	//	p.driftClient.TxSender,
	//	[]addresslookuptable.KeyedAddressLookupTable{*p.lookupTableAccount},
	//	[]interface{}{},
	//	&p.driftClient.Opts,
	//	SIM_CU_ESTIMATE_MULTIPLIER,
	//	p.simulateTxForCUEstimate,
	//	p.getBlockhashForTx(),
	//	false,
	//)
	if err != nil {
		p.log().Error("tryBulkFillPerpNodes simError: %s ( fillTxId: %d )", err.Error(), fillTxId)
	} else {
		if p.simulateTxForCUEstimate && len(simResult.SimError) != 0 {
			p.log().Error(
				"tryBulkFillPerpNodesForMarket simError: %s (fillTxId: %d)",
				simResult.SimError,
				fillTxId,
			)

			if len(simResult.SimTxLogs) > 0 {
				p.handleTransactionLogs(nodesToFill, simResult.SimTxLogs)
			}
		} else {
			//for _, node := range nodesSent {
			//	p.log().Info(fmt.Sprintf("%s Ts:%s Filler:%s M%d Maker %s Order%d Taker %s Order%d",
			//		"----------------",
			//		time.Now().Format(time.RFC3339),
			//		p.driftClient.Wallet.GetPublicKey().String(),
			//		node.Node.GetOrder().MarketIndex,
			//		"",
			//		-1,
			//		node.Node.GetUserAccount(),
			//		node.Node.GetOrder().OrderId,
			//	))
			//}
			if p.dryRun {
				p.log().Info("dry run, not sending tx (fillTxId: %d)", fillTxId)
			} else {
				p.sendFillTxAndParseLogs(
					fillTxId,
					nodesSent,
					simResult.Tx,
					buildForBundle,
				)
			}
			for _, node := range nodesSent {
				node.Node.TryFill()
			}
		}
	}
	return len(nodesSent)
}

func (p *FillerBot) getBlockhashForTx() string {
	cachedBlockhash := p.bhSubscriber.GetLatestBlockhash(CACHED_BLOCKHASH_OFFSET)
	if cachedBlockhash != nil {
		return cachedBlockhash.Blockhash.String()
	}

	recentBlockhash, err := p.driftClient.Rpc.GetLatestBlockhash(context.TODO(), rpc2.CommitmentConfirmed)
	if err != nil {
		return ""
	}
	return recentBlockhash.Value.Blockhash.String()
}

func (p *FillerBot) sendFillTxAndParseLogs(
	fillTxId int64,
	nodesSent []*dloblib.NodeToFill,
	tx *solana.Transaction,
	buildForBundle bool,
) {
	defer p.removeFillingNodes(nodesSent)
	if len(tx.Signatures) == 0 {
		tx = p.driftClient.Wallet.SignTransaction(tx)
	}
	txStart := src.NowMs()
	txSig := tx.Signatures[0]

	resp, err := p.driftClient.TxSender.SendTransaction(
		tx,
		p.driftClient.GetOpts(),
		true,
		nil,
	)

	p.registerTxSigToConfirm(txSig, src.NowMs(), nodesSent, fillTxId, TxTypeFill)

	if err != nil {
		p.log().Error("Failed to send tx: %s (fillTxId: %d), error: %s", txSig.String(), fillTxId, err.Error())
	} else if resp != nil {
		duration := src.NowMs() - txStart
		p.log().Info(
			"sent tx: %s, took: %d ms (fillTxId: %d)",
			resp.TxSig.String(),
			duration,
			fillTxId,
		)
	}
}
func (p *FillerBot) registerTxSigToConfirm(
	txSig solana.Signature,
	now int64,
	nodeFilled []*dloblib.NodeToFill,
	fillTxId int64,
	txType TxType,
) {
	p.pendingTxSigsToconfirm.Add(txSig.String(), &PendingTxSigs{
		Ts:         now,
		NodeFilled: nodeFilled,
		FillTxId:   fillTxId,
		TxType:     txType,
	})
	//
}

func (p *FillerBot) calcCompactU16EncodedSizeX(length uint64, params ...int) uint64 {
	elemSize := 1
	if len(params) > 0 {
		elemSize = params[0]
	}
	if length > 0x3fff {
		return length*uint64(elemSize) + 3
	} else if length > 0x7f {
		return length*uint64(elemSize) + 2
	} else {
		t := length * uint64(elemSize)
		if t < 1 {
			t = 1
		}
		return t + 1
	}
}

func (p *FillerBot) calcIxEncodedSize(ix solana.Instruction) uint64 {
	data, err := ix.Data()
	if err != nil {
		data = make([]byte, 0)
	}
	//return 1 +
	//	p.calcCompactU16EncodedSize(make([]any, len(ix.Accounts())), 1) +
	//	p.calcCompactU16EncodedSize(make([]any, len(data)), 1)
	return 1 +
		p.calcCompactU16EncodedSizeX(uint64(len(ix.Accounts())), 1) +
		p.calcCompactU16EncodedSizeX(uint64(len(data)), 1)
}

func (p *FillerBot) getNodeFillInfo(nodeToFill *dloblib.NodeToFill) (
	[]*accounts.DataAndSlot[*go_drift.MakerInfo],
	string,
	*drift2.User,
	uint64,
	*go_drift.ReferrerInfo,
	drift2.MarketType,
) {
	var makerInfos []*accounts.DataAndSlot[*go_drift.MakerInfo]

	if len(nodeToFill.MakerNodes) > 0 {
		makerNodesMap := make(map[string][]dloblib.IDLOBNode)
		for _, makerNode := range nodeToFill.MakerNodes {
			if p.isDLOBNodeThrottled(makerNode) {
				continue
			}

			if makerNode.GetUserAccount() == "" {
				continue
			}

			_, exists := makerNodesMap[makerNode.GetUserAccount()]
			if exists {
				makerNodesMap[makerNode.GetUserAccount()] = append(makerNodesMap[makerNode.GetUserAccount()], makerNode)
			} else {
				makerNodesMap[makerNode.GetUserAccount()] = []dloblib.IDLOBNode{makerNode}
			}
		}
		if len(makerNodesMap) > MAX_MAKERS_PER_FILL {
			makerNodesMap = SelectMakers(makerNodesMap)
		}

		for makerAccount, makerNodes := range makerNodesMap {
			makerNode := makerNodes[0]
			makerUserAccount := p.getUserAccountAndSlotFromMap(makerAccount)
			if makerUserAccount == nil {
				continue
			}
			makerAuthority := makerUserAccount.Data.Authority
			makerUserStats := p.userStatsMap.MustGet(makerAuthority.String())
			if makerUserStats == nil {
				continue
			}
			makerInfos = append(makerInfos, &accounts.DataAndSlot[*go_drift.MakerInfo]{
				Slot: makerUserAccount.Slot,
				Data: &go_drift.MakerInfo{
					Maker:            solana.MPK(makerNode.GetUserAccount()),
					MakerUserAccount: makerUserAccount.Data,
					Order:            makerNode.GetOrder(),
					MakerStats:       makerUserStats.GetPublicKey(),
				}})
		}
	}

	takerUserPubKey := nodeToFill.Node.GetUserAccount()
	takerUserAcct := p.getUserAccountAndSlotFromMap(takerUserPubKey)

	referrerInfo := p.userStatsMap.MustGet(takerUserAcct.Data.Authority.String()).GetReferrerInfo()

	return makerInfos,
		takerUserPubKey,
		takerUserAcct.Data,
		takerUserAcct.Slot,
		referrerInfo,
		nodeToFill.Node.GetOrder().MarketType
}

func (p *FillerBot) tryFillMultiMakerPerpNodes(
	nodeToFill *dloblib.NodeToFill,
	buildForBundle bool,
) {
	fillTxId := p.fillTxId
	p.fillTxId++

	nodeWithMakerSet := nodeToFill
	for !p.fillMultiMakerPerpNodes(fillTxId, nodeWithMakerSet, buildForBundle) {
		newMakerSet := nodeWithMakerSet.MakerNodes
		slices.SortFunc(newMakerSet, func(a, b dloblib.IDLOBNode) int {
			if r := rand.Intn(100); r >= 50 {
				return 1
			} else {
				return -1
			}
		})
		left := len(newMakerSet)
		if left > 1 {
			offset := int(math2.Ceil(float64(left) / 2))
			newMakerSet = newMakerSet[0:offset]
		} else {
			p.log().Error("No makers left to use for multi maker perp node (fillTxId: %d)", fillTxId)
			break
		}
		nodeWithMakerSet = &dloblib.NodeToFill{
			Node:       nodeWithMakerSet.Node,
			MakerNodes: newMakerSet,
		}
	}
}

func (p *FillerBot) logSlots() {
	p.log().Info(
		"blockhashSubscriber slot: %d, userMap slot: %d",
		p.bhSubscriber.GetSlot(),
		p.userMap.GetSlot(),
	)
}

func (p *FillerBot) setFillingNode(nodeSig string) {
	defer p.mxFillingNodes.Unlock()
	p.mxFillingNodes.Lock()
	p.fillingNodes[nodeSig] = time.Now().UnixMilli()
}

func (p *FillerBot) fillMultiMakerPerpNodes(
	fillTxId int64,
	nodeToFill *dloblib.NodeToFill,
	buildForBundle bool,
) bool {

	minLamports := uint64(MIN_LAMPORT_DEFAULT) //max(500, p.priorityFeeSubscriber.GetCustomStrategyResult())
	ixs := []solana.Instruction{
		computebudget.NewSetComputeUnitLimitInstructionBuilder().SetUnits(COMPUTE_UNIT_LIMIT).Build(),
		computebudget.NewSetComputeUnitPriceInstructionBuilder().SetMicroLamports(minLamports).Build(),
	}

	makerInfos, _, takerUser, _, referrerInfo, marketType :=
		p.getNodeFillInfo(nodeToFill)

	//p.log().Info(
	//	logMessageForNodeToFill(
	//		nodeToFill,
	//		takerUserPubkey,
	//		takerUserSlot,
	//		makerInfos,
	//		p.getMaxSlot(),
	//		fmt.Sprintf("filling multi maker perp node with %d makers (fillTxId: %d)", len(nodeToFill.MakerNodes), fillTxId),
	//	),
	//)
	if marketType != drift2.MarketType_Perp {
		return false
	}

	//user := p.driftClient.GetUser(nil, nil)
	var makerInfosToUse []*accounts.DataAndSlot[*go_drift.MakerInfo]
	makerInfosToUse = append(makerInfosToUse, makerInfos...)

	buildTxWithMakerInfos := func(
		makers []*accounts.DataAndSlot[*go_drift.MakerInfo],
	) (
		*lib.SimulateAndGetTxWithCUsResponse,
		error,
	) {

		ixs = append(ixs, p.driftClient.GetFillPerpOrderIx(
			addresses.GetUserAccountPublicKey(
				p.driftClient.GetProgram().GetProgramId(),
				takerUser.Authority,
				takerUser.SubAccountId,
			),
			takerUser,
			nodeToFill.Node.GetOrder(),
			utils.ValuesFunc(makers, func(t *accounts.DataAndSlot[*go_drift.MakerInfo]) *go_drift.MakerInfo {
				return t.Data
			}),
			referrerInfo,
			nil,
		))
		p.setFillingNode(src.GetNodeToFillSignature(nodeToFill))

		if p.revertOnFailure {
			ixs = append(ixs, p.driftClient.GetRevertFillIx(nil))
		}
		simResult, err := lib.SimulateAndGetTxWithCUsX(lib.SimulateAndGetTxWithCUsParams{
			Connection:          p.driftClient.Rpc,
			Payer:               p.driftClient.Wallet.GetWallet(),
			LookupTableAccounts: []addresslookuptable.KeyedAddressLookupTable{*p.lookupTableAccount},
			Ixs:                 ixs,
			CuLimitMultiplier:   utils.NewPtr(SIM_CU_ESTIMATE_MULTIPLIER),
			DoSimulation:        utils.NewPtr(p.simulateTxForCUEstimate),
			RecentBlockhash:     utils.NewPtr(p.getBlockhashForTx()),
			DumpTx:              utils.NewPtr(DUMP_TXS_IN_SIM),
		})

		return simResult, err
	}
	var simResult *lib.SimulateAndGetTxWithCUsResponse
	var err error
	for attempt := 0; ; attempt++ {
		simResult, err = buildTxWithMakerInfos(makerInfosToUse)
		if err != nil {
			break
		}
		txAccountKeys, _ := simResult.Tx.Message.GetAllKeys()
		txAccounts := len(txAccountKeys)
		makerInfosToUse = makerInfosToUse[0 : len(makerInfosToUse)-1]
		if txAccounts > MAX_ACCOUNTS_PER_TX || len(makerInfosToUse) == 0 {
			break
		}
		p.log().Info("(fillTxId: %d attempt %d) Too many accounts, remove 1 and try again (had %d maker and %d accounts)",
			fillTxId,
			attempt,
			len(makerInfosToUse),
			txAccounts,
		)
	}
	if len(makerInfosToUse) == 0 {
		p.log().Error(
			"No makerInfos left to use for multi maker perp node ( fillTxId: %d )",
			fillTxId,
		)
		return true
	}
	if err != nil {
		p.log().Error("tryFillMultiMakerPerpNodes simError: %s ( fillTxId: %d )", err.Error(), fillTxId)
	} else {
		p.log().Info("tryFillMultiMakerPerpNodes estimated CUs: %d ( fillTxId: %d )", simResult.CuEstimate, fillTxId)
		if len(simResult.SimError) != 0 {
			p.log().Error(
				"Error simulating multi maker perp node (fillTxId: %d): %s\nTaker slot: %d\nMaker slots: %s %d",
				fillTxId,
				simResult.SimError,
				takerUser.LastActiveSlot,
				makerInfosToUse[0].Data.MakerUserAccount.Authority.String(),
				makerInfosToUse[0].Data.MakerUserAccount.LastActiveSlot,
			)
			if len(simResult.SimTxLogs) > 0 {
				_, exceededCUs := p.handleTransactionLogs([]*dloblib.NodeToFill{nodeToFill}, simResult.SimTxLogs)
				if exceededCUs {
					return false
				}
			}

		} else {
			if !p.dryRun {
				p.sendFillTxAndParseLogs(fillTxId, []*dloblib.NodeToFill{nodeToFill}, simResult.Tx, true)
			} else {
				p.log().Info("dry run, not sending tx (fillTxId: %d)", fillTxId)
			}
			nodeToFill.Node.TryFill()

		}
	}
	return true
}

func (p *FillerBot) tryTriggerPerNodeForMarket(
	nodeToTrigger *dloblib.NodeToTrigger,
	buildForBundle bool,
) {
	defer p.removeTriggeringNodes(nodeToTrigger)

	nodeToTrigger.Node.SetTrigger(true)
	user := p.getUserAccountAndSlotFromMap(
		nodeToTrigger.Node.GetUserAccount(),
	)
	p.log().Info(
		"try to trigger (account: %s, slot: %d) order %d",
		nodeToTrigger.Node.GetUserAccount(),
		user.Data.LastActiveSlot,
		nodeToTrigger.Node.GetOrder().OrderId,
	)

	nodeSignature := src.GetNodeToTriggerSignature(nodeToTrigger)
	p.setTriggeringNodes(nodeSignature)

	ixs := []solana.Instruction{p.driftClient.GetTriggerOrderIx(
		solana.MPK(nodeToTrigger.Node.GetUserAccount()),
		user.Data,
		nodeToTrigger.Node.GetOrder(),
		nil,
	)}

	if p.revertOnFailure {
		ixs = append(ixs, p.driftClient.GetRevertFillIx(nil))
	}
	simResult, err := lib.SimulateAndGetTxWithCUsX(lib.SimulateAndGetTxWithCUsParams{
		Connection:          p.driftClient.Rpc,
		Payer:               p.driftClient.Wallet.GetWallet(),
		LookupTableAccounts: []addresslookuptable.KeyedAddressLookupTable{*p.lookupTableAccount},
		Ixs:                 ixs,
		CuLimitMultiplier:   utils.NewPtr(SIM_CU_ESTIMATE_MULTIPLIER),
		DoSimulation:        utils.NewPtr(p.simulateTxForCUEstimate),
		RecentBlockhash:     utils.NewPtr(p.getBlockhashForTx()),
		DumpTx:              utils.NewPtr(DUMP_TXS_IN_SIM),
	})
	//simResult, err := lib.SimulateAndGetTxWithCUs(
	//	ixs,
	//	p.driftClient.Rpc,
	//	p.driftClient.TxSender,
	//	[]addresslookuptable.KeyedAddressLookupTable{*p.lookupTableAccount},
	//	[]interface{}{},
	//	&p.driftClient.Opts,
	//	SIM_CU_ESTIMATE_MULTIPLIER,
	//	p.simulateTxForCUEstimate,
	//	p.getBlockhashForTx(),
	//	false,
	//)
	if err != nil {
		p.log().Error("tryTriggerPerNodeForMarket simError: %s", err.Error())
	} else {
		if p.simulateTxForCUEstimate && len(simResult.SimError) != 0 {
			p.log().Error(
				"executeTriggerablePerpNodesForMarket simError: %s",
				simResult.SimError,
			)

		} else {
			if p.dryRun {
				p.log().Info("dry run, not triggering node")
			} else {
				// Assuming the SOL check is now within the mutex's scope
				tx := p.driftClient.Wallet.SignTransaction(simResult.Tx)
				txSig := tx.Signatures[0]
				p.registerTxSigToConfirm(txSig, src.NowMs(), []*dloblib.NodeToFill{}, -1, TxTypeTrigger)

				if buildForBundle {
					// p.sendTxThroughJito(tx, "triggerOrder", txSig)
				} else { // if p.canSendOutsideJito()
					_, err = p.driftClient.SendTransaction(tx, nil, true)
					if err != nil {
						nodeToTrigger.Node.SetTrigger(false)
						p.log().Error(
							"error (%s) triggering order for user (account: %s) order: %d",
							err.Error(),
							nodeToTrigger.Node.GetUserAccount(),
							nodeToTrigger.Node.GetOrder().OrderId,
						)
					}
				}
			}
		}
	}
}
