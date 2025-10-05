package helper

import (
	"driftgo/dlob"
	"driftgo/lib/drift"
	"fmt"
	src "keeper-bots-2025/src"
	"keeper-bots-2025/src/log"
	"time"
)

func DumpOrderActionRecord(actionRecord *drift.OrderActionRecord, slot uint64, txSig string) {
	if actionRecord.Action == drift.OrderAction_Place {
		return
	}
	if actionRecord.Action == drift.OrderAction_Expire {
		msg := actionRecord.Action.String()
		log.Channel("filler").Info(fmt.Sprintf("ActionRecord %s | %d %s",
			msg,
			slot, txSig))
		return
	}

	var takerStr = ""
	if actionRecord.TakerOrderId != nil && *actionRecord.TakerOrderId != 0 {
		taker := actionRecord.Taker.String()
		takerDir := "bid"
		if *actionRecord.TakerOrderDirection == drift.PositionDirection_Short {
			takerDir = "ask"
		}
		takerBaseAssetAmount := float64(*actionRecord.TakerOrderBaseAssetAmount) / src.BaseUnit
		takerOrderCumulativeBaseAssetAmountFilled := float64(*actionRecord.TakerOrderCumulativeBaseAssetAmountFilled) / src.BaseUnit
		takerStr = fmt.Sprintf(
			"Taker: %s Order%d %s Amount(%.4f/Fill:%.4f)",
			taker, actionRecord.TakerOrderId,
			takerDir,
			takerBaseAssetAmount, takerOrderCumulativeBaseAssetAmountFilled,
		)
	}
	var makerStr = ""
	if actionRecord.MakerOrderId != nil && *actionRecord.MakerOrderId != 0 {
		maker := actionRecord.Maker.String()
		makerDir := "bid"
		if *actionRecord.MakerOrderDirection == drift.PositionDirection_Short {
			makerDir = "ask"
		}
		makerBaseAssetAmount := float64(*actionRecord.MakerOrderBaseAssetAmount) / src.BaseUnit
		makerOrderCumulativeBaseAssetAmountFilled := float64(*actionRecord.MakerOrderCumulativeBaseAssetAmountFilled) / src.BaseUnit
		makerStr = fmt.Sprintf(
			"Maker: %s Order%d %s Amount(%.4f/Fill:%.4f)",
			maker, actionRecord.MakerOrderId,
			makerDir,
			makerBaseAssetAmount, makerOrderCumulativeBaseAssetAmountFilled,
		)
	}
	var fillerStr = ""
	if actionRecord.Filler != nil {
		rewards := float64(*actionRecord.FillerReward) / 1000_000
		fillerStr = fmt.Sprintf("Filler: %s, reward: %.5f", actionRecord.Filler.String(), rewards)
	}
	actionStr := actionRecord.Action.String()
	msg := fmt.Sprintf("ActionRecord %s M%d %s %s %s", actionStr, actionRecord.MarketIndex, takerStr, makerStr, fillerStr)
	log.Channel("filler").Info(fmt.Sprintf("%s | %d %s",
		msg,
		slot, txSig))
}

func DumpOrderRecord(record *drift.OrderRecord, slot uint64, txSig string) {
	order := record.Order
	statusStr := record.Order.Status.String()
	if order.Status == drift.OrderStatus_Init {
		return
	}

	nodeType, nodeSubType := dlob.GetNodeType(&order, slot)
	nodeTypeStr := nodeType.String()
	nodeSubTypeStr := nodeSubType.String()

	priceStr := float64(order.Price) / src.PriceUnit
	auctionStartPrice := order.AuctionStartPrice
	auctionEndPrice := order.AuctionEndPrice

	maxTs := order.MaxTs
	nowTs := time.Now().Unix()
	infiniteMaxTs := nowTs + 10000000
	var maxTimeStr string
	if maxTs > 0 && maxTs < infiniteMaxTs {
		maxTimeStr = fmt.Sprintf(" MxTm: %ds", maxTs-nowTs)
	} else {
		maxTimeStr = ""
	}
	baseAssetAmountStr := float64(order.BaseAssetAmount) / src.BaseUnit
	baseAssetAmountFilledStr := float64(order.BaseAssetAmountFilled) / src.BaseUnit

	priceLogStr := fmt.Sprintf(" Price: %.4f(%.4f:%.4f)", priceStr, baseAssetAmountStr, baseAssetAmountFilledStr)
	var triggerLogStr string
	if order.TriggerPrice > 0 {
		triggerLogStr = fmt.Sprintf(" TgPrice: %.4f", float64(order.TriggerPrice)/src.PriceUnit)
	} else {
		triggerLogStr = ""
	}
	var oraclePriceOffsetStr string
	if order.OraclePriceOffset != 0 {
		oraclePriceOffsetStr = fmt.Sprintf(" OraclePriceOff: %d", order.OraclePriceOffset)
	} else {
		oraclePriceOffsetStr = ""
	}
	var auctionPriceStr string
	if auctionStartPrice != 0 || auctionEndPrice != 0 {
		auctionPriceStr = fmt.Sprintf(" APrice: %d ~ %d", auctionStartPrice, auctionEndPrice)
	} else {
		auctionPriceStr = ""
	}

	var postOnlyStr string
	if order.PostOnly {
		postOnlyStr = " PO"
	}
	var reduceOnlyStr string
	if order.ReduceOnly {
		reduceOnlyStr = " RO"
	}
	var immediateOrCancelStr string
	if order.ImmediateOrCancel {
		immediateOrCancelStr = " IOC"
	}

	msg := fmt.Sprintf(
		"Order User: %s M%d %s - %s(%s)%s%s%s%s%s%s%s%s (Order%d)",
		record.User.String(),
		order.MarketIndex,
		statusStr,
		nodeTypeStr, nodeSubTypeStr, maxTimeStr, priceLogStr, triggerLogStr,
		oraclePriceOffsetStr, auctionPriceStr, postOnlyStr, reduceOnlyStr, immediateOrCancelStr, order.OrderId,
	)
	log.Channel("filler").Info(fmt.Sprintf("%s | %d %s",
		msg,
		slot, txSig))
}
