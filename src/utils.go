package src

import (
	"context"
	go_drift "driftgo"
	"driftgo/constants"
	dloblib "driftgo/dlob/types"
	"driftgo/drift/config"
	"driftgo/drift/types"
	"driftgo/lib/drift"
	math2 "driftgo/math"
	oracles "driftgo/oracles/types"
	"driftgo/utils"
	"fmt"
	"github.com/gagliardetto/solana-go"
	associatedtokenaccount "github.com/gagliardetto/solana-go/programs/associated-token-account"
	"github.com/gagliardetto/solana-go/rpc"
	"math"
	"slices"
	"strings"
	"time"
)

var TOKEN_FAUCET_PROGRAM_ID = solana.MPK("V4v1mQiAdLz4qwckEb45WqHYceYizoib39cDBHSWfaB")

const JUPITER_SLIPPAGE_BPS = 100
const PRIORITY_FEE_SERVER_RATE_LIMIT_PER_MIN = 300

func NowMs() int64 {
	return time.Now().UnixMilli()
}

func Now() int64 {
	return time.Now().Unix()
}

func GetOrCreateAssociatedTokenAccount(
	connection *rpc.Client,
	mint solana.PublicKey,
	wallet solana.Wallet,
) solana.PublicKey {
	associatedTokenAccount, _, _ := solana.FindAssociatedTokenAddress(wallet.PublicKey(), mint)
	accountInfo, err := connection.GetAccountInfo(context.TODO(), associatedTokenAccount)
	if err != nil || accountInfo == nil {
		ix, err := associatedtokenaccount.NewCreateInstructionBuilder().
			SetWallet(wallet.PublicKey()).
			SetMint(mint).
			SetPayer(wallet.PublicKey()).
			ValidateAndBuild()
		if err != nil {
			return associatedTokenAccount
		}
		recentBlockHash, err := connection.GetLatestBlockhash(context.TODO(), rpc.CommitmentConfirmed)
		if err != nil {
			return associatedTokenAccount
		}
		tx, err := solana.NewTransactionBuilder().AddInstruction(ix).SetFeePayer(wallet.PublicKey()).SetRecentBlockHash(recentBlockHash.Value.Blockhash).Build()
		if err != nil {
			return associatedTokenAccount
		}
		_, _ = connection.SendTransaction(context.TODO(), tx)
	}
	return associatedTokenAccount
}

// loadCommaDelimitToArray
// loadCommaDelimitToStringArray

func ConvertToMarketType(input string) drift.MarketType {
	switch strings.ToUpper(input) {
	case "PERP":
		return drift.MarketType_Perp
	case "SPOT":
		return drift.MarketType_Spot
	default:
		panic("Invalid market type")
	}
}

// loadKeypair
// getWallet

func SleepMs(ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func SleepS(s int64) {
	SleepMs(s * 1000)
}

// decodeName

func GetNodeToFillSignature(node *dloblib.NodeToFill) string {
	if node.Node.GetUserAccount() == "" {
		return "~"
	}
	return GetFillSignatureFromUserAccountAndOrderId(node.Node.GetUserAccount(), node.Node.GetOrder().OrderId)
}

func GetFillSignatureFromUserAccountAndOrderId(userAccount string, orderId uint32) string {
	return fmt.Sprintf("%s-%d", userAccount, orderId)
}

func GetNodeToTriggerSignature(node *dloblib.NodeToTrigger) string {
	return GetOrderSignature(node.Node.GetOrder().OrderId, node.Node.GetUserAccount())
}

func GetOrderSignature(orderId uint32, userAccountKey string) string {
	return fmt.Sprintf("%s-%d", userAccountKey, orderId)
}

// waitForAllSubscribesToFinish

// calculateAccountValueUsd
// calculateBaseAmountToMarketMakePerp
// calculateBaseAmountToMarketMakeSpot
// isMarketVolatile
// isSpotMarketVolatile
// isSetComputeUnitsIx

func GetBestLimitBidExcludePubKey(
	dlob dloblib.IDLOB,
	marketIndex uint16,
	marketType drift.MarketType,
	slot uint64,
	oraclePriceData *oracles.OraclePriceData,
	excludedPubkey string,
	excludedUserAccountsAndOrder []struct {
		UserAccount string
		OrderId     uint32
	},
) dloblib.IDLOBNode {
	var bestBid dloblib.IDLOBNode
	dlob.GetRestingLimitBids(
		marketIndex,
		slot,
		marketType,
		oraclePriceData,
		nil,
	).Each(func(bid dloblib.IDLOBNode, idx int) bool {
		if bid.GetUserAccount() == excludedPubkey {
			return false
		}
		if excludedUserAccountsAndOrder != nil && len(excludedUserAccountsAndOrder) > 0 {
			if slices.ContainsFunc(excludedUserAccountsAndOrder, func(s struct {
				UserAccount string
				OrderId     uint32
			}) bool {
				return s.UserAccount == bid.GetUserAccount() && s.OrderId == bid.GetOrder().OrderId
			}) {
				return false
			}
		}
		bestBid = bid
		return true
	})
	return bestBid
}

func GetBestLimitAskExcludePubKey(
	dlob dloblib.IDLOB,
	marketIndex uint16,
	marketType drift.MarketType,
	slot uint64,
	oraclePriceData *oracles.OraclePriceData,
	excludedPubkey string,
	excludedUserAccountsAndOrder []struct {
		UserAccount string
		OrderId     uint32
	},
) dloblib.IDLOBNode {
	var bestAsk dloblib.IDLOBNode
	dlob.GetRestingLimitAsks(
		marketIndex,
		slot,
		marketType,
		oraclePriceData,
		nil,
	).Each(func(ask dloblib.IDLOBNode, idx int) bool {
		if ask.GetUserAccount() == excludedPubkey {
			return false
		}
		if excludedUserAccountsAndOrder != nil && len(excludedUserAccountsAndOrder) > 0 {
			if slices.ContainsFunc(excludedUserAccountsAndOrder, func(s struct {
				UserAccount string
				OrderId     uint32
			}) bool {
				return s.UserAccount == ask.GetUserAccount() && s.OrderId == ask.GetOrder().OrderId
			}) {
				return false
			}
		}
		bestAsk = ask
		return true
	})
	return bestAsk
}

func LoadKeypair(keyFilePath string) (solana.PrivateKey, error) {
	privateKey, err := solana.PrivateKeyFromBase58(keyFilePath)
	if err == nil {
		return privateKey, nil
	}
	return solana.PrivateKeyFromSolanaKeygenFile(keyFilePath)
}

func GetWallet(keyFilePath string) (*go_drift.Wallet, error) {
	keypair, err := LoadKeypair(keyFilePath)
	if err != nil {
		return nil, err
	}
	return &go_drift.Wallet{
		PrivateKey: keypair,
	}, nil
}

func GetDriftPriorityFeeEndpoint(driftEnv config.DriftEnv) string {
	return "https://dlob.drift.trade"
}

func ValidMinimumGasAmount(amount float64) bool {
	if amount < 0 {
		return false
	}
	return true
}

func ValidRebalanceSettledPnlThreshold(amount float64) bool {
	if amount < 1 || amount != math.Ceil(amount) {
		return false
	}
	return true
}

func CalculateAccountValueUsd(user types.IUser) int64 {
	netSpotValue := math2.ConvertToNumber(
		user.GetNetSpotMarketValue(nil),
		constants.QUOTE_PRECISION,
	)
	unrealizedPnl := math2.ConvertToNumber(
		user.GetUnrealizedPNL(true, nil, nil, false),
		constants.QUOTE_PRECISION,
	)
	return netSpotValue + unrealizedPnl
}

func CalculateBaseAmountToMarketMakePerp(
	perpMarketAccount *drift.PerpMarket,
	user types.IUser,
	targetLeverage float64,
) float64 {
	basePriceNormed := math2.ConvertToNumber(
		utils.BN(perpMarketAccount.Amm.HistoricalOracleData.LastOraclePriceTwap),
	)
	accountValueUsd := CalculateAccountValueUsd(user)

	targetLeverage *= 0.95

	maxBase := (float64(accountValueUsd) * targetLeverage) / float64(basePriceNormed)
	//marketSymbol := drift2.DecodeName(perpMarketAccount.Name[:])
	return maxBase
}

func CalculateBaseAmountToMarketMakeSpot(
	spotMarketAccount *drift.SpotMarket,
	user types.IUser,
	targetLeverage float64,
) float64 {
	basePriceNormed := math2.ConvertToNumber(
		utils.BN(spotMarketAccount.HistoricalOracleData.LastOraclePriceTwap),
	)
	accountValueUsd := CalculateAccountValueUsd(user)

	targetLeverage *= 0.95

	maxBase := (float64(accountValueUsd) * targetLeverage) / float64(basePriceNormed)
	//marketSymbol := drift2.DecodeName(perpMarketAccount.Name[:])
	return maxBase
}

func IsMarketVolatile(
	perpMarketAccount *drift.PerpMarket,
	oraclePriceData *oracles.OraclePriceData,
	volatileThreshold float64,
) bool {
	twapPrice := perpMarketAccount.Amm.HistoricalOracleData.LastOraclePriceTwap
	lastPrice := perpMarketAccount.Amm.HistoricalOracleData.LastOraclePrice
	currentPrice := oraclePriceData.Price.Int64()
	minDenom := float64(min(currentPrice, lastPrice, twapPrice))
	cVsL := math.Abs(float64(currentPrice-lastPrice) * float64(constants.PRICE_PRECISION.Int64()) / minDenom / float64(constants.PERCENTAGE_PRECISION.Int64()))
	cVsT := math.Abs(float64(currentPrice-twapPrice) * float64(constants.PRICE_PRECISION.Int64()) / minDenom / float64(constants.PERCENTAGE_PRECISION.Int64()))

	recentStd := float64(perpMarketAccount.Amm.OracleStd*constants.PRICE_PRECISION.Uint64()) / minDenom / float64(constants.PERCENTAGE_PRECISION.Int64())

	return recentStd > volatileThreshold ||
		cVsT > volatileThreshold ||
		cVsL > volatileThreshold
}

func IsSpotMarketVolatile(
	spotMarketAccount *drift.SpotMarket,
	oraclePriceData *oracles.OraclePriceData,
	volatileThreshold float64,
) bool {
	twapPrice := spotMarketAccount.HistoricalOracleData.LastOraclePriceTwap
	lastPrice := spotMarketAccount.HistoricalOracleData.LastOraclePrice
	currentPrice := oraclePriceData.Price.Int64()
	minDenom := float64(min(currentPrice, lastPrice, twapPrice))
	cVsL := math.Abs(float64(currentPrice-lastPrice) * float64(constants.PRICE_PRECISION.Int64()) / minDenom / float64(constants.PERCENTAGE_PRECISION.Int64()))
	cVsT := math.Abs(float64(currentPrice-twapPrice) * float64(constants.PRICE_PRECISION.Int64()) / minDenom / float64(constants.PERCENTAGE_PRECISION.Int64()))

	return cVsT > volatileThreshold ||
		cVsL > volatileThreshold
}
