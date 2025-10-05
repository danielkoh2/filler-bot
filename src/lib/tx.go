package lib

import (
	"context"
	go_drift "driftgo"
	"driftgo/tx"
	"driftgo/utils"
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	addresslookuptable "github.com/gagliardetto/solana-go/programs/address-lookup-table"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/rpc"
	"keeper-bots-2025/src"
	"math"
)

const PLACEHOLDER_BLOCKHASH = "Fdum64WVeej6DeL85REV9NvfSxEJNPZ74DBk7A8kTrKP"

type SimulateAndGetTxWithCUsResponse struct {
	CuEstimate    int64
	NCuEstimate   int64
	SimTxLogs     []string
	SimError      string
	SimTxDuration int64
	Tx            *solana.Transaction
}

type SimulateAndGetTxWithCUsParams struct {
	Connection          *rpc.Client
	Payer               solana.Wallet
	LookupTableAccounts []addresslookuptable.KeyedAddressLookupTable
	/// instructions to simulate and create transaction from
	Ixs []solana.Instruction
	/// multiplier to apply to the estimated CU usage, default: 1.0
	CuLimitMultiplier *float64
	/// set false to only create a tx without simulating for CU estimate
	DoSimulation *bool
	/// recentBlockhash to use in the final tx. If undefined, PLACEHOLDER_BLOCKHASH
	/// will be used for simulation, the final tx will have an empty blockhash so
	/// attempts to sign it will throw.
	RecentBlockhash *string
	/// set true to dump base64 transaction before and after simulating for CUs
	DumpTx *bool
}

func isSetComputeUnitsIx(ix solana.Instruction) bool {
	data, _ := ix.Data()
	return ix.ProgramID().Equals(solana.ComputeBudget) && data[0] == 2
}

func GetTransaction(
	payerKey solana.PublicKey,
	ixs []solana.Instruction,
	lookupTableAccounts []addresslookuptable.KeyedAddressLookupTable,
	recentBlockhash string,
) (*solana.Transaction, error) {
	builder := solana.NewTransactionBuilder()
	blockHash, _ := solana.HashFromBase58(recentBlockhash)
	builder.SetFeePayer(payerKey).SetRecentBlockHash(blockHash)
	for _, ix := range ixs {
		builder.AddInstruction(ix)
	}
	var addressTables = make(map[solana.PublicKey]solana.PublicKeySlice)
	for _, table := range lookupTableAccounts {
		addressTables[table.Key] = table.State.Addresses
	}
	return builder.WithOpt(solana.TransactionAddressTables(addressTables)).Build()
}

func SignTransaction(
	signer solana.PrivateKey,
	tx *solana.Transaction,
) *solana.Transaction {
	_, _ = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if signer.PublicKey().Equals(key) {
			return &signer
		}
		return nil
	})
	return tx
}

func SimulateAndGetTxWithCUsX(params SimulateAndGetTxWithCUsParams) (*SimulateAndGetTxWithCUsResponse, error) {
	if len(params.Ixs) == 0 {
		return nil, errors.New("can not simulate empty tx")
	}
	setCULimitIxIdx := -1
	for idx, ix := range params.Ixs {
		if isSetComputeUnitsIx(ix) {
			setCULimitIxIdx = idx
			break
		}
	}
	// if we don't have a set CU limit ix, add one to the beginning
	// otherwise the default CU limit for sim is 400k, which may be too low
	if setCULimitIxIdx == -1 {
		params.Ixs = append([]solana.Instruction{
			computebudget.NewSetComputeUnitLimitInstructionBuilder().SetUnits(1_400_000).Build(),
		}, params.Ixs...)
		setCULimitIxIdx = 0
	}
	recentBlockhash := utils.TTM[string](params.RecentBlockhash == nil, PLACEHOLDER_BLOCKHASH, *params.RecentBlockhash)
	var simTxDuration int64 = 0
	tx, err := GetTransaction(
		params.Payer.PublicKey(),
		params.Ixs,
		params.LookupTableAccounts,
		recentBlockhash,
	)
	if err != nil {
		return nil, err
	}
	if params.DoSimulation == nil || !*params.DoSimulation {
		return &SimulateAndGetTxWithCUsResponse{
			CuEstimate:    -1,
			NCuEstimate:   -1,
			SimTxLogs:     nil,
			SimError:      "",
			SimTxDuration: simTxDuration,
			Tx:            tx,
		}, nil
	}
	if params.DumpTx != nil && *params.DumpTx {
		fmt.Println("===== Simulating The following transaction =====")
		fmt.Println("Recent Blockhash : ", recentBlockhash)
		fmt.Println(tx.String())
		fmt.Println("================================================")
	}
	start := src.NowMs()
	resp, err := params.Connection.SimulateTransactionWithOpts(
		context.TODO(),
		SignTransaction(params.Payer.PrivateKey, tx),
		&rpc.SimulateTransactionOpts{
			SigVerify:              false,
			Commitment:             rpc.CommitmentProcessed,
			ReplaceRecentBlockhash: true,
		},
	)
	simTxDuration = src.NowMs() - start
	if err != nil {
		return nil, err
	}
	if resp.Value.UnitsConsumed == nil {
		return nil, errors.New("failed to get units consumed from simulateTransaction")
	}
	cuLimitMultiplier := utils.TTM[float64](params.CuLimitMultiplier == nil, 1.0, *params.CuLimitMultiplier)
	simTxLogs := resp.Value.Logs
	cuEstimate := *resp.Value.UnitsConsumed
	cusToUse := uint32(math.Floor(float64(cuEstimate) * cuLimitMultiplier))
	params.Ixs[setCULimitIxIdx] = computebudget.NewSetComputeUnitLimitInstructionBuilder().
		SetUnits(cusToUse).
		Build()
	txWithCUs, _ := GetTransaction(
		params.Payer.PublicKey(),
		params.Ixs,
		params.LookupTableAccounts,
		recentBlockhash,
	)
	simError := ""
	if resp.Value.Err != nil {
		simError = spew.Sdump(resp.Value.Err)
	}
	if params.DumpTx != nil && *params.DumpTx {
		fmt.Printf("== Simulation result, cuEstimate: %d, using: %d, blockhash: %s\n",
			cuEstimate,
			cusToUse,
			recentBlockhash,
		)
		fmt.Println(txWithCUs.String())
		fmt.Println("================================================")
	}
	// strip out the placeholder blockhash so user doesn't try to send the tx.
	// sending a tx with placeholder blockhash will cause `blockhash not found error`
	// which is suppressed if flight checks are skipped.
	if params.RecentBlockhash == nil {
		txWithCUs.Message.RecentBlockhash = solana.Hash{}
	}
	return &SimulateAndGetTxWithCUsResponse{
		CuEstimate:    int64(cuEstimate),
		NCuEstimate:   int64(cusToUse),
		SimTxLogs:     simTxLogs,
		SimError:      simError,
		SimTxDuration: simTxDuration,
		Tx:            txWithCUs,
	}, nil
}

func SimulateAndGetTxWithCUs(
	ixs []solana.Instruction,
	connection *rpc.Client,
	txSender tx.ITxSender,
	lookupTableAccounts []addresslookuptable.KeyedAddressLookupTable,
	additionalSigners []interface{},
	opts *go_drift.ConfirmOptions,
	cuLimitMultiplier float64,
	doSimulation bool,
	recentBlockhash string,
	dumpTx bool,
) (*SimulateAndGetTxWithCUsResponse, error) {
	if len(ixs) == 0 {
		return nil, fmt.Errorf("can not simulate empty tx")
	}
	setCULimitIxIdx := -1
	for idx, ix := range ixs {
		if isSetComputeUnitsIx(ix) {
			setCULimitIxIdx = idx
			break
		}
	}
	tx, err := txSender.GetTransaction(
		ixs,
		lookupTableAccounts,
		opts,
		utils.TT(recentBlockhash == "", PLACEHOLDER_BLOCKHASH, recentBlockhash),
		true,
	)
	if !doSimulation {
		return &SimulateAndGetTxWithCUsResponse{
			CuEstimate:    -1,
			NCuEstimate:   0,
			SimTxLogs:     nil,
			SimError:      "",
			SimTxDuration: 0,
			Tx:            tx,
		}, nil
	}

	start := src.NowMs()
	resp, err := connection.SimulateTransactionWithOpts(
		context.TODO(),
		tx,
		&rpc.SimulateTransactionOpts{
			SigVerify:              false,
			Commitment:             rpc.CommitmentProcessed,
			ReplaceRecentBlockhash: true,
		},
	)
	simTxDuration := src.NowMs() - start
	if err != nil {
		fmt.Println(err.Error())
	}
	if resp == nil {
		return nil, errors.New("failed to simulate transaction")
	}

	if resp.Value.UnitsConsumed == nil {
		return nil, errors.New("failed to get units consumed from simulateTransaction")
	}

	//fmt.Println("Units consumed : ", *resp.Value.UnitsConsumed)
	simTxLogs := resp.Value.Logs
	cuEstimate := *resp.Value.UnitsConsumed
	nCuEstimate := uint32(math.Floor(float64(cuEstimate) * cuLimitMultiplier))
	cuIx := computebudget.NewSetComputeUnitLimitInstructionBuilder().SetUnits(nCuEstimate).Build()
	if setCULimitIxIdx == -1 {
		ixs = append([]solana.Instruction{cuIx}, ixs...)
	} else {
		ixs[setCULimitIxIdx] = cuIx
	}
	txWithCUs, err := txSender.GetTransaction(
		ixs,
		lookupTableAccounts,
		opts,
		recentBlockhash,
		false,
	)
	simError := ""
	if resp.Value.Err != nil {

		simError = spew.Sdump(resp.Value.Err)
	}
	if dumpTx {
		fmt.Println("===== Simulating The following transaction =====")

		//txBody, _ := tx.MarshalBinary()
		//serializedTx := base64.StdEncoding.EncodeToString(txBody)
		//fmt.Println(serializedTx)
		fmt.Println("Recent Blockhash : ", recentBlockhash)
		fmt.Println(tx.String())
		fmt.Println("================================================")
	}
	return &SimulateAndGetTxWithCUsResponse{
		CuEstimate:    int64(cuEstimate),
		NCuEstimate:   int64(nCuEstimate),
		SimTxLogs:     simTxLogs,
		SimError:      simError,
		SimTxDuration: simTxDuration,
		Tx:            txWithCUs,
	}, nil
}
