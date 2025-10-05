package main

import (
	"context"
	go_drift "driftgo"
	solana2 "driftgo/lib/solana"
	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"testing"
)

// go test -run TestProgramAccount
func TestProgramAccount(t *testing.T) {
	spew.Dump("TestProgramAccount")
	connection := rpc.New("https://mainnet.helius-rpc.com/?api-key=a15f3c20-05f4-43ed-8ff9-92ecc5ca0c6c")
	filters := []rpc.RPCFilter{go_drift.GetUserFilter()}
	filters = append(filters, go_drift.GetNonIdleUserFiler())

	response, err := solana2.GetProgramAccountsContextWithOpts(
		connection,
		context.TODO(),
		solana.MPK("dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"),
		&rpc.GetProgramAccountsOpts{
			Encoding:   solana.EncodingBase64Zstd,
			Commitment: "Finalized",
			Filters:    filters,
		},
	)
	if err != nil {
		spew.Dump(err)
	}
	//spew.Dump(response, err)
	spew.Dump("Total Account:", len(response.Value))
}
