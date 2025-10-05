package main

//
//import (
//	"context"
//	"driftgo/lib/geyser"
//	solana_geyser_grpc "driftgo/lib/rpcpool/solana-geyser-grpc"
//	"fmt"
//	"github.com/gagliardetto/solana-go"
//	"sync"
//	"testing"
//	"time"
//)
//
//// go test --run TestGeyserAccountSubscriber
//func TestGeyserAccountSubscriber(t *testing.T) {
//	connec := geyser.NewConnection()
//	err := connec.Connect(geyser.ConnectionConfig{
//		Identity: "",
//		Token:    "77793701102d928c9faa7f0d94fe",
//		Endpoint: "hmsoft.rpcpool.com",
//		Insecure: false,
//	})
//	if err != nil {
//		t.Fatal(err)
//	}
//	sub, err := connec.Subscription()
//	if err != nil {
//		t.Fatal(err)
//	}
//	//request := geyser.NewSubscriptionRequestBuilder().Accounts("7P34csMzr4yJzUmQH95gVXb8dHharUyRVcnxqQFZjKex").Build()
//	request := geyser.NewSubscriptionRequestBuilder().Accounts("BUvduFTd2sWFagCunBPLupG8fBTJqweLw9DuhruNFSCm").Build()
//	sub.Subscribe(&geyser.SubscribeEvents{
//		Update: func(accountData interface{}, ts ...int64) {
//			update := accountData.(*solana_geyser_grpc.SubscribeUpdate)
//			if update == nil {
//				return
//			}
//			account := update.GetAccount()
//			txn := solana.SignatureFromBytes(account.Account.TxnSignature).String()
//			if account == nil {
//				return
//			}
//			pubKey := solana.PublicKeyFromBytes(account.GetAccount().Pubkey)
//			fmt.Printf("Key : %s %s\n", pubKey.String(), txn)
//		},
//		Error: nil,
//		Eof:   nil,
//	})
//	err = sub.Request(request)
//	if err != nil {
//		t.Fatal(err)
//	}
//	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
//	var wait sync.WaitGroup
//	wait.Add(1)
//	go func(ctx context.Context) {
//		for {
//			select {
//			case <-ctx.Done():
//				sub.Unsubscribe()
//				connec.Close()
//				wait.Done()
//				return
//			}
//		}
//	}(ctx)
//	wait.Wait()
//}
