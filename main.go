package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ServerTime struct {
	ServerTime int64
}

var (
	binanceapi      = "https://www.binance.com"
	testconn        = "/api/v3/ping"
	checkServerTime = "/api/v3/time"
	orderbook       = "/api/v3/depth"
	wsbinance       = "wss://stream.binance.com:9443"
	wsorderbook     = "/ws/%s@depth"
)

func main() {
	client := http.Client{
		Timeout: time.Second * 5,
	}

	resp, err := client.Get(binanceapi + testconn)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("status:", resp.Status)
	}
	buff := new(strings.Builder)
	_, err = io.Copy(buff, resp.Body)
	if err != nil {
		log.Fatal()
	}
	fmt.Println(buff.String())

	servertime, err := getServerTime(&client)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("curr time:", time.Now(), "server time:", time.UnixMilli(servertime))

	// orderBook, err := getOrderBook(&client, 100)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("order book:", orderBook)

	// tradeList, err := getTradeList(&client, 100)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("trade list:", tradeList)
	// getOrderBookUpdates()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}

	HandleOrderBook(ctx, &wg, ticker, &client, 100)
	HandleTrades(ctx, &wg, ticker, &client, 100)
	HandleKlines(ctx, &wg, ticker, &client, 100)

	// ch, err := getOrderBookUpdatesConc(ctx, &wg)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// updateAndPrintOrderBook(ctx, orderBook, ch, &wg, ticker)

	// tradech, err := getTradesUpdateCon(ctx, &wg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// updateTradeList(ctx, tradeList, tradech, &wg, ticker)
	wg.Wait()
}

func getServerTime(client *http.Client) (int64, error) {
	resp, err := client.Get(binanceapi + checkServerTime)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status: %s", resp.Status)
	}
	var body ServerTime
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.ServerTime, nil

}
