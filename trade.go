package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Trade struct {
	ID            int64  `json:"id"`
	Price         string `json:"price"`
	Quantity      string `json:"qty"`
	QuoteQuantity string `json:"quoteQty"`
	Time          int64  `json:"time"`
	IsBuyerMaker  bool   `json:"isBuyerMaker"`
	IsBestMatch   bool   `json:"isBestMatch"`
}

type TradeList struct {
	sync.Mutex
	Trades []Trade `json:"trades"`
}

func NewTradeList(trades []Trade) *TradeList {
	return &TradeList{Trades: trades}
}

func (t *TradeList) Len() int {
	return len(t.Trades)
}

func (t *TradeList) Pop() Trade {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	if len(t.Trades) == 0 {
		return Trade{}
	}

	trade := t.Trades[0]
	t.Trades = t.Trades[1:]
	return trade
}

func (t *TradeList) Push(trade Trade) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.Trades = append(t.Trades, trade)
}

func (t *TradeList) Update(trade Trade) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	if t.Len() >= 100 {
		t.Pop()

	}
	t.Push(trade)
}

type TradeEvent struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
	Ignore       bool   `json:"M"`
}

var (
	getTradesApi = "/api/v3/trades"
	wstradeApi   = "/ws/%s@trade"
)

func HandleTrades(ctx context.Context, wg *sync.WaitGroup, ticker *time.Ticker, client *http.Client, limit int) {
	tradeList, err := getTradeList(client, limit)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("trade list:", tradeList)

	tradech, err := getTradesUpdateCon(ctx, wg)
	if err != nil {
		log.Fatal(err)
	}

	updateTradeList(ctx, tradeList, tradech, wg, ticker)
}

func getTradeList(client *http.Client, limit int) (*TradeList, error) {
	params := url.Values{}
	params.Add("symbol", "BTCUSDT")
	params.Add("limit", strconv.Itoa(limit))
	u, err := url.ParseRequestURI(binanceapi)
	if err != nil {
		return nil, err
	}
	u.Path = getTradesApi
	u.RawQuery = params.Encode()
	fmt.Println("get order book url:", u.String())
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body []Trade
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	//fmt.Println("get order book:", body)
	return NewTradeList(body), nil
}

func getTradesUpdateCon(ctx context.Context, wg *sync.WaitGroup) (chan TradeEvent, error) {
	ch := make(chan TradeEvent, 100)
	c, _, err := websocket.DefaultDialer.Dial(wsbinance+fmt.Sprintf(wstradeApi, "btcusdt"), nil)
	if err != nil {
		// log.Fatal("dial:", err)
		return nil, err
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer c.Close()
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					return
				}
				var body TradeEvent
				if err := json.Unmarshal(message, &body); err != nil {
					log.Println("unmarshal:", err)
					return
				}
				fmt.Println("trade update:", body)
				if body.EventType == "trade" {
					ch <- body
				}
			}

		}
	}()
	return ch, nil
}

func updateTradeList(ctx context.Context, tradeList *TradeList, ch chan TradeEvent, wg *sync.WaitGroup, ticker *time.Ticker) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				trade := Trade{
					ID:            v.TradeID,
					Price:         v.Price,
					Quantity:      v.Quantity,
					QuoteQuantity: v.Quantity,
					Time:          v.TradeTime,
					IsBuyerMaker:  v.IsBuyerMaker,
				}
				tradeList.Update(trade)
				//fmt.Println("update trade list:", tradeList)
			case <-ticker.C:
				fmt.Println(tradeList)
			}
		}
	}()

}
