package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type OrderBookResp struct {
	LastUpdateId int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type OrderBookUpdate struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type OrderBook struct {
	sync.Mutex
	Updated      bool
	LastUpdateId int64
	Bids         map[string]string
	Asks         map[string]string
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids: make(map[string]string),
		Asks: make(map[string]string),
	}
}

func (ob *OrderBook) Update(update *OrderBookUpdate) {
	ob.Lock()
	defer ob.Unlock()
	if update.FinalUpdateID <= ob.LastUpdateId {
		return
	}
	if !ob.Updated {
		if update.FirstUpdateID > ob.LastUpdateId+1 || update.FinalUpdateID < ob.LastUpdateId+1 {
			return
		}
		ob.Updated = true
	} else {
		if update.FirstUpdateID != ob.LastUpdateId+1 {
			return
		}
	}
	ob.LastUpdateId = update.FinalUpdateID
	for _, bid := range update.Bids {
		price, qty := bid[0], bid[1]
		if qty == "0.00000000" {
			delete(ob.Bids, price)
			continue
		}
		ob.Bids[price] = qty
	}
	for _, ask := range update.Asks {
		price, qty := ask[0], ask[1]
		if qty == "0.00000000" {
			delete(ob.Asks, price)
			continue
		}
		ob.Asks[price] = qty
	}
}

func (ob *OrderBook) String() string {
	ob.Lock()
	defer ob.Unlock()
	var sb strings.Builder
	sb.WriteString("Bids:\n")
	for price, qty := range ob.Bids {
		sb.WriteString(fmt.Sprintf("%s: %s\n", price, qty))
	}
	sb.WriteString("Asks:\n")
	for price, qty := range ob.Asks {
		sb.WriteString(fmt.Sprintf("%s: %s\n", price, qty))
	}
	return sb.String()
}

func HandleOrderBook(ctx context.Context, wg *sync.WaitGroup, ticker *time.Ticker, client *http.Client, limit int) {
	orderBook, err := getOrderBook(client, limit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("order book:", orderBook)

	ch, err := getOrderBookUpdatesConc(ctx, wg)
	if err != nil {
		log.Fatal(err)
	}
	updateAndPrintOrderBook(ctx, orderBook, ch, wg, ticker)
}

func getOrderBook(client *http.Client, limit int) (*OrderBook, error) {

	params := url.Values{}
	params.Add("symbol", "BTCUSDT")
	params.Add("limit", strconv.Itoa(limit))
	u, err := url.ParseRequestURI(binanceapi)
	if err != nil {
		return nil, err
	}
	u.Path = orderbook
	u.RawQuery = params.Encode()
	fmt.Println("get order book url:", u.String())
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body OrderBookResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	ordbook := NewOrderBook()
	for _, v := range body.Bids {
		ordbook.Bids[v[0]] = v[1]
	}
	//fmt.Println("bids:", ordbook.Bids)
	for _, v := range body.Asks {
		ordbook.Asks[v[0]] = v[1]
	}
	//fmt.Println("asks:", ordbook.Asks)
	//fmt.Println("order book:", body)
	return ordbook, nil
}

// func getOrderBookUpdates() {
// 	c, _, err := websocket.DefaultDialer.Dial(wsbinance+fmt.Sprintf(wsorderbook, "btcusdt"), nil)
// 	if err != nil {
// 		log.Fatal("dial:", err)
// 		// return nil, err
// 	}

// 	defer c.Close()

// 	for {
// 		_, message, err := c.ReadMessage()
// 		if err != nil {
// 			log.Println("read:", err)
// 			return
// 		}
// 		var body OrderBookUpdate
// 		if err := json.Unmarshal(message, &body); err != nil {
// 			log.Println("unmarshal:", err)
// 			return
// 		}
// 		fmt.Println("order book update:", body)
// 	}
// }

func getOrderBookUpdatesConc(ctx context.Context, wg *sync.WaitGroup) (chan OrderBookUpdate, error) {
	ch := make(chan OrderBookUpdate, 10)
	c, _, err := websocket.DefaultDialer.Dial(wsbinance+fmt.Sprintf(wsorderbook, "btcusdt"), nil)
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
				var body OrderBookUpdate
				if err := json.Unmarshal(message, &body); err != nil {
					log.Println("unmarshal:", err)
					return
				}
				fmt.Println("order book update:", body)
				if body.EventType == "depthUpdate" {
					ch <- body
				}
			}

		}
	}()
	return ch, nil
}

func updateAndPrintOrderBook(ctx context.Context, orderBook *OrderBook, ch chan OrderBookUpdate, wg *sync.WaitGroup, ticker *time.Ticker) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				// ticker.Stop()
				fmt.Println("update and print order book done")
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				orderBook.Update(&v)
			case <-ticker.C:
				fmt.Println(orderBook.String())

			}
		}
	}()
}
