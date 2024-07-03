package main

import (
	"context"
	"database/sql"
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

var (
	getKlines = "/api/v3/klines"
	wsklines  = "/ws/%s@kline_%s"
)

type Kline struct {
	OpenTime                 int64  `json:"openTime"`
	Open                     string `json:"open"`
	High                     string `json:"high"`
	Low                      string `json:"low"`
	Close                    string `json:"close"`
	Volume                   string `json:"volume"`
	CloseTime                int64  `json:"closeTime"`
	QuoteAssetVolume         string `json:"quoteAssetVolume"`
	NumberOfTrades           int64  `json:"numberOfTrades"`
	TakerBuyBaseAssetVolume  string `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume string `json:"takerBuyQuoteAssetVolume"`
}

type KlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     struct {
		StartTime        int64  `json:"t"`
		CloseTime        int64  `json:"T"`
		Interval         string `json:"i"`
		FirstTradeID     int64  `json:"f"`
		LastTradeID      int64  `json:"L"`
		OpenPrice        string `json:"o"`
		ClosePrice       string `json:"c"`
		HighPrice        string `json:"h"`
		LowPrice         string `json:"l"`
		BaseAssetVolume  string `json:"v"`
		NumberOfTrades   int64  `json:"n"`
		IsClosed         bool   `json:"x"`
		QuoteAssetVolume string `json:"q"`
		TakerBuyBaseVol  string `json:"V"`
		TakerBuyQuoteVol string `json:"Q"`
	} `json:"k"`
}

type KlineList struct {
	sync.Mutex
	Index    int
	Symbol   string
	Interval string
	List     []Kline
}

func (kl *KlineList) Update(ke KlineEvent) {
	kl.Lock()
	defer kl.Unlock()

	if len(kl.List) == 0 || kl.List[len(kl.List)-1].OpenTime != ke.Kline.StartTime {
		kl.List = append(kl.List, Kline{
			OpenTime:                 ke.Kline.StartTime,
			Open:                     ke.Kline.OpenPrice,
			High:                     ke.Kline.HighPrice,
			Low:                      ke.Kline.LowPrice,
			Close:                    ke.Kline.ClosePrice,
			Volume:                   ke.Kline.BaseAssetVolume,
			CloseTime:                ke.Kline.CloseTime,
			QuoteAssetVolume:         ke.Kline.QuoteAssetVolume,
			NumberOfTrades:           ke.Kline.NumberOfTrades,
			TakerBuyBaseAssetVolume:  ke.Kline.TakerBuyBaseVol,
			TakerBuyQuoteAssetVolume: ke.Kline.TakerBuyQuoteVol,
		})

	} else {
		kl.List[len(kl.List)-1] = Kline{
			OpenTime:                 ke.Kline.StartTime,
			Open:                     ke.Kline.OpenPrice,
			High:                     ke.Kline.HighPrice,
			Low:                      ke.Kline.LowPrice,
			Close:                    ke.Kline.ClosePrice,
			Volume:                   ke.Kline.BaseAssetVolume,
			CloseTime:                ke.Kline.CloseTime,
			QuoteAssetVolume:         ke.Kline.QuoteAssetVolume,
			NumberOfTrades:           ke.Kline.NumberOfTrades,
			TakerBuyBaseAssetVolume:  ke.Kline.TakerBuyBaseVol,
			TakerBuyQuoteAssetVolume: ke.Kline.TakerBuyQuoteVol,
		}
	}
}

func (kl *KlineList) GetToInsert() []Kline {
	kl.Lock()
	defer kl.Unlock()
	klines := make([]Kline, len(kl.List)-kl.Index)
	for i := kl.Index; i < len(kl.List); i++ {
		if kl.List[i].CloseTime <= kl.List[i].OpenTime {
			kl.Index = i
			break
		}
		klines[i-kl.Index] = kl.List[i]

	}
	if kl.Index < len(kl.List) && kl.List[kl.Index].CloseTime > kl.List[kl.Index].OpenTime {
		kl.Index = len(kl.List)
	}

	return klines
}

func NewKlineList(symbol, interval string) *KlineList {
	return &KlineList{
		Symbol:   symbol,
		Interval: interval,
		List:     make([]Kline, 0),
	}
}

func HandleKlines(ctx context.Context, wg *sync.WaitGroup, ticker *time.Ticker, client *http.Client, limit int, db *sql.DB) {
	klineList, err := getKlinesdata(client, "BTCUSDT", "1d", limit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("kline list", klineList, len(klineList.List))

	ch, err := getKlineUpdatesCon(ctx, wg, "btcusdt", "BTCUSDT", "1d")
	if err != nil {
		log.Fatal(err)
	}

	updateKlines(ctx, klineList, ch, wg, ticker, db)
}

func getKlinesdata(client *http.Client, symbol string, interval string, limit int) (*KlineList, error) {
	params := url.Values{}
	params.Add("symbol", "BTCUSDT")
	params.Add("interval", interval)
	params.Add("limit", strconv.Itoa(limit))

	u, err := url.ParseRequestURI(binanceapi)
	if err != nil {
		return nil, err
	}
	u.Path = getKlines
	u.RawQuery = params.Encode()
	fmt.Println("get klines url:", u.String())
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rawbody [][]interface{}

	if err := json.NewDecoder(resp.Body).Decode(&rawbody); err != nil {
		return nil, err
	}

	klineList := NewKlineList(symbol, interval)

	for _, k := range rawbody {
		kline := Kline{
			OpenTime:                 int64(k[0].(float64)),
			Open:                     k[1].(string),
			High:                     k[2].(string),
			Low:                      k[3].(string),
			Close:                    k[4].(string),
			Volume:                   k[5].(string),
			CloseTime:                int64(k[6].(float64)),
			QuoteAssetVolume:         k[7].(string),
			NumberOfTrades:           int64(k[8].(float64)),
			TakerBuyBaseAssetVolume:  k[9].(string),
			TakerBuyQuoteAssetVolume: k[10].(string),
		}
		klineList.List = append(klineList.List, kline)
	}
	return klineList, nil
}

func getKlineUpdatesCon(ctx context.Context, wg *sync.WaitGroup, smallsymbol, symbol string, interval string) (chan KlineEvent, error) {
	ch := make(chan KlineEvent, 100)
	url := wsbinance + fmt.Sprintf(wsklines, smallsymbol, interval)
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		// log.Fatal("dial:", err)
		return nil, fmt.Errorf("klines: dial: %w, url: %s", err, url)
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
				var body KlineEvent
				if err := json.Unmarshal(message, &body); err != nil {
					log.Println("unmarshal:", err)
					return
				}
				fmt.Println("kline update:", body)
				if body.EventType == "kline" && body.Symbol == symbol {
					ch <- body
				}
			}

		}
	}()
	return ch, nil
}

func updateKlines(ctx context.Context, klineList *KlineList, ch chan KlineEvent, wg *sync.WaitGroup, ticker *time.Ticker, db *sql.DB) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():

				ticker.Stop()
				if err := cleanKlinesTable(db); err != nil {
					fmt.Println("clean klines table error:", err)
				}
				fmt.Println("kline flow is finished")
				return
			case v, ok := <-ch:
				if !ok {
					return
				}

				klineList.Update(v)
				//fmt.Println("update trade list:", tradeList)
			case <-ticker.C:
				newklines := klineList.GetToInsert()
				if err := batchInsertKlines(db, newklines); err != nil {
					fmt.Println("insert klines error:", err)
				}
				//fmt.Println("insert klines:", newklines)
				// fmt.Println("current kline list", len(klineList.List))
			}
		}
	}()
}
