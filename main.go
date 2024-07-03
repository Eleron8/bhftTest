package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

type ServerTime struct {
	ServerTime int64
}

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "bhft_test"
)

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

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, closeDB, err := getDb(psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	defer closeDB()

	if err := runMigration(db); err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	HandleOrderBook(ctx, &wg, ticker, &client, 100)
	HandleTrades(ctx, &wg, ticker, &client, 100)
	HandleKlines(ctx, &wg, ticker, &client, 100, db)

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

	go func() {
		oscall := <-ctx.Done()
		fmt.Println("Received syscall:", oscall)
		fmt.Println("start shutting down")
		stop()
		fmt.Println("shutting down")
	}()

	// <-ctx.Done()
	// <-signalChan
	// fmt.Println("start shutting down")
	// // cancel()
	// fmt.Println("shutting down")

	wg.Wait()
	fmt.Println("everything is finished")

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

func getDb(psqlInfo string) (*sql.DB, func() error, error) {
	// fmt.Println(psqlInfo)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, nil, err
	}
	fmt.Println("Successfully connected! psql url:", psqlInfo)

	return db, db.Close, nil
}

func runMigration(db *sql.DB) error {
	migration := `
	CREATE TABLE IF NOT EXISTS klines (
		open_time BIGINT NOT NULL,
		close_time BIGINT NOT NULL,
		open DOUBLE PRECISION NOT NULL,
		high DOUBLE PRECISION NOT NULL,
		low DOUBLE PRECISION NOT NULL,
		close DOUBLE PRECISION NOT NULL,
		volume DOUBLE PRECISION NOT NULL
	);
	`
	_, err := db.Exec(migration)
	return err
}

func cleanKlinesTable(db *sql.DB) error {
	delete := `
	DELETE FROM klines;
	`
	_, err := db.Exec(delete)
	return err
}

func batchInsertKlines(db *sql.DB, klines []Kline) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO klines (open_time, close_time, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, k := range klines {
		open, err := strconv.ParseFloat(k.Open, 64)
		if err != nil {
			return err
		}
		high, err := strconv.ParseFloat(k.High, 64)
		if err != nil {
			return err
		}
		low, err := strconv.ParseFloat(k.Low, 64)
		if err != nil {
			return err
		}
		close, err := strconv.ParseFloat(k.Close, 64)
		if err != nil {
			return err
		}
		volume, err := strconv.ParseFloat(k.Volume, 64)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(k.OpenTime, k.CloseTime, open, high, low, close, volume)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
