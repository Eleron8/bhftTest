
1. Initial request to get snapshot Recent Trades List:
	- Make HTTP GET request to get initial snapshot of Recent Trades List
	- example:
	- `curl  https://www.binance.com/api/v3/trades?symbol=BTCUSDT&limit=100`
	- response:
```
[
{
"id":3655005919,
"price":"61078.55000000",
"qty":"0.02467000",
"quoteQty":"1506.80782850",
"time":1719676619331,
"isBuyerMaker":true,
"isBestMatch":true
    },
    ...
]
```

	parameters:
		symbol - string
		 limit - int
	source: Memory

2. Establish websocket connection to aggTrade stream:
	- example:
	- `wss://stream.binance.com:9443/ws/btcusdt@trade`
	- event format:

```
{
  "e": "trade", // Event type
  "E": 1719847009412, // Event time
  "s": "BTCUSDT", // Symbol
  "t": 3657323994, // TradeId
  "p": "62996.01000000", // Price
  "q": "0.00176000", // Quantity
  "T": 1719847009411, // Trade time
  "m": false, // If the buyer is the market maker
  "M": true // Ignore
}
```
 
3. Process real-time data:
 - Add a new trade to the recent traded list.
 - if the trade list has limits remove oldest trades and keep the size of the list within limits
