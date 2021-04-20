use futures::stream::{BoxStream, StreamExt};
use openlimits::{
    binance::{
        BinanceParameters,
        BinanceWebsocket,
    },
    exchange_ws::{ExchangeWs, OpenLimitsWs},
    model::{
        Side,
        websocket::{Subscription, WebSocketResponse, OpenLimitsWebSocketMessage}
    },
    shared::Result,
};
use rust_decimal::prelude::*;
use tokio::{
    sync::mpsc,
};
use sqlx::postgres::PgPool;
use std::{collections::VecDeque, time::Duration};
use tokio::time::{timeout, sleep};

#[derive(Debug)]
struct Trade {
    market: String,
    quantity: Decimal,
    price: Decimal,
    timestamp: i64,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::join!(
        async move {
            loop {
                if let Ok(mut stream) = connect_websocket().await {
                    while let Ok(Some(Ok(message))) = timeout(Duration::from_secs(5), stream.next()).await {
                        match message {
                            WebSocketResponse::Generic(OpenLimitsWebSocketMessage::Trades(trades)) => {
                                for trade in trades {
                                    let market = trade.market_pair;
                                    let quantity = match trade.side {
                                        Side::Buy => -trade.qty,
                                        Side::Sell => trade.qty,
                                    };
                                    let price = trade.price;
                                    let timestamp = trade.created_at as i64;
            
                                    tx.send(Trade {
                                        market,
                                        quantity,
                                        price,
                                        timestamp
                                    }).unwrap()
                                }
                            },
                            _ => ()
                        }
                    }
                } else {
                    sleep(Duration::from_secs(5)).await;
                }
            }
        },
        async move {
	        let url = dotenv::var("DATABASE_URL").unwrap();
            let pool = PgPool::connect(&url).await.unwrap();

            let mut buffer = VecDeque::new();

            while let Some(trade) = rx.recv().await {
                buffer.push_back(trade);

                if buffer.len() > 2048 {
                    let mut entries = "INSERT INTO trades (market, quantity, price, timestamp) VALUES ".to_owned();
                    while let Some(Trade {
                            market,
                            quantity,
                            price,
                            timestamp,
                        }) = buffer.pop_front()
                    {
                        entries += &format!("('{}', {}, {}, {}),", market, quantity, price, timestamp);
                    }
                    entries.pop();
                    entries.push(';');
            	    
                    sqlx::query(&entries).execute(&pool).await.unwrap();
                }
            }
        }
    );
}

async fn connect_websocket() -> Result<
    BoxStream<
        'static,
        Result<WebSocketResponse<<BinanceWebsocket as ExchangeWs>::Response>>,
    >,
> {
    let subscriptions = [
        "BTCUSDT",
        "ETHUSDT",
        "CHZUSDT",
        "BNBUSDT",
        "DOGEUSDT",
        "ADAUSDT",
        "BCHUSDT",
        "XRPUSDT",
        "LTCUSDT",
        "EOSUSDT",
        "DOTUSDT",
        "THETAUSDT",
        "LINKUSDT",
        "XMRUSDT",
        "XLMUSDT",
        "BTTUSDT",
        "TRXUSDT",
        "VETUSDT",
    ];

    let subscriptions = subscriptions
        .iter()
        .map(|symbol| Subscription::Trades(symbol.to_lowercase().to_string()))
        .collect::<Vec<Subscription>>();

    let stream = OpenLimitsWs {
        websocket: BinanceWebsocket::new(BinanceParameters::prod())
        .await?,
    }
    .create_stream(&subscriptions)
    .await?;

    Ok(stream)
}