use futures::stream::StreamExt;
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
};
use rust_decimal::prelude::*;
use tokio::{
    sync::mpsc,
};
use sqlx::postgres::PgPool;
use std::collections::VecDeque;

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
                let mut stream = OpenLimitsWs {
                    websocket: BinanceWebsocket::new(BinanceParameters::prod())
                        .await
                        .expect("Failed to create Client"),
                }
                .create_stream(
                    [
                        "BTCUSDT",
                        "ETHUSDT",
                        "CHZUSDT",
                        "BNBUSDT",
                        "DOGEUSDT",
                        "MANAUSDT",
                        "ADAUSDT",
                        "BCHUSDT"
                    ]
                        .iter()
                        .map(|symbol| Subscription::Trades(symbol.to_lowercase().to_string()))
                        .collect::<Vec<Subscription>>()
                        .as_slice()

                )
                .await
                .expect("Couldn't create stream.");

                while let Some(Ok(message)) = stream.next().await {
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
            }
        },
        async move {
	    let url = dotenv::var("DATABASE_URL").unwrap();
            println!("{}", url);
            let pool = PgPool::connect(&url).await.unwrap();

            let mut buffer = VecDeque::new();

            while let Some(trade) = rx.recv().await {
                println!("{:?}", trade);
                buffer.push_back(trade);

                if buffer.len() > 128 {
                    let mut entries = "INSERT INTO trades VALUES ".to_owned();
                    while let Some(Trade {
                            market,
                            quantity,
                            price,
                            timestamp,
                        }) = buffer.pop_front()
                    {
                        entries += &format!("({}, {}, {}, {}), ", market, quantity, price, timestamp);
                    }
                    entries.pop();
                    entries.push(';');
            
                    sqlx::query(&entries).execute(&pool).await.unwrap();
                }
            }
        }
    );
}
