CREATE TABLE trades (
	market VARCHAR(8) NOT NULL,
	quantity DECIMAL NOT NULL,
	price DECIMAL NOT NULL,
	timestamp BIGINT NOT NULL,
	PRIMARY KEY(market, timestamp)
)