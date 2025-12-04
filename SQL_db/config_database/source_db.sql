CREATE TABLE fact_trades (
    symbol VARCHAR(20),
    tradeId VARCHAR(15),
    side VARCHAR(10),
    price DECIMAL(10,1),
    quantity FLOAT,
    trade_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    date_part DATE

);
CREATE TABLE fact_order_books(
    symbol VARCHAR(20),
    side VARCHAR(10),
    price DECIMAL(10,1),
    quantity FLOAT,
    snapshot_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    date_part DATE
);
CREATE TABLE fact_funding_rate(
    symbol VARCHAR(20),
    instrument_type VARCHAR(10),
    funding_rate DECIMAL(20,15),
    next_funding_rate  DECIMAL(20,15),
    funding_time TIMESTAMP,
    next_funding_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    date_part DATE
);
CREATE TABLE fact_mark_price(
    symbol VARCHAR(20),
    instrument_type VARCHAR(10),
    mark_price DECIMAL(20,15),
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    date_part DATE
);
CREATE TABLE fact_ohlc(
    symbol VARCHAR(20),
    channel VARCHAR(10),
    candle_time TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    is_confirmed INTEGER,
    ingestion_time TIMESTAMP,
    date_part DATE
);
