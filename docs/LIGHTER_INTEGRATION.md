# Lighter API Integration with QuestDB

This document describes the integration of [Lighter](https://lighter.xyz) decentralized exchange API with quants-lab, using QuestDB for high-speed trade data ingestion via InfluxDB Line Protocol (ILP).

## Overview

The Lighter integration provides:

1. **LighterDataSource** - A data source for fetching market data from Lighter API
2. **QuestDBClient** - A high-performance database client using ILP for ultra-fast ingestion
3. **LighterTradesIngestionTask** - Automated task for ingesting trades data
4. **LighterCandlesIngestionTask** - Automated task for ingesting OHLCV data

## Architecture

```
Lighter API → LighterDataSource → QuestDBClient (ILP) → QuestDB
```

### Why QuestDB?

QuestDB is a high-performance time-series database optimized for:
- **Ultra-fast ingestion**: ILP can ingest millions of rows per second
- **Real-time analytics**: Efficient queries on time-series data
- **Low latency**: Optimized for trading and financial data
- **PostgreSQL compatibility**: Query using standard SQL

### Why replace MongoDB?

For high-frequency trade data:
- MongoDB is designed for general-purpose document storage
- QuestDB is specifically optimized for time-series data
- ILP ingestion is 10-100x faster than traditional inserts
- Better compression and storage efficiency for numeric time-series

Note: MongoDB is still used for task orchestration and metadata. QuestDB is specifically for high-speed trade data ingestion.

## Installation

### 1. Install Dependencies

The required dependencies are already added to `pyproject.toml`:

```bash
pip install -e .
```

This will install:
- `lighter-python` - Official Lighter Python SDK
- `questdb>=2.0.0` - QuestDB client with ILP support

### 2. Start QuestDB

Start QuestDB using Docker Compose:

```bash
docker-compose -f docker-compose-db.yml up -d questdb
```

QuestDB will be available on:
- Web Console: http://localhost:9000
- ILP (ingestion): localhost:9009
- PostgreSQL: localhost:8812

## Usage

### Basic Usage - LighterDataSource

```python
import asyncio
from core.data_sources.lighter import LighterDataSource

async def main():
    # Initialize Lighter data source
    lighter = LighterDataSource(testnet=False)

    # Get market information
    markets = await lighter.get_market_info()
    print(markets)

    # Get trades for BTC-USDC (market_id=0)
    trades = await lighter.get_trades(market_id=0, limit=100)
    print(trades)

    # Get candlestick data
    candles = await lighter.get_candles(
        trading_pair="BTC-USDC",
        interval="1h",
        limit=100
    )
    print(candles.candles_df)

    # Get order book
    order_book = await lighter.get_order_book(market_id=0, limit=50)
    print(order_book)

asyncio.run(main())
```

### Basic Usage - QuestDBClient

```python
from core.services.questdb_client import QuestDBClient
from datetime import datetime

# Initialize QuestDB client
questdb = QuestDBClient(
    host="localhost",
    port=9009,
    auto_flush=True,
    auto_flush_rows=75000
)

# Connect and insert data
with questdb:
    trades = [
        {
            'trade_id': 1,
            'timestamp': datetime.now(),
            'market_id': 0,
            'price': 42000.0,
            'size': 0.5,
            'usd_amount': 21000.0,
            'type': 'trade',
            'is_maker_ask': True,
            'tx_hash': '0x...',
        }
    ]

    questdb.insert_trades(trades, table_name='lighter_trades')
```

### Automated Data Ingestion

Use the pre-configured tasks to automate data collection:

1. **Configure the task** - See `config/lighter_ingestion_example.yml`

2. **Run the task**:

```bash
# Using the task runner
python -m core.tasks.runner --config config/lighter_ingestion_example.yml

# Or integrate into your existing task pipeline
```

## Configuration Reference

### LighterDataSource Configuration

```python
LighterDataSource(
    host="https://mainnet.zklighter.elliot.ai",  # API host
    testnet=False  # Set True for testnet
)
```

### QuestDBClient Configuration

```python
QuestDBClient(
    host="localhost",          # QuestDB host
    port=9009,                 # ILP port
    auth=None,                 # Optional: (key_id, private_key, public_key)
    tls=False,                 # Use TLS
    auto_flush=True,           # Enable auto-flush
    auto_flush_rows=75000,     # Flush after N rows
    auto_flush_interval=1000,  # Flush interval (ms)
)
```

### Task Configuration

See `config/lighter_ingestion_example.yml` for a complete example:

```yaml
tasks:
  - name: lighter_trades_ingestion
    enabled: true
    task_class: app.tasks.data_collection.lighter_trades_ingestion_task.LighterTradesIngestionTask
    schedule:
      type: frequency
      frequency_hours: 1
    config:
      market_ids: [0, 1, 2]  # BTC, ETH, SOL
      trades_limit: 1000
      lookback_hours: 24
      questdb_host: localhost
      questdb_port: 9009
      questdb_table: lighter_trades
```

## Data Schema

### Trades Table (`lighter_trades`)

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | Trade execution time |
| market_id | SYMBOL | Market identifier |
| type | SYMBOL | Trade type (trade, liquidation, deleverage) |
| is_maker_ask | SYMBOL | Whether maker was on ask side |
| trade_id | LONG | Unique trade identifier |
| price | DOUBLE | Trade price |
| size | DOUBLE | Trade size |
| usd_amount | DOUBLE | Trade value in USD |
| ask_account_id | LONG | Ask side account ID |
| bid_account_id | LONG | Bid side account ID |
| block_height | LONG | Blockchain block height |
| taker_fee | LONG | Taker fee |
| maker_fee | LONG | Maker fee |
| tx_hash | STRING | Transaction hash |

### Candles Table (`lighter_candles`)

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | Candle opening time |
| market_id | SYMBOL | Market identifier |
| interval | SYMBOL | Candle interval (1h, 4h, 1d) |
| open | DOUBLE | Opening price |
| high | DOUBLE | Highest price |
| low | DOUBLE | Lowest price |
| close | DOUBLE | Closing price |
| volume | DOUBLE | Trading volume |
| quote_volume | DOUBLE | Quote asset volume |

## Querying Data

### Using QuestDB Web Console

1. Open http://localhost:9000
2. Run SQL queries:

```sql
-- Get recent trades
SELECT * FROM lighter_trades
ORDER BY timestamp DESC
LIMIT 100;

-- Calculate VWAP for last hour
SELECT
    market_id,
    avg(price) as avg_price,
    sum(price * size) / sum(size) as vwap,
    sum(size) as total_volume
FROM lighter_trades
WHERE timestamp > dateadd('h', -1, now())
GROUP BY market_id;

-- Candles aggregation
SELECT
    timestamp,
    first(open) as open,
    max(high) as high,
    min(low) as low,
    last(close) as close,
    sum(volume) as volume
FROM lighter_candles
WHERE market_id = '0' AND interval = '1h'
SAMPLE BY 1d;
```

### Using Python (PostgreSQL protocol)

```python
import psycopg2
import pandas as pd

# Connect using PostgreSQL protocol
conn = psycopg2.connect(
    host='localhost',
    port=8812,
    database='qdb',
    user='admin',
    password='quest'
)

# Query data
df = pd.read_sql_query(
    "SELECT * FROM lighter_trades WHERE market_id = '0' ORDER BY timestamp DESC LIMIT 1000",
    conn
)

conn.close()
```

## Performance Tuning

### QuestDB Optimization

The docker-compose configuration includes optimized settings:

```yaml
environment:
  - QDB_CAIRO_COMMIT_LAG=1000           # Commit lag in microseconds
  - QDB_CAIRO_MAX_UNCOMMITTED_ROWS=10000  # Max uncommitted rows
  - QDB_LINE_TCP_NET_CONNECTION_LIMIT=256 # Connection limit
```

### Ingestion Performance

- **Batch size**: Configure `auto_flush_rows` (default: 75,000)
- **Flush interval**: Configure `auto_flush_interval` (default: 1000ms)
- **Concurrent ingestion**: Run multiple tasks for different markets in parallel

Expected performance:
- **ILP ingestion**: 100,000+ rows/second
- **Query latency**: <100ms for most queries
- **Storage**: ~10-20 bytes per trade (with compression)

## Monitoring

### Check QuestDB Health

```bash
curl http://localhost:9003/status
```

### Monitor Ingestion

```sql
-- Check table size
SELECT count(*) FROM lighter_trades;

-- Check latest data
SELECT max(timestamp) FROM lighter_trades;

-- Check ingestion rate (rows per minute)
SELECT
    date_trunc('minute', timestamp) as minute,
    count(*) as rows_per_minute
FROM lighter_trades
WHERE timestamp > dateadd('h', -1, now())
SAMPLE BY 1m;
```

### Task Monitoring

Tasks store execution metrics in MongoDB:

```python
from core.services.mongodb_client import MongoDBClient
from core.tasks.storage import MongoDBTaskStorage

# Query task execution history
storage = MongoDBTaskStorage()
executions = await storage.get_executions(
    task_name="lighter_trades_ingestion",
    limit=10
)
```

## Troubleshooting

### QuestDB Connection Issues

```python
from core.services.questdb_client import QuestDBClient

# Test connection
if QuestDBClient.test_connection('localhost', 9009):
    print("QuestDB is reachable")
else:
    print("Cannot connect to QuestDB")
    print("Make sure QuestDB is running: docker-compose -f docker-compose-db.yml up -d questdb")
```

### Common Issues

1. **Port already in use**
   - Change port mappings in `docker-compose-db.yml`

2. **Out of memory**
   - Reduce `auto_flush_rows` to flush more frequently
   - Increase Docker memory limit

3. **Slow queries**
   - Ensure timestamp column is indexed (automatic in QuestDB)
   - Use `SAMPLE BY` for aggregations
   - Add `WHERE` clauses to limit data scanned

## Market ID Mapping

Update `MARKET_ID_MAPPING` in `core/data_sources/lighter.py`:

```python
MARKET_ID_MAPPING = {
    "BTC-USDC": 0,
    "ETH-USDC": 1,
    "SOL-USDC": 2,
    # Add more mappings...
}
```

Or fetch dynamically:

```python
lighter = LighterDataSource()
markets = await lighter.get_market_info()
print(markets[['market_id', 'symbol']])
```

## Next Steps

1. **Customize market selection** - Edit `market_ids` in task config
2. **Adjust ingestion frequency** - Modify `frequency_hours` in schedule
3. **Add custom queries** - Create analytics tasks using QuestDB data
4. **Set up alerts** - Monitor ingestion health and data quality
5. **Scale horizontally** - Run multiple ingestion tasks for different market groups

## References

- [Lighter API Documentation](https://apidocs.lighter.xyz)
- [Lighter Python SDK](https://github.com/elliottech/lighter-python)
- [QuestDB Documentation](https://questdb.io/docs/)
- [ILP Reference](https://questdb.io/docs/reference/api/ilp/overview/)

## Support

For issues or questions:
- Lighter: https://docs.lighter.xyz
- QuestDB: https://questdb.io/community
- Quants Lab: Open an issue on GitHub
