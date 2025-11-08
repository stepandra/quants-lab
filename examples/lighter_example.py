"""
Example script demonstrating Lighter API integration with QuestDB.

This script shows how to:
1. Fetch market data from Lighter API
2. Ingest trades into QuestDB using ILP
3. Query data from QuestDB

Prerequisites:
- QuestDB running on localhost:9009
- Run: docker-compose -f docker-compose-db.yml up -d questdb
"""

import asyncio
from datetime import datetime, timedelta
from core.data_sources.lighter import LighterDataSource
from core.services.questdb_client import QuestDBClient


async def example_fetch_data():
    """Example: Fetch data from Lighter API."""
    print("=" * 60)
    print("Example 1: Fetching data from Lighter API")
    print("=" * 60)

    # Initialize Lighter data source
    lighter = LighterDataSource(testnet=False)

    # Get market information
    print("\n1. Fetching market information...")
    try:
        markets = await lighter.get_market_info()
        print(f"Found {len(markets)} markets")
        print(markets.head())
    except Exception as e:
        print(f"Note: Market info fetch failed (expected if API is unavailable): {e}")

    # Get recent trades for market 0 (BTC-USDC)
    print("\n2. Fetching recent trades for market 0 (BTC-USDC)...")
    try:
        trades = await lighter.get_trades(market_id=0, limit=10)
        print(f"Fetched {len(trades)} trades")
        if not trades.empty:
            print(trades[['timestamp', 'price', 'size', 'type']].head())
    except Exception as e:
        print(f"Note: Trades fetch failed (expected if API is unavailable): {e}")

    # Get candlestick data
    print("\n3. Fetching 1-hour candles...")
    try:
        # Note: This requires a valid trading pair mapping
        candles = await lighter.get_candles(
            trading_pair="BTC-USDC",
            interval="1h",
            limit=10
        )
        print(f"Fetched {len(candles.candles_df)} candles")
        if not candles.candles_df.empty:
            print(candles.candles_df.head())
    except Exception as e:
        print(f"Note: Candles fetch failed: {e}")


async def example_questdb_ingestion():
    """Example: Ingest data into QuestDB."""
    print("\n" + "=" * 60)
    print("Example 2: Ingesting data into QuestDB")
    print("=" * 60)

    # Test QuestDB connection
    print("\n1. Testing QuestDB connection...")
    if not QuestDBClient.test_connection('localhost', 9009):
        print("❌ Cannot connect to QuestDB on localhost:9009")
        print("   Please start QuestDB: docker-compose -f docker-compose-db.yml up -d questdb")
        return
    else:
        print("✓ QuestDB is reachable")

    # Initialize clients
    lighter = LighterDataSource(testnet=False)

    # Fetch some trades
    print("\n2. Fetching trades from Lighter...")
    try:
        trades_df = await lighter.get_trades(market_id=0, limit=100)
        print(f"Fetched {len(trades_df)} trades")
    except Exception as e:
        print(f"❌ Failed to fetch trades: {e}")
        print("   Creating sample data for demonstration...")

        # Create sample data for demonstration
        import pandas as pd
        trades_df = pd.DataFrame([
            {
                'trade_id': i,
                'timestamp': datetime.now() - timedelta(minutes=i),
                'market_id': 0,
                'price': 42000.0 + i * 10,
                'size': 0.1,
                'usd_amount': 4200.0,
                'type': 'trade',
                'is_maker_ask': i % 2 == 0,
                'ask_account_id': 1,
                'bid_account_id': 2,
                'block_height': 1000000 + i,
                'tx_hash': f'0x{i:064x}',
                'taker_fee': 100,
                'maker_fee': 50,
            }
            for i in range(10)
        ])
        print(f"Created {len(trades_df)} sample trades")

    # Ingest into QuestDB
    print("\n3. Ingesting trades into QuestDB...")
    try:
        with QuestDBClient(host='localhost', port=9009) as questdb:
            trades_list = trades_df.to_dict('records')
            questdb.insert_trades(trades_list, table_name='lighter_trades_example')
            print(f"✓ Ingested {len(trades_list)} trades into QuestDB")
            print("  Table: lighter_trades_example")
            print(f"  View in web console: http://localhost:9000")
    except Exception as e:
        print(f"❌ Failed to ingest data: {e}")
        print("   Make sure QuestDB is running with ILP enabled")


async def example_query_data():
    """Example: Query data from QuestDB (would require psycopg2)."""
    print("\n" + "=" * 60)
    print("Example 3: Querying data from QuestDB")
    print("=" * 60)

    print("\nTo query data from QuestDB, you can:")
    print("1. Use the web console: http://localhost:9000")
    print("2. Use PostgreSQL protocol (port 8812)")
    print("3. Use the HTTP API (port 9000)")

    print("\nExample SQL query:")
    print("""
    SELECT
        timestamp,
        market_id,
        price,
        size,
        type
    FROM lighter_trades_example
    ORDER BY timestamp DESC
    LIMIT 10;
    """)

    print("\nExample Python query using psycopg2:")
    print("""
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(
        host='localhost',
        port=8812,
        database='qdb',
        user='admin',
        password='quest'
    )

    df = pd.read_sql_query(
        "SELECT * FROM lighter_trades_example ORDER BY timestamp DESC LIMIT 100",
        conn
    )

    print(df)
    conn.close()
    """)


async def main():
    """Run all examples."""
    print("\n")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║        Lighter API + QuestDB Integration Examples         ║")
    print("╚════════════════════════════════════════════════════════════╝")

    # Example 1: Fetch data from Lighter
    await example_fetch_data()

    # Example 2: Ingest data into QuestDB
    await example_questdb_ingestion()

    # Example 3: Query data (informational)
    await example_query_data()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Check QuestDB web console: http://localhost:9000")
    print("2. Configure automated ingestion: config/lighter_ingestion_example.yml")
    print("3. Read full documentation: docs/LIGHTER_INTEGRATION.md")
    print()


if __name__ == "__main__":
    asyncio.run(main())
