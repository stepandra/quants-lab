import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from core.data_sources.lighter import LighterDataSource
from core.services.questdb_client import QuestDBClient
from core.tasks import BaseTask, TaskContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LighterTradesIngestionTask(BaseTask):
    """
    Download trades data from Lighter API and ingest into QuestDB using high-speed ILP.

    This task:
    1. Fetches recent trades from Lighter API for specified markets
    2. Ingests trades data into QuestDB using InfluxDB Line Protocol (ILP)
    3. Supports continuous ingestion with configurable intervals
    """

    def __init__(self, config):
        super().__init__(config)

        # Configuration with defaults
        task_config = self.config.config

        # Lighter API configuration
        self.use_testnet = task_config.get("use_testnet", False)
        self.lighter_host = task_config.get("lighter_host", None)

        # Market configuration
        self.market_ids = task_config.get("market_ids", [0, 1, 2])  # Default: BTC, ETH, SOL
        self.trading_pairs = task_config.get("trading_pairs", None)  # Alternative: use trading pairs

        # Ingestion configuration
        self.trades_limit = task_config.get("trades_limit", 1000)
        self.lookback_hours = task_config.get("lookback_hours", 24)

        # QuestDB configuration
        self.questdb_host = task_config.get("questdb_host", "localhost")
        self.questdb_port = task_config.get("questdb_port", 9009)
        self.questdb_table = task_config.get("questdb_table", "lighter_trades")
        self.questdb_auth = task_config.get("questdb_auth", None)
        self.questdb_tls = task_config.get("questdb_tls", False)

        # Performance tuning
        self.auto_flush_rows = task_config.get("auto_flush_rows", 75000)
        self.auto_flush_interval = task_config.get("auto_flush_interval", 1000)

        # Initialize clients
        self.lighter = None
        self.questdb = None

    async def setup(self, context: TaskContext) -> None:
        """Setup task before execution, including validation of prerequisites."""
        try:
            await super().setup(context)

            # Initialize Lighter data source
            self.lighter = LighterDataSource(
                host=self.lighter_host,
                testnet=self.use_testnet
            )

            # Initialize QuestDB client
            self.questdb = QuestDBClient(
                host=self.questdb_host,
                port=self.questdb_port,
                auth=self.questdb_auth,
                tls=self.questdb_tls,
                auto_flush=True,
                auto_flush_rows=self.auto_flush_rows,
                auto_flush_interval=self.auto_flush_interval
            )

            # Test QuestDB connection
            if not QuestDBClient.test_connection(self.questdb_host, self.questdb_port):
                raise RuntimeError(
                    f"Cannot connect to QuestDB at {self.questdb_host}:{self.questdb_port}. "
                    "Make sure QuestDB is running and ILP port is accessible."
                )

            # Connect to QuestDB
            self.questdb.connect()

            # Fetch market info if using trading pairs
            if self.trading_pairs:
                logger.info("Fetching market information from Lighter...")
                await self.lighter.get_market_info()
                # Convert trading pairs to market IDs
                self.market_ids = [
                    self.lighter.get_market_id(pair)
                    for pair in self.trading_pairs
                ]

            logger.info(f"Setup completed for {context.task_name}")
            logger.info(f"Lighter host: {self.lighter.host}")
            logger.info(f"QuestDB: {self.questdb_host}:{self.questdb_port}")
            logger.info(f"Table: {self.questdb_table}")
            logger.info(f"Market IDs: {self.market_ids}")
            logger.info(f"Trades limit per market: {self.trades_limit}")
            logger.info(f"Lookback hours: {self.lookback_hours}")

        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

    async def cleanup(self, context: TaskContext, result) -> None:
        """Cleanup after task execution."""
        try:
            # Disconnect from QuestDB
            if self.questdb:
                self.questdb.disconnect()

            await super().cleanup(context, result)
            logger.info(f"Cleanup completed for {context.task_name}")
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        """Main execution logic."""
        start_execution = datetime.now(timezone.utc)
        logger.info(f"Starting Lighter trades ingestion for {len(self.market_ids)} markets")

        try:
            # Track statistics
            stats = {
                "markets_processed": 0,
                "markets_total": len(self.market_ids),
                "trades_ingested": 0,
                "errors": 0,
                "start_time": start_execution.isoformat(),
            }

            # Calculate time range
            end_time = int(datetime.now(timezone.utc).timestamp())
            start_time = int((datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)).timestamp())

            logger.info(f"Time range: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")

            # Process each market
            for i, market_id in enumerate(self.market_ids):
                try:
                    logger.info(f"Fetching trades for market {market_id} [{i+1}/{len(self.market_ids)}]")

                    # Fetch trades from Lighter
                    trades_df = await self.lighter.get_trades(
                        market_id=market_id,
                        limit=self.trades_limit,
                        start_time=start_time,
                        end_time=end_time
                    )

                    if trades_df.empty:
                        logger.warning(f"No trades found for market {market_id}")
                        stats["markets_processed"] += 1
                        continue

                    logger.info(f"Fetched {len(trades_df)} trades for market {market_id}")

                    # Convert DataFrame to list of dicts for QuestDB ingestion
                    trades_list = trades_df.to_dict('records')

                    # Ingest into QuestDB using ILP
                    self.questdb.insert_trades(
                        trades=trades_list,
                        table_name=self.questdb_table
                    )

                    stats["trades_ingested"] += len(trades_df)
                    stats["markets_processed"] += 1

                    logger.info(
                        f"Ingested {len(trades_df)} trades for market {market_id} "
                        f"(Total: {stats['trades_ingested']})"
                    )

                except Exception as e:
                    logger.error(f"Error processing market {market_id}: {e}")
                    stats["errors"] += 1
                    continue

            # Calculate execution time
            execution_time = (datetime.now(timezone.utc) - start_execution).total_seconds()
            stats["execution_time_seconds"] = execution_time
            stats["end_time"] = datetime.now(timezone.utc).isoformat()

            logger.info(
                f"Ingestion completed: {stats['trades_ingested']} trades from "
                f"{stats['markets_processed']}/{stats['markets_total']} markets "
                f"in {execution_time:.2f}s"
            )

            # Store result metrics
            result = {
                "status": "completed",
                "stats": stats,
                "metrics": {
                    "trades_ingested": stats["trades_ingested"],
                    "markets_processed": stats["markets_processed"],
                    "execution_time": execution_time,
                    "trades_per_second": stats["trades_ingested"] / execution_time if execution_time > 0 else 0,
                }
            }

            return result

        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise


class LighterCandlesIngestionTask(BaseTask):
    """
    Download candles (OHLCV) data from Lighter API and ingest into QuestDB.

    This task fetches historical candlestick data for specified markets and intervals.
    """

    def __init__(self, config):
        super().__init__(config)

        # Configuration with defaults
        task_config = self.config.config

        # Lighter API configuration
        self.use_testnet = task_config.get("use_testnet", False)
        self.lighter_host = task_config.get("lighter_host", None)

        # Market configuration
        self.market_ids = task_config.get("market_ids", [0, 1, 2])
        self.trading_pairs = task_config.get("trading_pairs", None)

        # Candles configuration
        self.intervals = task_config.get("intervals", ["1h", "1d"])
        self.lookback_days = task_config.get("lookback_days", 30)
        self.limit = task_config.get("limit", 1000)

        # QuestDB configuration
        self.questdb_host = task_config.get("questdb_host", "localhost")
        self.questdb_port = task_config.get("questdb_port", 9009)
        self.questdb_table = task_config.get("questdb_table", "lighter_candles")

        # Initialize clients
        self.lighter = None
        self.questdb = None

    async def setup(self, context: TaskContext) -> None:
        """Setup task before execution."""
        try:
            await super().setup(context)

            self.lighter = LighterDataSource(
                host=self.lighter_host,
                testnet=self.use_testnet
            )

            self.questdb = QuestDBClient(
                host=self.questdb_host,
                port=self.questdb_port,
                auto_flush=True
            )

            if not QuestDBClient.test_connection(self.questdb_host, self.questdb_port):
                raise RuntimeError(f"Cannot connect to QuestDB at {self.questdb_host}:{self.questdb_port}")

            self.questdb.connect()

            if self.trading_pairs:
                await self.lighter.get_market_info()
                self.market_ids = [self.lighter.get_market_id(pair) for pair in self.trading_pairs]

            logger.info(f"Setup completed for {context.task_name}")
            logger.info(f"Market IDs: {self.market_ids}")
            logger.info(f"Intervals: {self.intervals}")

        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise

    async def cleanup(self, context: TaskContext, result) -> None:
        """Cleanup after task execution."""
        try:
            if self.questdb:
                self.questdb.disconnect()
            await super().cleanup(context, result)
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        """Main execution logic."""
        start_execution = datetime.now(timezone.utc)

        try:
            stats = {
                "markets_processed": 0,
                "intervals_processed": 0,
                "candles_ingested": 0,
                "errors": 0,
            }

            start_time = int((datetime.now(timezone.utc) - timedelta(days=self.lookback_days)).timestamp())
            end_time = int(datetime.now(timezone.utc).timestamp())

            for market_id in self.market_ids:
                for interval in self.intervals:
                    try:
                        logger.info(f"Fetching {interval} candles for market {market_id}")

                        # Note: trading_pair needs to be resolved from market_id
                        # For now, we'll use a placeholder
                        trading_pair = f"MARKET-{market_id}"

                        candles = await self.lighter.get_candles(
                            trading_pair=trading_pair,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            limit=self.limit
                        )

                        if candles.candles_df.empty:
                            logger.warning(f"No candles for market {market_id} {interval}")
                            continue

                        # Ingest into QuestDB
                        self.questdb.insert_candles(
                            candles=candles.candles_df,
                            table_name=self.questdb_table,
                            market_id=market_id,
                            interval=interval
                        )

                        stats["candles_ingested"] += len(candles.candles_df)
                        stats["intervals_processed"] += 1

                        logger.info(f"Ingested {len(candles.candles_df)} candles for market {market_id} {interval}")

                    except Exception as e:
                        logger.error(f"Error processing market {market_id} {interval}: {e}")
                        stats["errors"] += 1
                        continue

                stats["markets_processed"] += 1

            execution_time = (datetime.now(timezone.utc) - start_execution).total_seconds()

            return {
                "status": "completed",
                "stats": stats,
                "metrics": {
                    "candles_ingested": stats["candles_ingested"],
                    "execution_time": execution_time,
                }
            }

        except Exception as e:
            logger.error(f"Execution failed: {e}")
            raise
