import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import socket

import pandas as pd
from questdb.ingress import Sender, IngressError, TimestampNanos

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBClient:
    """
    QuestDB client with ILP (InfluxDB Line Protocol) support for high-speed data ingestion.

    QuestDB is a high-performance time-series database optimized for real-time analytics.
    ILP provides ultra-fast ingestion rates for time-series data.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9009,
        auth: Optional[tuple] = None,
        tls: bool = False,
        auto_flush: bool = True,
        auto_flush_rows: int = 75000,
        auto_flush_interval: int = 1000,  # milliseconds
    ):
        """
        Initialize QuestDB client.

        Args:
            host: QuestDB server host
            port: ILP port (default: 9009)
            auth: Optional tuple of (key_id, private_key, public_key) for authentication
            tls: Whether to use TLS
            auto_flush: Enable automatic flushing
            auto_flush_rows: Flush after this many rows
            auto_flush_interval: Flush after this many milliseconds
        """
        self.host = host
        self.port = port
        self.auth = auth
        self.tls = tls
        self.auto_flush = auto_flush
        self.auto_flush_rows = auto_flush_rows
        self.auto_flush_interval = auto_flush_interval
        self._sender: Optional[Sender] = None
        self._connected = False

        logger.info(f"QuestDB client initialized for {host}:{port}")

    def connect(self):
        """Establish connection to QuestDB using ILP."""
        try:
            conf_str = f'tcp::addr={self.host}:{self.port};'

            if self.auto_flush:
                conf_str += f'auto_flush_rows={self.auto_flush_rows};auto_flush_interval={self.auto_flush_interval};'

            if self.tls:
                conf_str += 'tls=on;'

            if self.auth:
                key_id, private_key, public_key = self.auth
                conf_str += f'username={key_id};token={private_key};'

            self._sender = Sender.from_conf(conf_str)
            self._connected = True
            logger.info(f"Connected to QuestDB at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            raise

    def disconnect(self):
        """Close connection to QuestDB."""
        if self._sender:
            try:
                self._sender.close()
                self._connected = False
                logger.info("Disconnected from QuestDB")
            except Exception as e:
                logger.error(f"Error disconnecting from QuestDB: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def insert_trades(
        self,
        trades: List[Dict[str, Any]],
        table_name: str = "lighter_trades"
    ):
        """
        Insert trades data using ILP for high-speed ingestion.

        Args:
            trades: List of trade dictionaries with fields:
                - timestamp: Unix timestamp or datetime
                - market_id: Market identifier
                - price: Trade price
                - size: Trade size
                - type: Trade type
                - is_maker_ask: Boolean
                - And other trade fields
            table_name: Table name for trades data

        Example trade dict:
            {
                'trade_id': 12345,
                'timestamp': 1699999999,
                'market_id': 0,
                'price': 42000.0,
                'size': 0.5,
                'usd_amount': 21000.0,
                'type': 'trade',
                'is_maker_ask': True,
                'tx_hash': '0x...',
                ...
            }
        """
        if not self._connected:
            raise RuntimeError("Not connected to QuestDB. Call connect() first.")

        try:
            for trade in trades:
                # Convert timestamp to TimestampNanos
                ts = trade.get('timestamp')
                if isinstance(ts, pd.Timestamp):
                    ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))
                elif isinstance(ts, datetime):
                    ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))
                elif isinstance(ts, (int, float)):
                    # Assume Unix timestamp in seconds
                    ts_nanos = TimestampNanos(int(ts * 1_000_000_000))
                else:
                    ts_nanos = TimestampNanos.now()

                # Start building the row
                self._sender.row(
                    table_name,
                    symbols={
                        'market_id': str(trade.get('market_id', 0)),
                        'type': str(trade.get('type', 'trade')),
                        'is_maker_ask': str(trade.get('is_maker_ask', False)),
                    },
                    columns={
                        'trade_id': trade.get('trade_id', 0),
                        'price': float(trade.get('price', 0.0)),
                        'size': float(trade.get('size', 0.0)),
                        'usd_amount': float(trade.get('usd_amount', 0.0)),
                        'ask_account_id': trade.get('ask_account_id', 0),
                        'bid_account_id': trade.get('bid_account_id', 0),
                        'block_height': trade.get('block_height', 0),
                        'taker_fee': trade.get('taker_fee', 0),
                        'maker_fee': trade.get('maker_fee', 0),
                        'tx_hash': str(trade.get('tx_hash', '')),
                    },
                    at=ts_nanos
                )

            # Flush to ensure data is written
            if not self.auto_flush:
                self._sender.flush()

            logger.info(f"Inserted {len(trades)} trades into {table_name}")
        except IngressError as e:
            logger.error(f"Error inserting trades: {e}")
            raise

    def insert_candles(
        self,
        candles: pd.DataFrame,
        table_name: str = "lighter_candles",
        market_id: Optional[int] = None,
        interval: Optional[str] = None
    ):
        """
        Insert candles (OHLCV) data using ILP.

        Args:
            candles: DataFrame with columns: timestamp, open, high, low, close, volume
            table_name: Table name for candles data
            market_id: Market identifier
            interval: Candle interval (e.g., '1h', '5m')
        """
        if not self._connected:
            raise RuntimeError("Not connected to QuestDB. Call connect() first.")

        if candles.empty:
            logger.warning("Empty candles DataFrame, nothing to insert")
            return

        try:
            for idx, row in candles.iterrows():
                # Get timestamp
                if isinstance(idx, pd.Timestamp):
                    ts_nanos = TimestampNanos(int(idx.timestamp() * 1_000_000_000))
                else:
                    ts_nanos = TimestampNanos.now()

                symbols = {}
                if market_id is not None:
                    symbols['market_id'] = str(market_id)
                if interval is not None:
                    symbols['interval'] = interval

                self._sender.row(
                    table_name,
                    symbols=symbols,
                    columns={
                        'open': float(row.get('open', 0.0)),
                        'high': float(row.get('high', 0.0)),
                        'low': float(row.get('low', 0.0)),
                        'close': float(row.get('close', 0.0)),
                        'volume': float(row.get('volume', 0.0)),
                        'quote_volume': float(row.get('quote_volume', 0.0)),
                    },
                    at=ts_nanos
                )

            if not self.auto_flush:
                self._sender.flush()

            logger.info(f"Inserted {len(candles)} candles into {table_name}")
        except IngressError as e:
            logger.error(f"Error inserting candles: {e}")
            raise

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        symbols: Optional[List[str]] = None,
        timestamp_col: str = 'timestamp'
    ):
        """
        Insert arbitrary DataFrame using ILP.

        Args:
            df: DataFrame to insert
            table_name: Table name
            symbols: List of column names to treat as symbols (tags)
            timestamp_col: Column name for timestamp
        """
        if not self._connected:
            raise RuntimeError("Not connected to QuestDB. Call connect() first.")

        if df.empty:
            logger.warning("Empty DataFrame, nothing to insert")
            return

        symbols = symbols or []

        try:
            for idx, row in df.iterrows():
                # Get timestamp
                if timestamp_col in df.columns:
                    ts = row[timestamp_col]
                elif isinstance(idx, pd.Timestamp):
                    ts = idx
                else:
                    ts = datetime.now()

                if isinstance(ts, pd.Timestamp):
                    ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))
                elif isinstance(ts, datetime):
                    ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))
                elif isinstance(ts, (int, float)):
                    ts_nanos = TimestampNanos(int(ts * 1_000_000_000))
                else:
                    ts_nanos = TimestampNanos.now()

                # Separate symbols and columns
                symbol_dict = {col: str(row[col]) for col in symbols if col in row}
                column_dict = {}

                for col in df.columns:
                    if col in symbols or col == timestamp_col:
                        continue

                    value = row[col]
                    if pd.isna(value):
                        continue

                    # Convert to appropriate type
                    if isinstance(value, (int, float)):
                        column_dict[col] = float(value)
                    else:
                        column_dict[col] = str(value)

                self._sender.row(
                    table_name,
                    symbols=symbol_dict,
                    columns=column_dict,
                    at=ts_nanos
                )

            if not self.auto_flush:
                self._sender.flush()

            logger.info(f"Inserted {len(df)} rows into {table_name}")
        except IngressError as e:
            logger.error(f"Error inserting DataFrame: {e}")
            raise

    def flush(self):
        """Manually flush pending data."""
        if self._sender and self._connected:
            self._sender.flush()
            logger.debug("Flushed pending data to QuestDB")

    @staticmethod
    def test_connection(host: str = "localhost", port: int = 9009, timeout: int = 5) -> bool:
        """
        Test if QuestDB is reachable.

        Args:
            host: QuestDB host
            port: ILP port
            timeout: Connection timeout in seconds

        Returns:
            True if connection successful
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
