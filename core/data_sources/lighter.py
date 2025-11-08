import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import lighter
from lighter import ApiClient, Configuration, OrderApi, CandlestickApi, AccountApi

from core.data_structures.candles import Candles
from core.data_paths import data_paths

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LighterDataSource:
    """
    Data source for Lighter decentralized exchange.
    Provides access to trades, candles, order books, and other market data.
    """

    # Lighter uses market_id instead of trading pairs
    # Map common trading pairs to market IDs
    MARKET_ID_MAPPING = {
        "BTC-USDC": 0,
        "ETH-USDC": 1,
        "SOL-USDC": 2,
        # Add more mappings as needed
    }

    INTERVAL_MAPPING = {
        '1m': '1m',
        '3m': '3m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '6h': '6h',
        '12h': '12h',
        '1d': '1d',
        '3d': '3d',
        '1w': '1w'
    }

    def __init__(self, host: str = "https://mainnet.zklighter.elliot.ai", testnet: bool = False):
        """
        Initialize Lighter data source.

        Args:
            host: API host URL (default: mainnet)
            testnet: If True, use testnet instead of mainnet
        """
        logger.info("Initializing LighterDataSource")

        if testnet:
            host = "https://testnet.zklighter.elliot.ai"

        self.host = host
        self.configuration = Configuration(host=host)
        self._candles_cache: Dict[Tuple[str, str, int, int], pd.DataFrame] = {}
        self._trades_cache: Dict[Tuple[int, int, int], pd.DataFrame] = {}
        self._market_info_cache: Optional[pd.DataFrame] = None

    async def _get_client(self) -> ApiClient:
        """Create an API client instance."""
        return ApiClient(configuration=self.configuration)

    def get_market_id(self, trading_pair: str) -> int:
        """
        Get market ID for a trading pair.

        Args:
            trading_pair: Trading pair (e.g., "BTC-USDC")

        Returns:
            Market ID
        """
        if trading_pair in self.MARKET_ID_MAPPING:
            return self.MARKET_ID_MAPPING[trading_pair]

        # Try to extract from market info
        if self._market_info_cache is not None:
            matching = self._market_info_cache[
                self._market_info_cache['symbol'] == trading_pair
            ]
            if not matching.empty:
                return int(matching.iloc[0]['market_id'])

        raise ValueError(f"Unknown trading pair: {trading_pair}. Please add to MARKET_ID_MAPPING or fetch market info first.")

    async def get_market_info(self) -> pd.DataFrame:
        """
        Get information about all available markets.

        Returns:
            DataFrame with market information
        """
        if self._market_info_cache is not None:
            return self._market_info_cache

        client = await self._get_client()
        try:
            order_api = OrderApi(client)
            response = await order_api.order_books()

            markets = []
            if hasattr(response, 'order_books'):
                for market in response.order_books:
                    markets.append({
                        'market_id': market.market_id,
                        'symbol': market.symbol if hasattr(market, 'symbol') else f"MARKET-{market.market_id}",
                        'base_token': market.base_token if hasattr(market, 'base_token') else None,
                        'quote_token': market.quote_token if hasattr(market, 'quote_token') else None,
                        'taker_fee': market.taker_fee if hasattr(market, 'taker_fee') else None,
                        'maker_fee': market.maker_fee if hasattr(market, 'maker_fee') else None,
                    })

            self._market_info_cache = pd.DataFrame(markets)
            return self._market_info_cache
        finally:
            await client.close()

    async def get_candles(
        self,
        trading_pair: str,
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000
    ) -> Candles:
        """
        Get candlestick data for a trading pair.

        Args:
            trading_pair: Trading pair (e.g., "BTC-USDC")
            interval: Candle interval (1m, 5m, 1h, etc.)
            start_time: Start timestamp (Unix seconds)
            end_time: End timestamp (Unix seconds)
            limit: Maximum number of candles

        Returns:
            Candles object with OHLCV data
        """
        market_id = self.get_market_id(trading_pair)

        if start_time is None:
            start_time = int((datetime.now() - timedelta(days=30)).timestamp())
        if end_time is None:
            end_time = int(datetime.now().timestamp())

        # Check cache
        cache_key = (trading_pair, interval, start_time, end_time)
        if cache_key in self._candles_cache:
            logger.info(f"Using cached candles for {trading_pair} {interval}")
            return Candles(
                candles_df=self._candles_cache[cache_key],
                connector_name="lighter",
                trading_pair=trading_pair,
                interval=interval
            )

        client = await self._get_client()
        try:
            candlestick_api = CandlestickApi(client)

            # Lighter interval format
            resolution = self.INTERVAL_MAPPING.get(interval, interval)

            response = await candlestick_api.candlesticks(
                market_id=market_id,
                resolution=resolution,
                start_timestamp=start_time,
                end_timestamp=end_time,
                count_back=limit
            )

            candles_data = []
            if hasattr(response, 'candlesticks') and response.candlesticks:
                for candle in response.candlesticks:
                    candles_data.append({
                        'timestamp': pd.to_datetime(candle.timestamp, unit='s'),
                        'open': float(candle.open),
                        'high': float(candle.high),
                        'low': float(candle.low),
                        'close': float(candle.close),
                        'volume': float(candle.volume) if hasattr(candle, 'volume') else 0.0,
                        'quote_volume': float(candle.quote_volume) if hasattr(candle, 'quote_volume') else 0.0,
                    })

            df = pd.DataFrame(candles_data)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
                df.sort_index(inplace=True)

            # Cache the result
            self._candles_cache[cache_key] = df

            return Candles(
                candles_df=df,
                connector_name="lighter",
                trading_pair=trading_pair,
                interval=interval
            )
        finally:
            await client.close()

    async def get_trades(
        self,
        market_id: Optional[int] = None,
        trading_pair: Optional[str] = None,
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get recent trades for a market.

        Args:
            market_id: Market ID (either market_id or trading_pair required)
            trading_pair: Trading pair (e.g., "BTC-USDC")
            limit: Maximum number of trades
            start_time: Start timestamp (Unix seconds)
            end_time: End timestamp (Unix seconds)

        Returns:
            DataFrame with trades data
        """
        if market_id is None:
            if trading_pair is None:
                raise ValueError("Either market_id or trading_pair must be provided")
            market_id = self.get_market_id(trading_pair)

        # Check cache
        cache_key = (market_id, limit, start_time or 0)
        if cache_key in self._trades_cache:
            logger.info(f"Using cached trades for market {market_id}")
            return self._trades_cache[cache_key]

        client = await self._get_client()
        try:
            order_api = OrderApi(client)

            # Use recent_trades endpoint for simpler queries
            response = await order_api.recent_trades(
                market_id=market_id,
                limit=limit
            )

            trades_data = []
            if hasattr(response, 'trades') and response.trades:
                for trade in response.trades:
                    trades_data.append({
                        'trade_id': trade.trade_id,
                        'timestamp': pd.to_datetime(trade.timestamp, unit='s'),
                        'market_id': trade.market_id,
                        'price': float(trade.price),
                        'size': float(trade.size),
                        'usd_amount': float(trade.usd_amount),
                        'type': trade.type,
                        'is_maker_ask': trade.is_maker_ask,
                        'ask_account_id': trade.ask_account_id,
                        'bid_account_id': trade.bid_account_id,
                        'block_height': trade.block_height,
                        'tx_hash': trade.tx_hash,
                        'taker_fee': trade.taker_fee if hasattr(trade, 'taker_fee') else None,
                        'maker_fee': trade.maker_fee if hasattr(trade, 'maker_fee') else None,
                    })

            df = pd.DataFrame(trades_data)
            if not df.empty:
                df.sort_values('timestamp', ascending=False, inplace=True)

            # Cache the result
            self._trades_cache[cache_key] = df

            return df
        finally:
            await client.close()

    async def get_order_book(
        self,
        market_id: Optional[int] = None,
        trading_pair: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, List[Tuple[float, float]]]:
        """
        Get order book for a market.

        Args:
            market_id: Market ID (either market_id or trading_pair required)
            trading_pair: Trading pair (e.g., "BTC-USDC")
            limit: Number of levels per side

        Returns:
            Dictionary with 'bids' and 'asks' lists of (price, size) tuples
        """
        if market_id is None:
            if trading_pair is None:
                raise ValueError("Either market_id or trading_pair must be provided")
            market_id = self.get_market_id(trading_pair)

        client = await self._get_client()
        try:
            order_api = OrderApi(client)
            response = await order_api.order_book_orders(
                market_id=market_id,
                limit=limit
            )

            order_book = {
                'bids': [],
                'asks': []
            }

            if hasattr(response, 'bids') and response.bids:
                order_book['bids'] = [
                    (float(order.price), float(order.size))
                    for order in response.bids
                ]

            if hasattr(response, 'asks') and response.asks:
                order_book['asks'] = [
                    (float(order.price), float(order.size))
                    for order in response.asks
                ]

            return order_book
        finally:
            await client.close()

    async def get_funding_rates(
        self,
        market_id: Optional[int] = None,
        trading_pair: Optional[str] = None,
        interval: str = "1h",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get funding rate history for a perpetual market.

        Args:
            market_id: Market ID (either market_id or trading_pair required)
            trading_pair: Trading pair (e.g., "BTC-USDC")
            interval: Interval for funding rates
            start_time: Start timestamp (Unix seconds)
            end_time: End timestamp (Unix seconds)
            limit: Maximum number of records

        Returns:
            DataFrame with funding rate data
        """
        if market_id is None:
            if trading_pair is None:
                raise ValueError("Either market_id or trading_pair must be provided")
            market_id = self.get_market_id(trading_pair)

        if start_time is None:
            start_time = int((datetime.now() - timedelta(days=7)).timestamp())
        if end_time is None:
            end_time = int(datetime.now().timestamp())

        client = await self._get_client()
        try:
            candlestick_api = CandlestickApi(client)

            resolution = self.INTERVAL_MAPPING.get(interval, interval)

            response = await candlestick_api.fundings(
                market_id=market_id,
                resolution=resolution,
                start_timestamp=start_time,
                end_timestamp=end_time,
                count_back=limit
            )

            funding_data = []
            if hasattr(response, 'fundings') and response.fundings:
                for funding in response.fundings:
                    funding_data.append({
                        'timestamp': pd.to_datetime(funding.timestamp, unit='s'),
                        'funding_rate': float(funding.funding_rate) if hasattr(funding, 'funding_rate') else 0.0,
                        'market_id': market_id,
                    })

            df = pd.DataFrame(funding_data)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
                df.sort_index(inplace=True)

            return df
        finally:
            await client.close()

    # Synchronous wrapper methods for backward compatibility
    def get_candles_sync(self, *args, **kwargs) -> Candles:
        """Synchronous wrapper for get_candles."""
        return asyncio.run(self.get_candles(*args, **kwargs))

    def get_trades_sync(self, *args, **kwargs) -> pd.DataFrame:
        """Synchronous wrapper for get_trades."""
        return asyncio.run(self.get_trades(*args, **kwargs))

    def get_order_book_sync(self, *args, **kwargs) -> Dict[str, List[Tuple[float, float]]]:
        """Synchronous wrapper for get_order_book."""
        return asyncio.run(self.get_order_book(*args, **kwargs))

    def get_market_info_sync(self) -> pd.DataFrame:
        """Synchronous wrapper for get_market_info."""
        return asyncio.run(self.get_market_info())
