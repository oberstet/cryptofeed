import sys
import time
import platform
from pprint import pprint
from decimal import Decimal

from cryptofeed import FeedHandler
from cryptofeed.defines import (CANDLES, BID, ASK, BLOCKCHAIN, FUNDING, GEMINI,
                                L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST,
                                PERPETUAL, TICKER, TRADES, INDEX)
from cryptofeed.exchanges import FTX, BinanceFutures, Bitfinex, Bitmex, Coinbase, HuobiSwap, OKEx, Bybit
from cryptofeed.exchanges.kraken_futures import KrakenFutures
from cryptofeed.exchanges.dydx import dYdX

PYPY = platform.python_implementation() == 'PyPy'

# 1. BINANCE_FUTURES
# 2. BITFINEX
# 3. BITMEX
# 4. COINBASE
# 5. DYDX
# 6. FTX
# 7. HUOBI_SWAP
# 8. OKEX

# ping -c 10 stream.binancefuture.com
# ping -c 10 api.bitfinex.com
# ping -c 10 www.bitmex.com
# ping -c 10 ws-feed.pro.coinbase.com
# ping -c 10 api.dydx.exchange
# ping -c 10 ftx.com
# ping -c 10 api.hbdm.com
# ping -c 10 ws.okex.com


QUOTE_TOKENS = ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'BTC', 'ETH']


async def ticker(t, receipt_timestamp):
    if t.timestamp is not None:
        assert isinstance(t.timestamp, float)
    assert isinstance(t.exchange, str)
    assert isinstance(t.bid, Decimal)
    assert isinstance(t.ask, Decimal)
    # print(f'Ticker received at {receipt_timestamp}: {t}')


TRADE_started = None
TRADE_count = None


async def trade(t, receipt_timestamp):
    assert isinstance(t.timestamp, float)
    assert isinstance(t.side, str)
    assert isinstance(t.amount, Decimal)
    assert isinstance(t.price, Decimal)
    assert isinstance(t.exchange, str)
    global TRADE_started, TRADE_count
    if TRADE_started is None:
        TRADE_started = time.time_ns()
        TRADE_count = 1
    else:
        TRADE_count += 1
        now = time.time_ns()
        duration = (now - TRADE_started) / 10**9
        if duration > 1:
            print('\n{} TRADE events/sec'.format(TRADE_count))
            TRADE_started = None

L2_BOOK_started = None
L2_BOOK_count = None
L2_BOOK_sum_dl = 0


async def book(book, receipt_timestamp):
    global L2_BOOK_started, L2_BOOK_count, L2_BOOK_sum_dl
    if L2_BOOK_started is None:
        L2_BOOK_started = time.time_ns()
        L2_BOOK_count = 1
        L2_BOOK_sum_dl = 0
    else:
        L2_BOOK_count += 1
        now = time.time_ns()
        duration = (now - L2_BOOK_started) / 10**9
        if book.delta:
            L2_BOOK_sum_dl += len(book.delta[BID]) + len(book.delta[ASK])
        if duration > 1:
            print('\n{} L2_BOOK events/sec ({} book updates per event average)'.format(L2_BOOK_count, float(L2_BOOK_sum_dl) / float(L2_BOOK_count)))
            L2_BOOK_started = None


async def funding(f, receipt_timestamp):
    print(f"Funding update received at {receipt_timestamp}: {f}")


async def oi(update, receipt_timestamp):
    print(f"Open Interest update received at {receipt_timestamp}: {update}")


async def index(i, receipt_timestamp):
    print(f"Index received at {receipt_timestamp}: {i}")


async def candle_callback(c, receipt_timestamp):
    print(f"Candle received at {receipt_timestamp}: {c}")


async def liquidations(liquidation, receipt_timestamp):
    print(f"Liquidation received at {receipt_timestamp}: {liquidation}")


def main():
    config = {'log': {'filename': 'demo.log', 'level': 'INFO', 'disabled': False}}

    #print('*' * 100)
    #pprint(sorted(QUOTE_TOKENS))

    all_pairs = {
        'binance': {},
        'bitfinex': {},
        'bitmex': {},
        'coinbase': {},
        'dydx': {},
        'ftx': {},
        'huobi': {},
        'okex': {},
    }

    tokens = [x.split('-')[0] for x in dYdX.symbols()]
    #print('*' * 100)
    #pprint(sorted(tokens))

    for e in all_pairs:
        for t in tokens:
            all_pairs[e][t] = 0

    dydx_pairs = dYdX.symbols()
    for t in tokens:
        all_pairs['dydx'][t] = 1

    binance_futures_pairs = []
    for p in BinanceFutures.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
            binance_futures_pairs.append(p)
            all_pairs['binance'][pp[0]] += 1

    ftx_pairs = []
    for p in FTX.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
            ftx_pairs.append(p)
            all_pairs['ftx'][pp[0]] += 1

    if not PYPY:
        coinbase_pairs = []
        for p in Coinbase.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS:
                coinbase_pairs.append(p)
                all_pairs['coinbase'][pp[0]] += 1
    else:
        coinbase_pairs = None

    if False:
        kraken_futures_pairs = []
        for p in KrakenFutures.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
                kraken_futures_pairs.append(p)
                # all_pairs['kraken'][pp[0]] += 1
    else:
        kraken_futures_pairs = None

    if not PYPY:
        okex_pairs = []
        for p in OKEx.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
                okex_pairs.append(p)
                all_pairs['okex'][pp[0]] += 1
    else:
        okex_pairs = False

    if False:
        bybit_pairs = []
        for p in Bybit.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
                bybit_pairs.append(p)
                # all_pairs['bybit'][pp[0]] += 1
    else:
        bybit_pairs = False

    huobi_pairs = []
    for p in HuobiSwap.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
            huobi_pairs.append(p)
            all_pairs['huobi'][pp[0]] += 1

    bitfinex_pairs = []
    for p in Bitfinex.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and len(pp) > 1 and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
            bitfinex_pairs.append(p)
            all_pairs['bitfinex'][pp[0]] += 1

    bitmex_pairs = []
    for p in Bitmex.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in QUOTE_TOKENS and len(pp) > 2 and pp[2] == 'PERP':
            bitmex_pairs.append(p)
            all_pairs['bitmex'][pp[0]] += 1

    print('=' * 100)
    print()
    if binance_futures_pairs:
        print('BinanceFutures {}'.format(len(binance_futures_pairs)))
    if bitfinex_pairs:
        print('Bitfinex       {}'.format(len(bitfinex_pairs)))
    if bitmex_pairs:
        print('Bitmex         {}'.format(len(bitmex_pairs)))
    if coinbase_pairs:
        print('Coinbase       {}'.format(len(coinbase_pairs)))
    if dydx_pairs:
        print('dYdX           {}'.format(len(dydx_pairs)))
    if ftx_pairs:
        print('FTX            {}'.format(len(ftx_pairs)))
    if huobi_pairs:
        print('HuobiSwap      {}'.format(len(huobi_pairs)))
    if okex_pairs:
        print('OKEx           {}'.format(len(okex_pairs)))

    s = 0
    s += len(binance_futures_pairs)
    s += len(bitfinex_pairs)
    s += len(bitmex_pairs)
    s += len(coinbase_pairs)
    s += len(dydx_pairs)
    s += len(ftx_pairs)
    s += len(huobi_pairs)
    s += len(okex_pairs)
    print()
    print('All            {}'.format(s))

    print()
    print('-' * 100)
    print()

    #pprint(all_pairs)
    #print('*' * 100)

    m = []
    for t in sorted(tokens):
        v = []
        for e in sorted(all_pairs.keys()):
            v.append(all_pairs[e][t])
        m.append(v)

    print('Exchanges:')
    pprint(sorted(all_pairs.keys()))
    print()
    print('Base Assets:')
    pprint(sorted(tokens))
    print()
    print('Quote Assets:')
    pprint(sorted(QUOTE_TOKENS))
    print()
    print('Availability of Pairs (Perpetual Futures):')
    pprint(m)
    print()
    print('=' * 100)

    sys.exit(0)

    f = FeedHandler(config=config)

    if binance_futures_pairs:
        f.add_feed(BinanceFutures(symbols=binance_futures_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if bitfinex_pairs:
        f.add_feed(Bitfinex(symbols=bitfinex_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if bitmex_pairs:
        f.add_feed(Bitmex(timeout=5000, symbols=bitmex_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if coinbase_pairs:
        f.add_feed(Coinbase(subscription={L2_BOOK: coinbase_pairs, TRADES: coinbase_pairs}, callbacks={L2_BOOK: book, TRADES: trade}))

    if dydx_pairs:
        f.add_feed(dYdX(symbols=dYdX.symbols(), channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if ftx_pairs:
        f.add_feed(FTX(checksum_validation=True, symbols=ftx_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if huobi_pairs:
        f.add_feed(HuobiSwap(symbols=huobi_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    if okex_pairs:
        f.add_feed(OKEx(checksum_validation=True, symbols=okex_pairs, channels=[L2_BOOK, TRADES], callbacks={L2_BOOK: book, TRADES: trade}))

    f.run()


if __name__ == '__main__':
    main()
