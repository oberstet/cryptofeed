import sys
import time
from pprint import pprint
from decimal import Decimal

from cryptofeed import FeedHandler
from cryptofeed.defines import (CANDLES, BID, ASK, BLOCKCHAIN, FUNDING, GEMINI,
                                L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST,
                                PERPETUAL, TICKER, TRADES, INDEX)
from cryptofeed.exchanges import FTX, BinanceFutures, Bitfinex, Bitmex, Coinbase, HuobiSwap, OKEx, Bybit
from cryptofeed.exchanges.kraken_futures import KrakenFutures
from cryptofeed.exchanges.dydx import dYdX


async def ticker(t, receipt_timestamp):
    if t.timestamp is not None:
        assert isinstance(t.timestamp, float)
    assert isinstance(t.exchange, str)
    assert isinstance(t.bid, Decimal)
    assert isinstance(t.ask, Decimal)
    # print(f'Ticker received at {receipt_timestamp}: {t}')


async def trade(t, receipt_timestamp):
    assert isinstance(t.timestamp, float)
    assert isinstance(t.side, str)
    assert isinstance(t.amount, Decimal)
    assert isinstance(t.price, Decimal)
    assert isinstance(t.exchange, str)
    # print(f"Trade received at {receipt_timestamp}: {t}")


started = None
count = None
sum_dl = 0

async def book(book, receipt_timestamp):
    global started, count, sum_dl
    if started is None:
        started = time.time_ns()
        count = 1
        sum_dl = 0
    else:
        count += 1
        now = time.time_ns()
        duration = (now - started) / 10**9
        if book.delta:
            sum_dl += len(book.delta[BID]) + len(book.delta[ASK])
        if duration > 1:
            print('\n{} events/sec with {} entries average'.format(count, float(sum_dl) / float(count)))
            started = None
    # sys.stdout.write('.')
    # sys.stdout.flush()
    # print(f'Book received at {receipt_timestamp} for {book.exchange} - {book.symbol}, with {len(book.book)} entries. Top of book prices: {book.book.asks.index(0)[0]} - {book.book.bids.index(0)[0]}')
    # if book.delta:
    #     print(f"Delta from last book contains {len(book.delta[BID]) + len(book.delta[ASK])} entries.")
    # if book.sequence_number:
    #    assert isinstance(book.sequence_number, int)


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

    # 2022-02-08 03:44:50,684 : INFO : BINANCE_FUTURES: feed shutdown starting...
    # 2022-02-08 03:44:50,684 : INFO : BITFINEX: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : BITMEX: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : COINBASE: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : DYDX: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : FTX: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : HUOBI_SWAP: feed shutdown starting...
    # 2022-02-08 03:44:50,685 : INFO : OKEX: feed shutdown starting...

    quote_tokens = ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'BTC', 'ETH']
    print('*' * 100)
    pprint(sorted(quote_tokens))

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
    print('*' * 100)
    pprint(sorted(tokens))

    for e in all_pairs:
        for t in tokens:
            all_pairs[e][t] = 0

    dydx_pairs = dYdX.symbols()

    binance_futures_pairs = []
    for p in BinanceFutures.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
            binance_futures_pairs.append(p)
            all_pairs['binance'][pp[0]] += 1

    ftx_pairs = []
    for p in FTX.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
            ftx_pairs.append(p)
            all_pairs['ftx'][pp[0]] += 1

    if False:
        coinbase_pairs = []
        for p in Coinbase.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in quote_tokens:
                coinbase_pairs.append(p)
                all_pairs['coinbase'][pp[0]] += 1
    else:
        coinbase_pairs = None

    if False:
        kraken_futures_pairs = []
        for p in KrakenFutures.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
                kraken_futures_pairs.append(p)
                # all_pairs['kraken'][pp[0]] += 1
    else:
        kraken_futures_pairs = None

    if False:
        okex_pairs = []
        for p in OKEx.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
                okex_pairs.append(p)
                all_pairs['okex'][pp[0]] += 1
    else:
        okex_pairs = False

    if False:
        bybit_pairs = []
        for p in Bybit.symbols():
            pp = p.split('-')
            if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
                bybit_pairs.append(p)
                # all_pairs['bybit'][pp[0]] += 1
    else:
        bybit_pairs = False

    huobi_pairs = []
    for p in HuobiSwap.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
            huobi_pairs.append(p)
            all_pairs['huobi'][pp[0]] += 1

    bitfinex_pairs = []
    for p in Bitfinex.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and len(pp) > 1 and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
            bitfinex_pairs.append(p)
            all_pairs['bitfinex'][pp[0]] += 1

    bitmex_pairs = []
    for p in Bitmex.symbols():
        pp = p.split('-')
        if pp[0] in set(tokens) and pp[1] in quote_tokens and len(pp) > 2 and pp[2] == 'PERP':
            bitmex_pairs.append(p)
            all_pairs['bitmex'][pp[0]] += 1

    print('*' * 100)
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
    print('*' * 100)

    # sys.exit(0)

    f = FeedHandler(config=config)

    if binance_futures_pairs:
        f.add_feed(BinanceFutures(symbols=binance_futures_pairs, channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    if bitfinex_pairs:
        f.add_feed(Bitfinex(symbols=bitfinex_pairs, channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    if bitmex_pairs:
        f.add_feed(Bitmex(timeout=5000, symbols=bitmex_pairs, channels=[L2_BOOK], callbacks={L2_BOOK:book}))

    if coinbase_pairs:
        f.add_feed(Coinbase(subscription={L2_BOOK: coinbase_pairs}, callbacks={L2_BOOK: book}))

    if dydx_pairs:
        f.add_feed(dYdX(symbols=dYdX.symbols(), channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    if ftx_pairs:
        f.add_feed(FTX(checksum_validation=True, symbols=ftx_pairs, channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    if huobi_pairs:
        f.add_feed(HuobiSwap(symbols=huobi_pairs, channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    if okex_pairs:
        f.add_feed(OKEx(checksum_validation=True, symbols=okex_pairs, channels=[L2_BOOK], callbacks={L2_BOOK: book}))

    f.run()


if __name__ == '__main__':
    main()
