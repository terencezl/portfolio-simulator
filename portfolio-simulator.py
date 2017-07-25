#!/usr/bin/env python
import pandas as pd
import time
from datetime import datetime
from threading import Thread, Lock
import sys
import argparse


class Clock(object):
    """
    Simulate a clock with acceleration.
    """

    def __init__(self, one_hr_as_sec=720):
        """
        param sone_hr_as_sec: defines acceleration. 3600 means regular speed.
                720 (default) means 5 times the regular speed.
        """

        self._time_start = time.time()
        self._one_hr_as_sec = one_hr_as_sec

    def time(self):
        """
        Get current time of the clock.
        """

        return pd.Timestamp('2014-01-01 00:00:00') + pd.Timedelta(time.time() -\
            self._time_start, unit='s') * self._one_hr_as_sec


def get_feed(feed, clock):
    """
    Simulate a feed source that outputs a row if that row has a timestamp
    smaller or equal to current time.

    If no feed is available anymore, raise IOError.
    """

    if not feed:
        raise IOError("No more feed.")

    row = feed.pop(0)
    while True:
        if row['timestamp'] <= clock.time():
            return row


class Portfolio(object):
    """
    A portfolio that tracks fill orders and prints P&L with each price update.
    """

    def __init__(self, fills_records, prices_records, clock, is_test=False):
        """
        param fills_records: fills in the records format.
        param prices_records: prices in the records format.
        """

        self._cash = 0
        self._holdings = {}
        self._prices = {}
        self._is_stopped_fill_update = True
        self._side_to_sign = {'B': 1, 'S': -1}
        self._fills_records = fills_records
        self._prices_records = prices_records
        self._clock = clock
        self.lock = Lock()
        self._is_test = is_test

    def start_fill_update(self):
        """
        Start a thread to update fill orders in the background.
        """

        print("start fill update.")
        self._is_stopped_fill_update = False
        Thread(target=self._fill_update).start()

    def stop_fill_update(self):
        """
        Stop fill update thread.
        """

        self._is_stopped_fill_update = True

    def is_stopped_fill_update(self):
        """
        if fill update thread is stopped.
        """

        return self._is_stopped_fill_update

    def _fill_update(self):
        """
        A loop that continuously retrieves fill orders and updates the current
        holdings and cash.

        If no feed for fills, sleep for 5s and retry.
        """

        while True:
            if self.is_stopped_fill_update():
                print("stop fill update.")
                return

            try:
                row = get_feed(self._fills_records, self._clock)
            except IOError:
                if not self._is_test:
                    print('No feed for fills. Sleep for 5s and retry.')
                    time.sleep(5)
                    continue
                else:
                    break

            with self.lock:
                if row['symbol'] not in self._holdings:
                    self._holdings[row['symbol']] = \
                                row['size'] * self._side_to_sign[row['side']]
                else:
                    self._holdings[row['symbol']] += \
                                row['size'] * self._side_to_sign[row['side']]
                self._cash -= \
                    row['size'] * row['price'] * self._side_to_sign[row['side']]

    def get_cash(self):
        """
        Get cash.
        """

        return self._cash

    def get_mtm(self):
        """
        Get mark to market value of all current holdings.
        """

        try:
            return sum([self._prices[key] * value
                for key, value in self._holdings.items()])
        except KeyError:
            print("Warning: Current price not found for all holdings. This only affects one line below.")
            return sum([self._prices[key] * value
                for key, value in self._holdings.items() if key in self._prices])

    def get_pnl(self):
        """
        Get P&L: cash + mtm.
        """

        return self.get_cash() + self.get_mtm()

    def get_price_update(self):
        """
        A loop that continuously retrieves price updates and prints out the
        P&L message. Format:

        1)  Message type: This is always PNL for this file
        2)  Milliseconds from the unix timestamp
        3)  Symbol name
        4)  Signed Size Owned
        5)  Mark to Market P&L

        If no feed for prices, sleep for 5s and retry.
        """

        while True:
            try:
                row = get_feed(self._prices_records, self._clock)
            except IOError:
                if not self._is_test:
                    print('No feed for prices. Sleep for 5s and retry.')
                    time.sleep(5)
                    continue
                else:
                    break

            self._prices[row['symbol']] = row['price']
            timestamp = int((row['timestamp'] - datetime(1970,1,1))\
                                .total_seconds() * 1000)
            with self.lock:
                if row['symbol'] in self._holdings:
                    print("PNL %s %s %s %.2f" % \
                     (timestamp, row['symbol'], self._holdings[row['symbol']], self.get_pnl()))


def test_portfolio():
    from nose.tools import assert_almost_equal
    """
    Get one day of fill orders and prices and assert cash, mtm and pnl values are expected.

    Install nose, then run with `nosetest positionservice.py` along with fills.gz and prices.gz.

    Output:
    .
    ----------------------------------------------------------------------
    Ran 1 test in 5.994s

    OK
    """

    fills = pd.read_table('fills.gz', sep='\s+', header=None,
        names=['type', 'timestamp', 'symbol', 'price', 'size', 'side'])
    fills['timestamp'] = pd.to_datetime(fills['timestamp'], unit='ms')
    fills = fills.query('timestamp < "2014-01-02"')

    prices = pd.read_table('prices.gz', sep='\s+', header=None,
        names=['type', 'timestamp', 'symbol', 'price'])
    prices['timestamp'] = pd.to_datetime(prices['timestamp'], unit='ms')
    prices = prices.query('timestamp < "2014-01-02 01:00:00"')

    # convert to records format
    fills_records = fills.to_dict('records')
    prices_records = prices.to_dict('records')

    # start the clock
    clock = Clock(one_hr_as_sec=18000)
    portfolio = Portfolio(fills_records, prices_records, clock, is_test=True)
    # start receiving updates
    portfolio.start_fill_update()
    portfolio.get_price_update()
    portfolio.stop_fill_update()

    print(portfolio.get_cash(), portfolio.get_mtm(), portfolio.get_pnl())

    assert_almost_equal(portfolio.get_cash(), -676493.0)
    assert_almost_equal(portfolio.get_mtm(), 666720.0)
    assert_almost_equal(portfolio.get_pnl(), -9773.0)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("fills", help="fills or fills.gz")
    ap.add_argument("prices", help="prices or prices.gz")
    args = ap.parse_args()

    # load and convert fills and prices
    fills = pd.read_table(args.fills, sep='\s+', header=None,
        names=['type', 'timestamp', 'symbol', 'price', 'size', 'side'])
    fills['timestamp'] = pd.to_datetime(fills['timestamp'], unit='ms')

    prices = pd.read_table(args.prices, sep='\s+', header=None,
        names=['type', 'timestamp', 'symbol', 'price'])
    prices['timestamp'] = pd.to_datetime(prices['timestamp'], unit='ms')

    # convert to records format
    fills_records = fills.to_dict('records')
    prices_records = prices.to_dict('records')

    # start the clock
    clock = Clock(one_hr_as_sec=3600)
    portfolio = Portfolio(fills_records, prices_records, clock)
    # start receiving updates
    try:
        portfolio.start_fill_update()
        portfolio.get_price_update()
    except KeyboardInterrupt:
        print('stop price update.')
        portfolio.stop_fill_update()
