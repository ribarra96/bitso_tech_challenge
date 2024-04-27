import requests
import time
import os
import json
from datetime import datetime

class GET_BOOKS:
    def __init__(self, books, output_directory):
        self.books = books
        self.output_directory = output_directory
        self.duration = duration
    @staticmethod
    def get_order_book(book):
        url = f"https://sandbox.bitso.com/api/v3/order_book?book={book}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch order book for {book}")
            return None
    @staticmethod
    def calculate_spread(order_book):
        if order_book is not None:
            best_bid = float(order_book['payload']['bids'][0]['price'])
            best_ask = float(order_book['payload']['asks'][0]['price'])
            spread = (best_ask - best_bid) * 100 / best_ask
            return spread
        else:
            return None

    def record_orderbook(self, book, orderbook_timestamp, bid, ask, spread):
        data = {
            "orderbook_timestamp": orderbook_timestamp,
            "book": book,
            "bid": bid,
            "ask": ask,
            "spread": spread
        }
        filename = f"{self.output_directory}/{orderbook_timestamp[:10]}.json"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        with open(filename, 'a') as f:
            json.dump(data, f)
            f.write('\n')

    def monitor_order_books(self):
        start_time = time.time()
        end_time = start_time + self.duration
        while time.time() < end_time:
            timestamp = datetime.now().isoformat()
            for book in books:
                order_book = self.get_order_book(book)
                if order_book:
                    bid = float(order_book['payload']['bids'][0]['price'])
                    ask = float(order_book['payload']['asks'][0]['price'])
                    spread = self.calculate_spread(order_book)
                    if spread is not None:
                        self.record_orderbook(book, timestamp, bid, ask, spread, output_directory)
            time.sleep(1)  # Adjust the interval as nee

books = ['btc_mxn', 'usd_mxn']
output_directory = "~/bitso_tech_challenge/challenge_1/target_files/"
duration = 600
monitor = GET_BOOKS(books, output_directory, duration)
monitor.monitor_order_books()
