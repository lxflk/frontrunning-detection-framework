#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import requests
from datetime import datetime, timezone


class colors:
    INFO = "\033[94m"
    OK = "\033[92m"
    FAIL = "\033[91m"
    END = "\033[0m"


def get_prices():
    """
    Fetches historical Ethereum prices in USD with daily granularity starting with a specific date using CryptoCompare API.
    Logs the response if an error occurs.
    """
    try:
        api_key = "45d559f2a0ca65c3cf827c9ffc4fc0337f6a83b3560fc4dd7eeaa35437935d27"  # Replace with your CryptoCompare API key
        symbol = "ETH"
        currency = "USD"

        # Set the start date and calculate the number of days from start date to today
        start_date = "2020-11-20 07:03:58"  # Start date in string format (Captures block 11300000, last block analyzed by Torres)
        start_timestamp = int(
            datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").timestamp()
        )
        current_timestamp = int(time.time())

        # Calculate the number of days to fetch data for
        days_to_fetch = (current_timestamp - start_timestamp) // (24 * 60 * 60)

        # Make the API request
        url = f"https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym={currency}&limit={days_to_fetch-1}&toTs={current_timestamp}&api_key={api_key}"

        response = requests.get(url)
        data = response.json()

        # Check for valid response
        if data.get("Response") != "Success" or "Data" not in data.get("Data", {}):
            print(f"API Response: {response.text}")
            print("Error: Unable to fetch data from CryptoCompare.")
            return []

        # Extract prices
        prices = [
            {"time": day["time"], "price": day["close"]} for day in data["Data"]["Data"]
        ]

        if not prices:
            print("Error: No prices were retrieved from the API.")
            return []

        return prices

    except requests.RequestException as e:
        print(f"Error fetching prices: {e}")
        return []


def get_one_eth_to_usd(timestamp, prices):
    if not prices or len(prices) == 0:
        return 0  # Fallback for empty prices dataset

    # Convert to midnight UTC
    midnight_timestamp = int(
        datetime.fromtimestamp(timestamp, timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    )

    # Look for exact match
    for price_entry in prices:
        if price_entry["time"] == midnight_timestamp:
            return price_entry["price"]

    print("Didnt find exact price, use nearest timestamp")
    # Fallback: find the nearest timestamp
    nearest_price = min(prices, key=lambda x: abs(x["time"] - midnight_timestamp))
    return nearest_price["price"]


def request_debug_trace(
    connection,
    transaction_hash,
    custom_tracer=True,
    request_timeout=100,
    disable_stack=True,
    disable_memory=True,
    disable_storage=True,
):
    """
    Request a debug trace for a transaction using Geth's debug_traceTransaction.
    """
    data, tracer = None, None
    if custom_tracer:
        with open(
            os.path.join(os.path.dirname(__file__), "call_tracer.js"), "r"
        ) as file:
            tracer = file.read().replace("\n", "")
    if tracer:
        data = json.dumps(
            {
                "id": 1,
                "method": "debug_traceTransaction",
                "params": [
                    transaction_hash,
                    {"tracer": tracer, "timeout": str(request_timeout) + "s"},
                ],
            }
        )
    else:
        data = json.dumps(
            {
                "id": 1,
                "method": "debug_traceTransaction",
                "params": [
                    transaction_hash,
                    {
                        "disableStack": disable_stack,
                        "disableMemory": disable_memory,
                        "disableStorage": disable_storage,
                    },
                ],
            }
        )
    headers = {"Content-Type": "application/json"}
    connection.request("GET", "/", data, headers)
    response = connection.getresponse()
    if response.status == 200 and response.reason == "OK":
        return json.loads(response.read())
    return {
        "error": {
            "status": response.status,
            "reason": response.reason,
            "data": response.read().decode(),
        }
    }
