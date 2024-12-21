#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:
    python3 insertion.py <BLOCK_RANGE_START>:<BLOCK_RANGE_END>

Example:
    python3 insertion.py 10000000:10000010
"""

import os
import sys
import time
import numpy
import decimal
import pymongo
import requests
import multiprocessing

from web3 import Web3
from web3.providers.legacy_websocket import LegacyWebSocketProvider

# Ensure we can import from the sibling "utils" directory
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils.settings import WEB3_WS_PROVIDER, MONGO_HOST, MONGO_PORT, ETHERSCAN_API_KEY
from utils.utils import colors, get_prices, get_one_eth_to_usd

##############################################################################
# Constants
##############################################################################
TOKEN_AMOUNT_DELTA = (
    0.01  # Maximum difference between buying and selling amount of tokens (1% default).
)
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"  # ERC-20 "Transfer" event hash
TOKEN_PURCHASE = "0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f"  # Uniswap V1 "TokenPurchase"
ETH_PURCHASE = "0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705"  # Uniswap V1 "ETHPurchase"

##############################################################################
# Global references set during init_process()
##############################################################################
w3 = None
prices = []
mongo_connection = None


##############################################################################
# MongoDB Helpers
##############################################################################
def is_block_analyzed(block_number):
    """
    Check if a block has already been analyzed by looking it up in the 'insertion_status' collection in MongoDB.
    Returns the record if found, otherwise None.
    """
    status = mongo_connection["front_running"]["insertion_status"].find_one(
        {"block_number": block_number}
    )
    return status


def store_block_status(block_number, execution_time):
    """
    Store the block analysis status into the 'insertion_status' collection in MongoDB
    to mark it as analyzed (helps avoid repeated work).
    """
    collection = mongo_connection["front_running"]["insertion_status"]
    collection.insert_one(
        {"block_number": block_number, "execution_time": execution_time}
    )

    # Create index for 'block_number' if not already present.
    if "block_number" not in collection.index_information():
        collection.create_index("block_number")


##############################################################################
# Event Fetching
##############################################################################
def fetch_transfer_events(block_number):
    """
    Fetch all ERC-20 'Transfer' logs for a specific block using web3.eth.filter().
    Returns a list of matching transfer event logs.
    """
    events = []
    try:
        # Filter only for transfer events in the given block range
        transfer_filter = {
            "fromBlock": block_number,
            "toBlock": block_number,
            "topics": [TRANSFER],
        }
        events = w3.eth.filter(transfer_filter).get_all_entries()
    except Exception as e:
        print(
            colors.FAIL
            + f"Error fetching transfer events for block {block_number}: {e}"
            + colors.END
        )
    return events


def parse_transfer_event(event):
    """
    Extract the 'from', 'to', and 'value' fields from a raw 'Transfer' event.
    This function returns None if any field is missing or invalid.
    """
    event_data = event["data"].hex().replace("0x", "")
    # The ERC-20 'Transfer' event typically has 3 indexed topics + data for value.
    if event_data and len(event["topics"]) == 3:
        _from = w3.to_checksum_address(
            "0x" + event["topics"][1].hex().replace("0x", "")[24:64]
        )
        _to = w3.to_checksum_address(
            "0x" + event["topics"][2].hex().replace("0x", "")[24:64]
        )
        _value = int(event_data[0:64], 16)
        return _from, _to, _value
    return None


##############################################################################
# Exchange Identification
##############################################################################
def identify_exchange_name(address):
    """
    Attempts to identify the exchange name by calling various on-chain methods:
    1) name() for Uniswap V2/SushiSwap
    2) name() returning bytes32 for Uniswap V1
    3) converterType() for Bancor
    4) Etherscan contract API
    If all attempts fail, returns the original address as the fallback name.
    """
    # 1) Attempt name() (string)
    try:
        exchange_contract = w3.eth.contract(
            address=address,
            abi=[
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [
                        {"internalType": "string", "name": "", "type": "string"}
                    ],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function",
                }
            ],
        )
        exchange_name = exchange_contract.functions.name().call()
        if exchange_name.startswith("SushiSwap"):
            return "SushiSwap"
        return exchange_name
    except:
        pass

    # 2) Attempt name() returning bytes32 (Uniswap V1 style)
    try:
        exchange_contract = w3.eth.contract(
            address=address,
            abi=[
                {
                    "name": "name",
                    "outputs": [{"type": "bytes32", "name": "out"}],
                    "inputs": [],
                    "constant": True,
                    "payable": False,
                    "type": "function",
                    "gas": 1623,
                }
            ],
        )
        exchange_name_bytes = exchange_contract.functions.name().call()
        exchange_name = exchange_name_bytes.decode("utf-8").replace("\u0000", "")
        return exchange_name
    except:
        pass

    # 3) Attempt Bancor converterType()
    try:
        exchange_contract = w3.eth.contract(
            address=address,
            abi=[
                {
                    "constant": True,
                    "inputs": [],
                    "name": "converterType",
                    "outputs": [{"name": "", "type": "string"}],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function",
                }
            ],
        )
        exchange_name = exchange_contract.functions.converterType().call().capitalize()
        if exchange_name.startswith("Bancor"):
            return "Bancor"
        return exchange_name
    except:
        pass

    # 4) Etherscan fallback
    try:
        response = requests.get(
            f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={ETHERSCAN_API_KEY}"
        ).json()
        exchange_name = response["result"][0]["ContractName"]
        if exchange_name.startswith("Bancor"):
            return "Bancor"
        return exchange_name
    except:
        pass

    # Fallback to just returning the address if no name identified
    return address


def identify_token_name(token_address):
    """
    Attempts to retrieve the token's 'name' by calling name() as a string or bytes32.
    If all attempts fail, returns the token_address as the fallback.
    """
    # Attempt name() returning string
    try:
        token_contract = w3.eth.contract(
            address=token_address,
            abi=[
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [
                        {"internalType": "string", "name": "", "type": "string"}
                    ],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function",
                }
            ],
        )
        return token_contract.functions.name().call()
    except:
        pass

    # Attempt name() returning bytes32
    try:
        token_contract = w3.eth.contract(
            address=token_address,
            abi=[
                {
                    "name": "name",
                    "outputs": [{"type": "bytes32", "name": "out"}],
                    "inputs": [],
                    "constant": True,
                    "payable": False,
                    "type": "function",
                    "gas": 1623,
                }
            ],
        )
        token_name = (
            token_contract.functions.name().call().decode("utf-8").replace("\u0000", "")
        )
        return token_name
    except:
        pass

    # Fallback: use the address as the "name"
    return token_address


##############################################################################
# Sandwich Attack Checks / Calculations
##############################################################################
def look_for_whale_event(events_for_token, event_a1, event_a2, attackers_set):
    """
    Given the first (A1) and second (A2) attacker events, search the list of events_for_token
    to identify a Whale transaction that sits in between (A1 < Whale < A2).
    Whale event must not be in attackers_set already.
    Returns the whale event if found, else None.
    """
    for asset_transfer in events_for_token:
        if (
            event_a1["transactionIndex"]
            < asset_transfer["transactionIndex"]
            < event_a2["transactionIndex"]
            and asset_transfer["transactionHash"].hex() not in attackers_set
        ):
            return asset_transfer
    return None


def parse_addresses_and_values(event):
    """
    Given an ERC-20 transfer event, parse the from/to addresses and the value.
    Returns (_from, _to, _value).
    """
    _from = w3.to_checksum_address(
        "0x" + event["topics"][1].hex().replace("0x", "")[24:64]
    )
    _to = w3.to_checksum_address(
        "0x" + event["topics"][2].hex().replace("0x", "")[24:64]
    )
    _value = int(event["data"].hex()[0:64], 16)
    return _from, _to, _value


def compute_cost_and_gain(tx1, tx2, block_number, events, whale_tx):
    """
    Compute cost in gas (for tx1 and tx2), and the gain in terms of WETH or fallback detection for Uniswap V1.
    Returns (total_cost, gain, eth_spent, eth_received, eth_whale).
    """
    # Compute cost from gas usage
    receipt1 = w3.eth.get_transaction_receipt(tx1["hash"])
    cost1 = receipt1["gasUsed"] * tx1["gasPrice"]
    receipt2 = w3.eth.get_transaction_receipt(tx2["hash"])
    cost2 = receipt2["gasUsed"] * tx2["gasPrice"]
    total_cost = cost1 + cost2

    eth_spent, eth_received, eth_whale = 0, 0, 0
    gain = None

    # Look for WETH (or Bancor ETH) transfer logs in the same block
    tx1_event, tx2_event, whale_event_eth = None, None, None
    for transfer_event in events:
        if (
            not tx1_event
            and transfer_event["transactionHash"] == tx1["hash"]
            and transfer_event["address"].lower()
            in [
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "0xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315",
            ]
        ):
            tx1_event = transfer_event
        elif (
            not tx2_event
            and transfer_event["transactionHash"] == tx2["hash"]
            and transfer_event["address"].lower()
            in [
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "0xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315",
            ]
        ):
            tx2_event = transfer_event
        elif (
            not whale_event_eth
            and transfer_event["transactionHash"] == whale_tx["hash"]
            and transfer_event["address"].lower()
            in [
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "0xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315",
            ]
        ):
            whale_event_eth = transfer_event

        if tx1_event and tx2_event and whale_event_eth:
            break

    if tx1_event and tx2_event and whale_event_eth:
        eth_spent = int(tx1_event["data"].hex().replace("0x", "")[0:64], 16)
        eth_received = int(tx2_event["data"].hex().replace("0x", "")[0:64], 16)
        eth_whale = int(whale_event_eth["data"].hex().replace("0x", "")[0:64], 16)
        gain = eth_received - eth_spent
    else:
        # Try to detect the fallback method for Uniswap V1
        exchange_events = []
        try:
            exchange_events += w3.eth.filter(
                {
                    "fromBlock": block_number,
                    "toBlock": block_number,
                    "topics": [TOKEN_PURCHASE],
                }
            ).get_all_entries()
            exchange_events += w3.eth.filter(
                {
                    "fromBlock": block_number,
                    "toBlock": block_number,
                    "topics": [ETH_PURCHASE],
                }
            ).get_all_entries()
        except:
            pass

        tx1_event_v1, tx2_event_v1, whale_event_v1 = None, None, None
        for ex_evt in exchange_events:
            if ex_evt["transactionHash"] == tx1["hash"]:
                tx1_event_v1 = ex_evt
            elif ex_evt["transactionHash"] == tx2["hash"]:
                tx2_event_v1 = ex_evt
            elif ex_evt["transactionHash"] == whale_tx["hash"]:
                whale_event_v1 = ex_evt

            if tx1_event_v1 and tx2_event_v1 and whale_event_v1:
                break

        # If we match the typical pattern: Attack buy = TOKEN_PURCHASE, Attack sell = ETH_PURCHASE
        if (
            tx1_event_v1
            and tx2_event_v1
            and tx1_event_v1["address"] == tx2_event_v1["address"]
            and ("0x" + tx1_event_v1["topics"][0].hex()) == TOKEN_PURCHASE
            and ("0x" + tx2_event_v1["topics"][0].hex()) == ETH_PURCHASE
        ):
            eth_spent = int(tx1_event_v1["topics"][2].hex(), 16)
            eth_received = int(tx2_event_v1["topics"][3].hex(), 16)
            eth_whale = int(tx1_event_v1["topics"][2].hex(), 16)
            gain = eth_received - eth_spent

    return total_cost, gain, eth_spent, eth_received, eth_whale


def insert_finding_into_db(
    block_number,
    block_timestamp,
    tx1,
    whale_tx,
    tx2,
    one_eth_to_usd_price,
    total_cost,
    gain,
    profit,
    exchange_address,
    exchange_name,
    token_address,
    token_name,
    eth_spent,
    eth_whale,
    eth_received,
    _value_a1,
    _value_w,
    _value_a2,
    interface,
    bot_address,
    same_sender,
    same_receiver,
    same_token_amount,
):
    """
    Insert a detected frontrunning finding (insertion attack) into the 'insertion_results' collection.
    Also ensures the relevant indexes are created if not already present.
    """
    collection = mongo_connection["front_running"]["insertion_results"]

    # Convert tx objects to dictionaries, remove certain fields, and make them JSON-friendly
    tx1_dict = dict(tx1)
    whale_tx_dict = dict(whale_tx)
    tx2_dict = dict(tx2)

    for d in [tx1_dict, whale_tx_dict, tx2_dict]:
        d.pop("blockNumber", None)
        d.pop("blockHash", None)
        d.pop("r", None)
        d.pop("s", None)
        d.pop("v", None)
        d["value"] = str(d["value"])
        d["hash"] = d["hash"].hex()

    # Calculate numeric values to store
    if gain is not None:
        gain_eth = (
            float(Web3.from_wei(gain, "ether"))
            if gain >= 0
            else -float(Web3.from_wei(abs(gain), "ether"))
        )
    else:
        gain_eth = 0.0

    if profit >= 0:
        profit_eth = float(Web3.from_wei(profit, "ether"))
    else:
        profit_eth = -float(Web3.from_wei(abs(profit), "ether"))

    finding = {
        "block_number": block_number,
        "block_timestamp": block_timestamp,
        "first_transaction": tx1_dict,
        "whale_transaction": whale_tx_dict,
        "second_transaction": tx2_dict,
        "eth_usd_price": float(one_eth_to_usd_price),
        "cost_eth": float(Web3.from_wei(total_cost, "ether")),
        "cost_usd": float(Web3.from_wei(total_cost, "ether"))
        * float(one_eth_to_usd_price),
        "gain_eth": gain_eth,
        "gain_usd": float(gain_eth) * float(one_eth_to_usd_price),
        "profit_eth": profit_eth,
        "profit_usd": abs(profit_eth) * float(one_eth_to_usd_price),
        "exchange_address": exchange_address,
        "exchange_name": exchange_name,
        "token_address": token_address,
        "token_name": token_name,
        "first_transaction_eth_amount": str(eth_spent),
        "whale_transaction_eth_amount": str(eth_whale),
        "second_transaction_eth_amount": str(eth_received),
        "first_transaction_token_amount": str(_value_a1),
        "whale_transaction_token_amount": str(_value_w),
        "second_transaction_token_amount": str(_value_a2),
        "interface": interface,
        "bot_address": bot_address,
        "same_sender": same_sender,
        "same_receiver": same_receiver,
        "same_token_amount": same_token_amount,
    }

    collection.insert_one(finding)

    # Create indexes if not already present
    if "block_number" not in collection.index_information():
        collection.create_index("block_number")
        collection.create_index("block_timestamp")
        collection.create_index("eth_usd_price")
        collection.create_index("cost_eth")
        collection.create_index("cost_usd")
        collection.create_index("gain_eth")
        collection.create_index("gain_usd")
        collection.create_index("profit_eth")
        collection.create_index("profit_usd")
        collection.create_index("exchange_address")
        collection.create_index("exchange_name")
        collection.create_index("token_address")
        collection.create_index("token_name")
        collection.create_index("first_transaction_eth_amount")
        collection.create_index("whale_transaction_eth_amount")
        collection.create_index("second_transaction_eth_amount")
        collection.create_index("first_transaction_token_amount")
        collection.create_index("whale_transaction_token_amount")
        collection.create_index("second_transaction_token_amount")
        collection.create_index("interface")
        collection.create_index("bot_address")
        collection.create_index("same_sender")
        collection.create_index("same_receiver")
        collection.create_index("same_token_amount")


def handle_detected_sandwich(
    event_a1,
    event_a2,
    whale_event,
    block_number,
    events,
    transfer_to_map,
    asset_transfers_map,
    whales_set,
    attackers_set,
):
    """
    Once a sandwich pattern is suspected (A1 -> Whale -> A2),
    validate the gasPrice ordering, compute gains, identify exchange/token names,
    and finally log the discovery to DB.
    """
    # Basic parse
    _from_a1, _to_a1, _value_a1 = parse_addresses_and_values(event_a1)
    _from_a2, _to_a2, _value_a2 = parse_addresses_and_values(event_a2)
    _from_w = w3.to_checksum_address(
        "0x" + whale_event["topics"][1].hex().replace("0x", "")[24:64]
    )
    _to_w = w3.to_checksum_address(
        "0x" + whale_event["topics"][2].hex().replace("0x", "")[24:64]
    )
    _value_w = int(whale_event["data"].hex()[0:64], 16)

    # Convert to full transaction objects
    tx1 = w3.eth.get_transaction(event_a1["transactionHash"])
    whale_tx = w3.eth.get_transaction(whale_event["transactionHash"])
    tx2 = w3.eth.get_transaction(event_a2["transactionHash"])

    # Check typical sandwich gasPrice rules
    if (
        tx1["from"] != whale_tx["from"]
        and tx2["from"] != whale_tx["from"]
        and tx1["gasPrice"] > whale_tx["gasPrice"]
        and tx2["gasPrice"] <= whale_tx["gasPrice"]
    ):
        # Identify exchange if possible
        token_address = whale_event["address"]
        token_name = identify_token_name(token_address)

        exchange_address = _from_w  # Whale's 'from' is presumably the DEX
        exchange_name = identify_exchange_name(exchange_address)

        # Compute cost/gain
        total_cost, gain, eth_spent, eth_received, eth_whale = compute_cost_and_gain(
            tx1, tx2, block_number, events, whale_tx
        )
        if gain is not None:
            # Mark events as attacker
            attackers_set.add(event_a1["transactionHash"].hex())
            attackers_set.add(event_a2["transactionHash"].hex())
            whales_set.add(whale_event["transactionHash"].hex())

            # Display summary info
            print(
                "   Index BlockNum \t Transaction Hash \t\t\t From \t\t\t\t To \t\t\t\t Gas Price \t Exchange (Token)"
            )
            print(
                "1. "
                + str(tx1["transactionIndex"])
                + " \t "
                + str(tx1["blockNumber"])
                + " \t "
                + tx1["hash"].hex()
                + " \t "
                + tx1["from"]
                + " \t "
                + tx1["to"]
                + " \t "
                + str(tx1["gasPrice"])
            )
            print(
                colors.INFO
                + "W: "
                + str(whale_tx["transactionIndex"])
                + " \t "
                + str(whale_tx["blockNumber"])
                + " \t "
                + whale_tx["hash"].hex()
                + " \t "
                + whale_tx["from"]
                + " \t "
                + whale_tx["to"]
                + " \t "
                + str(whale_tx["gasPrice"])
                + " \t "
                + exchange_name
                + " ("
                + token_name
                + ")"
                + colors.END
            )
            print(
                "2. "
                + str(tx2["transactionIndex"])
                + " \t "
                + str(tx2["blockNumber"])
                + " \t "
                + tx2["hash"].hex()
                + " \t "
                + tx2["from"]
                + " \t "
                + tx2["to"]
                + " \t "
                + str(tx2["gasPrice"])
            )

            print("Cost: " + str(Web3.from_wei(total_cost, "ether")) + " ETH")

            if gain > 0:
                print("Gain: " + str(Web3.from_wei(gain, "ether")) + " ETH")
            else:
                print("Gain: -" + str(Web3.from_wei(abs(gain), "ether")) + " ETH")

            profit = gain - total_cost
            block_data = w3.eth.get_block(block_number)
            one_eth_to_usd_price = decimal.Decimal(
                float(get_one_eth_to_usd(block_data["timestamp"], prices))
            )

            if profit >= 0:
                profit_usd = Web3.from_wei(profit, "ether") * one_eth_to_usd_price
                print(
                    colors.OK
                    + "Profit: "
                    + str(Web3.from_wei(profit, "ether"))
                    + " ETH ("
                    + str(profit_usd)
                    + " USD)"
                    + colors.END
                )
            else:
                profit_usd = -Web3.from_wei(abs(profit), "ether") * one_eth_to_usd_price
                print(
                    colors.FAIL
                    + "Profit: -"
                    + str(Web3.from_wei(abs(profit), "ether"))
                    + " ETH ("
                    + str(profit_usd)
                    + " USD)"
                    + colors.END
                )

            # Decide if via a "bot" or "exchange" directly
            interface = "bot"
            if tx1["to"] == whale_tx["to"] == tx2["to"] or (
                _to_a1 == tx1["from"] and _from_a2 == tx2["from"]
            ):
                interface = "exchange"

            bot_address = None
            if interface == "bot" and _from_a2 == _to_a1:
                bot_address = _to_a1

            same_sender = tx1["from"] == tx2["from"]
            same_receiver = tx1["to"] == tx2["to"]
            same_token_amount = _value_a1 == _value_a2

            insert_finding_into_db(
                block_number=block_number,
                block_timestamp=block_data["timestamp"],
                tx1=tx1,
                whale_tx=whale_tx,
                tx2=tx2,
                one_eth_to_usd_price=one_eth_to_usd_price,
                total_cost=total_cost,
                gain=gain,
                profit=profit,
                exchange_address=exchange_address,
                exchange_name=exchange_name,
                token_address=token_address,
                token_name=token_name,
                eth_spent=eth_spent,
                eth_whale=eth_whale,
                eth_received=eth_received,
                _value_a1=_value_a1,
                _value_w=_value_w,
                _value_a2=_value_a2,
                interface=interface,
                bot_address=bot_address,
                same_sender=same_sender,
                same_receiver=same_receiver,
                same_token_amount=same_token_amount,
            )


def process_event_pair_for_sandwich(
    event_a1, event_a2, asset_transfers_map, attackers_set, whales_set, events
):
    """
    Given a pair of events (A1, A2), check if they qualify for a sandwich detection:
    1) Validate token amounts (A1 vs A2).
    2) Identify a Whale event in between them.
    3) If found, handle the detected sandwich.
    """
    _from_a1, _to_a1, _value_a1 = parse_addresses_and_values(event_a1)
    _from_a2, _to_a2, _value_a2 = parse_addresses_and_values(event_a2)

    delta = abs(_value_a2 - _value_a1) / max(_value_a2, _value_a1)
    if delta <= TOKEN_AMOUNT_DELTA:
        token_address = event_a1["address"]
        # Whale event must be found among asset_transfers_map[token_address]
        if token_address not in asset_transfers_map:
            asset_transfers_map[token_address] = []

        whale_event = look_for_whale_event(
            asset_transfers_map[token_address], event_a1, event_a2, attackers_set
        )
        if whale_event:
            # If found, confirm it meets the typical conditions
            handle_detected_sandwich(
                event_a1,
                event_a2,
                whale_event,
                event_a1["blockNumber"],
                events,
                None,
                asset_transfers_map,
                whales_set,
                attackers_set,
            )


##############################################################################
# Main Analysis Logic
##############################################################################
def analyze_block(block_number):
    """
    Main function to analyze a single block for insertion-based frontrunning.
    1. Checks if the block is already analyzed (to avoid duplication).
    2. Fetches all ERC-20 'Transfer' events for that block.
    3. Iterates over those events, storing them in data structures.
    4. Identifies insertion (sandwich) attacks and logs them to MongoDB.
    """
    start_time = time.time()
    print("Analyzing block number:", block_number)

    # Check if block has been analyzed
    status_record = is_block_analyzed(block_number)
    if status_record:
        print(f"Block {block_number} already analyzed!")
        return status_record["execution_time"]

    # Fetch events
    events = fetch_transfer_events(block_number)

    whales = set()  # store whale transaction hashes
    attackers = set()  # store attacker transaction hashes
    transfer_to = {}  # (token + 'to') -> last event
    asset_transfers = {}  # token -> list of events

    for event in events:
        # Ignore Wrapped ETH and Bancor ETH token transfers
        if event["address"].lower() not in [
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "0xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315",
        ]:
            parsed = parse_transfer_event(event)
            if not parsed:
                continue
            _from, _to, _value = parsed

            # Ignore zero-value transfers or self-transfers
            if _value <= 0 or _from == _to:
                continue

            # Check if there's a previously seen event (A1) that might form a sandwich with current event (A2).
            # We look up the map by (token + from) to find "attacker's first transaction".
            stored_key = event["address"] + _from
            if (
                stored_key in transfer_to
                and transfer_to[stored_key]["transactionIndex"] + 1
                < event["transactionIndex"]
            ):
                # Potential second attacker transaction found
                event_a1 = transfer_to[stored_key]
                event_a2 = event
                process_event_pair_for_sandwich(
                    event_a1, event_a2, asset_transfers, attackers, whales, events
                )

            # Update transfer_to map and asset_transfers
            transfer_to[event["address"] + _to] = event
            if event["address"] not in asset_transfers:
                asset_transfers[event["address"]] = []
            asset_transfers[event["address"]].append(event)

    # Done with all events. Store block status
    execution_time = time.time() - start_time
    store_block_status(block_number, execution_time)
    return execution_time


def init_process(_prices):
    """
    Initialize web3 connection and global references in each multiprocessing worker.
    This is called once per worker process to set up shared resources.
    """
    global w3
    global prices
    global mongo_connection

    w3 = Web3(LegacyWebSocketProvider(WEB3_WS_PROVIDER))
    if w3.provider.is_connected:
        print("Connected worker to", w3.client_version)
    else:
        print(
            colors.FAIL + f"Error: Could not connect to {WEB3_WS_PROVIDER}" + colors.END
        )

    prices = _prices
    mongo_connection = pymongo.MongoClient(
        f"mongodb://{MONGO_HOST}:{MONGO_PORT}", maxPoolSize=None
    )


def main():
    """
    Orchestrates multiprocessing across a range of Ethereum blocks to detect insertion frontrunning attacks.
    Usage:
        python3 insertion.py <BLOCK_RANGE_START>:<BLOCK_RANGE_END>
    """
    if len(sys.argv) != 2 or sys.argv[1] in {"-h", "--help"}:
        print(__doc__.strip())
        sys.exit(-1)

    if ":" not in sys.argv[1]:
        print(
            colors.FAIL
            + f"Error: Please provide a valid block range: 'python3 {sys.argv[0]} <START_BLOCK>:<END_BLOCK>'"
            + colors.END
        )
        sys.exit(-2)

    block_range_str = sys.argv[1].split(":")
    if len(block_range_str) != 2 or not all(
        item.isnumeric() for item in block_range_str
    ):
        print(
            colors.FAIL
            + f"Error: Please provide integer block range: 'python3 {sys.argv[0]} <START_BLOCK>:<END_BLOCK>'"
            + colors.END
        )
        sys.exit(-3)

    block_range_start = int(block_range_str[0])
    block_range_end = int(block_range_str[1])

    # Retrieve historical ETH prices
    prices_data = get_prices()

    # Set up multiprocessing
    multiprocessing.set_start_method("fork")
    print(
        f"Running detection of insertion frontrunning attacks with {multiprocessing.cpu_count()} CPUs"
    )
    print("Initializing workers...")

    # Create pool and map each block in the range to analyze_block
    with multiprocessing.Pool(
        processes=2,  # Could also use multiprocessing.cpu_count()
        initializer=init_process,
        initargs=(prices_data,),
    ) as pool:
        start_total = time.time()
        execution_times = pool.map(
            analyze_block, range(block_range_start, block_range_end + 1)
        )
        end_total = time.time()

        print("Total execution time:", end_total - start_total)
        if execution_times:
            print("Max execution time:", numpy.max(execution_times))
            print("Mean execution time:", numpy.mean(execution_times))
            print("Median execution time:", numpy.median(execution_times))
            print("Min execution time:", numpy.min(execution_times))


if __name__ == "__main__":
    main()
