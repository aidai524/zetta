from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
ORDERS_MATCHED_TOPIC = "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c"
FEE_CHARGED_TOPIC = "0x55bb3cade9d43b798a4fe5ffdd05024b2d7870df53920673bfc7e68047cd0ab1"
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
POSITION_SPLIT_TOPIC = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
POSITIONS_MERGE_TOPIC = "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca"
PAYOUT_REDEMPTION_TOPIC = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
USDC_DECIMALS = 1_000_000


@dataclass(frozen=True)
class DecodedOrderFilled:
    order_hash: str
    maker: str
    taker: str
    side: str
    token_id: str
    maker_amount: int
    taker_amount: int
    fee: int
    builder: str
    metadata: str


@dataclass(frozen=True)
class DecodedOrdersMatched:
    taker_order_hash: str
    taker_order_maker: str
    side: str
    token_id: str
    maker_amount: int
    taker_amount: int


@dataclass(frozen=True)
class DecodedFeeCharged:
    receiver: str
    amount: int


def decode_order_filled(log: dict[str, Any]) -> DecodedOrderFilled | None:
    topics = parse_topics(log.get("topics_json") or log.get("topics"))
    if len(topics) < 4 or str(topics[0]).lower() != ORDER_FILLED_TOPIC:
        return None
    words = data_words(str(log.get("data") or ""))
    if len(words) < 7:
        return None
    side_value = int(words[0], 16)
    return DecodedOrderFilled(
        order_hash=str(topics[1]),
        maker=topic_address(str(topics[2])),
        taker=topic_address(str(topics[3])),
        side=side_name(side_value),
        token_id=str(int(words[1], 16)),
        maker_amount=int(words[2], 16),
        taker_amount=int(words[3], 16),
        fee=int(words[4], 16),
        builder=f"0x{words[5]}",
        metadata=f"0x{words[6]}",
    )


def decode_orders_matched(log: dict[str, Any]) -> DecodedOrdersMatched | None:
    topics = parse_topics(log.get("topics_json") or log.get("topics"))
    if len(topics) < 3 or str(topics[0]).lower() != ORDERS_MATCHED_TOPIC:
        return None
    words = data_words(str(log.get("data") or ""))
    if len(words) < 4:
        return None
    side_value = int(words[0], 16)
    return DecodedOrdersMatched(
        taker_order_hash=str(topics[1]),
        taker_order_maker=topic_address(str(topics[2])),
        side=side_name(side_value),
        token_id=str(int(words[1], 16)),
        maker_amount=int(words[2], 16),
        taker_amount=int(words[3], 16),
    )


def decode_fee_charged(log: dict[str, Any]) -> DecodedFeeCharged | None:
    topics = parse_topics(log.get("topics_json") or log.get("topics"))
    if len(topics) < 2 or str(topics[0]).lower() != FEE_CHARGED_TOPIC:
        return None
    words = data_words(str(log.get("data") or ""))
    if not words:
        return None
    return DecodedFeeCharged(
        receiver=topic_address(str(topics[1])),
        amount=int(words[0], 16),
    )


def exchange_fill_row(log: dict[str, Any], *, ingested_at: datetime | None = None) -> dict[str, Any] | None:
    decoded = decode_order_filled(log)
    if decoded is None:
        return None
    price, size, notional = fill_price_size_notional(decoded)
    return {
        "chain_id": int(log.get("chain_id") or 137),
        "block_number": int(log.get("block_number") or 0),
        "transaction_hash": str(log.get("transaction_hash") or ""),
        "log_index": int(log.get("log_index") or 0),
        "market_id": "",
        "condition_id": "",
        "token_id": decoded.token_id,
        "maker": decoded.maker,
        "taker": decoded.taker,
        "side": decoded.side,
        "price": price,
        "size": size,
        "notional": notional,
        "raw_json": json.dumps(
            {
                "log": log,
                "decoded": decoded.__dict__,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "ingested_at": ingested_at or datetime.now(UTC),
    }


def orders_matched_row(
    log: dict[str, Any], *, ingested_at: datetime | None = None
) -> dict[str, Any] | None:
    decoded = decode_orders_matched(log)
    if decoded is None:
        return None
    price, size, notional = fill_price_size_notional(decoded)
    return {
        "chain_id": int(log.get("chain_id") or 137),
        "block_number": int(log.get("block_number") or 0),
        "transaction_hash": str(log.get("transaction_hash") or ""),
        "log_index": int(log.get("log_index") or 0),
        "taker_order_hash": decoded.taker_order_hash,
        "taker_order_maker": decoded.taker_order_maker,
        "side": decoded.side,
        "token_id": decoded.token_id,
        "maker_amount": decoded.maker_amount / USDC_DECIMALS,
        "taker_amount": decoded.taker_amount / USDC_DECIMALS,
        "price": price,
        "size": size,
        "notional": notional,
        "raw_json": json.dumps(
            {
                "log": log,
                "decoded": decoded.__dict__,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "ingested_at": ingested_at or datetime.now(UTC),
    }


def fee_charged_row(
    log: dict[str, Any], *, ingested_at: datetime | None = None
) -> dict[str, Any] | None:
    decoded = decode_fee_charged(log)
    if decoded is None:
        return None
    return {
        "chain_id": int(log.get("chain_id") or 137),
        "block_number": int(log.get("block_number") or 0),
        "transaction_hash": str(log.get("transaction_hash") or ""),
        "log_index": int(log.get("log_index") or 0),
        "receiver": decoded.receiver,
        "amount": decoded.amount / USDC_DECIMALS,
        "raw_json": json.dumps(
            {
                "log": log,
                "decoded": decoded.__dict__,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "ingested_at": ingested_at or datetime.now(UTC),
    }


def balance_movement_rows(
    log: dict[str, Any], *, ingested_at: datetime | None = None
) -> list[dict[str, Any]]:
    topics = parse_topics(log.get("topics_json") or log.get("topics"))
    if len(topics) < 4:
        return []
    topic0 = str(topics[0]).lower()
    if topic0 == TRANSFER_SINGLE_TOPIC:
        words = data_words(str(log.get("data") or ""))
        if len(words) < 2:
            return []
        return [
            balance_movement_row(
                log,
                operator=topic_address(str(topics[1])),
                from_address=topic_address(str(topics[2])),
                to_address=topic_address(str(topics[3])),
                token_id=str(int(words[0], 16)),
                amount=int(words[1], 16) / USDC_DECIMALS,
                transfer_type="single",
                batch_index=0,
                ingested_at=ingested_at,
            )
        ]
    if topic0 == TRANSFER_BATCH_TOPIC:
        ids, values = decode_uint_arrays(str(log.get("data") or ""))
        rows = []
        for index, token_id in enumerate(ids):
            amount = values[index] if index < len(values) else 0
            rows.append(
                balance_movement_row(
                    log,
                    operator=topic_address(str(topics[1])),
                    from_address=topic_address(str(topics[2])),
                    to_address=topic_address(str(topics[3])),
                    token_id=str(token_id),
                    amount=amount / USDC_DECIMALS,
                    transfer_type="batch",
                    batch_index=index,
                    ingested_at=ingested_at,
                )
            )
        return rows
    return []


def lifecycle_event_row(
    log: dict[str, Any], *, ingested_at: datetime | None = None
) -> dict[str, Any] | None:
    topics = parse_topics(log.get("topics_json") or log.get("topics"))
    if len(topics) < 4:
        return None
    topic0 = str(topics[0]).lower()
    event_type = lifecycle_event_type(topic0)
    if not event_type:
        return None
    collateral_token, condition_id, partition, amount = decode_lifecycle_event(
        event_type=event_type,
        topics=topics,
        data=str(log.get("data") or ""),
    )
    return {
        "chain_id": int(log.get("chain_id") or 137),
        "block_number": int(log.get("block_number") or 0),
        "transaction_hash": str(log.get("transaction_hash") or ""),
        "log_index": int(log.get("log_index") or 0),
        "event_type": event_type,
        "stakeholder": topic_address(str(topics[1])),
        "collateral_token": collateral_token,
        "parent_collection_id": str(topics[3]),
        "condition_id": condition_id,
        "partition_json": json.dumps(partition, separators=(",", ":")),
        "amount": amount / USDC_DECIMALS,
        "raw_json": json.dumps(log, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at or datetime.now(UTC),
    }


def lifecycle_event_type(topic0: str) -> str:
    if topic0 == POSITION_SPLIT_TOPIC:
        return "split"
    if topic0 == POSITIONS_MERGE_TOPIC:
        return "merge"
    if topic0 == PAYOUT_REDEMPTION_TOPIC:
        return "redeem"
    return ""


def decode_lifecycle_event(
    *,
    event_type: str,
    topics: list[str],
    data: str,
) -> tuple[str, str, list[int], int]:
    if event_type in {"split", "merge"}:
        collateral_token, partition, amount = decode_split_merge_data(data)
        condition_id = str(topics[3]) if len(topics) > 3 else ""
        return collateral_token, condition_id, partition, amount
    if event_type == "redeem":
        condition_id, partition, amount = decode_redeem_data(data)
        collateral_token = topic_address(str(topics[2])) if len(topics) > 2 else ""
        return collateral_token, condition_id, partition, amount
    return "", "", [], 0


def decode_split_merge_data(data: str) -> tuple[str, list[int], int]:
    words = data_words(data)
    if len(words) < 3:
        return "", [], 0
    collateral_token = topic_address(words[0])
    partition_offset = int(words[1], 16) // 32
    amount = int(words[2], 16)
    return collateral_token, decode_uint_array(words, partition_offset), amount


def decode_redeem_data(data: str) -> tuple[str, list[int], int]:
    words = data_words(data)
    if len(words) < 3:
        return "", [], 0
    condition_id = f"0x{words[0]}"
    partition_offset = int(words[1], 16) // 32
    amount = int(words[2], 16)
    return condition_id, decode_uint_array(words, partition_offset), amount


def balance_movement_row(
    log: dict[str, Any],
    *,
    operator: str,
    from_address: str,
    to_address: str,
    token_id: str,
    amount: float,
    transfer_type: str,
    batch_index: int,
    ingested_at: datetime | None,
) -> dict[str, Any]:
    return {
        "chain_id": int(log.get("chain_id") or 137),
        "block_number": int(log.get("block_number") or 0),
        "transaction_hash": str(log.get("transaction_hash") or ""),
        "log_index": int(log.get("log_index") or 0),
        "batch_index": batch_index,
        "operator": operator,
        "from_address": from_address,
        "to_address": to_address,
        "token_id": token_id,
        "amount": amount,
        "transfer_type": transfer_type,
        "raw_json": json.dumps(log, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at or datetime.now(UTC),
    }


def fill_price_size_notional(decoded: DecodedOrderFilled) -> tuple[float, float, float]:
    maker = decoded.maker_amount / USDC_DECIMALS
    taker = decoded.taker_amount / USDC_DECIMALS
    if decoded.side == "BUY":
        notional = maker
        size = taker
    else:
        notional = taker
        size = maker
    price = notional / size if size else 0.0
    return price, size, notional


def parse_topics(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).lower() for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parse_topics(parsed)
    return []


def data_words(data: str) -> list[str]:
    cleaned = data[2:] if data.startswith("0x") else data
    if not cleaned:
        return []
    return [cleaned[index : index + 64] for index in range(0, len(cleaned), 64)]


def decode_uint_arrays(data: str) -> tuple[list[int], list[int]]:
    words = data_words(data)
    if len(words) < 2:
        return [], []
    first_offset = int(words[0], 16) // 32
    second_offset = int(words[1], 16) // 32
    return decode_uint_array(words, first_offset), decode_uint_array(words, second_offset)


def decode_uint_array(words: list[str], offset_words: int) -> list[int]:
    if offset_words >= len(words):
        return []
    length = int(words[offset_words], 16)
    start = offset_words + 1
    return [int(word, 16) for word in words[start : start + length]]


def topic_address(topic: str) -> str:
    cleaned = topic[2:] if topic.startswith("0x") else topic
    return f"0x{cleaned[-40:]}".lower()


def side_name(value: int) -> str:
    if value == 0:
        return "BUY"
    if value == 1:
        return "SELL"
    return str(value)
