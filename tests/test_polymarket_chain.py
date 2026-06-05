from zetta.chain.polymarket import (
    ORDER_FILLED_TOPIC,
    balance_movement_rows,
    data_words,
    decode_uint_arrays,
    decode_fee_charged,
    decode_split_merge_data,
    decode_order_filled,
    decode_orders_matched,
    exchange_fill_row,
    fee_charged_row,
    fill_price_size_notional,
    lifecycle_event_row,
    orders_matched_row,
    side_name,
    topic_address,
)


REAL_ORDER_FILLED_LOG = {
    "chain_id": 137,
    "block_number": 87909517,
    "transaction_hash": "0x03beb7ad6347d3d73cd47eb7b620869786f659e0f995f9ff4c70b43987da9da1",
    "log_index": 977,
    "topics_json": (
        '["0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",'
        '"0x38ab5357e3d348f979b2e8a9fba721a8762437eb25c64278a41845d16e8f2a94",'
        '"0x00000000000000000000000086a29f88fcc23ea2b0e01b4e186b043e26c873a8",'
        '"0x0000000000000000000000004c818c1965c5e544a8046ccea1d9d5052cea4e45"]'
    ),
    "data": (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000001"
        "be01ed2dbe761b31af76b05e195bbcb779f149f076710c4ea0ce184b453404a"
        "00000000000000000000000000000000000000000000000000000000004490c2"
        "0000000000000000000000000000000000000000000000000000000000631541"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
    ),
}

REAL_ORDERS_MATCHED_LOG = {
    "chain_id": 137,
    "block_number": 87909517,
    "transaction_hash": "0x03beb7ad6347d3d73cd47eb7b620869786f659e0f995f9ff4c70b43987da9da1",
    "log_index": 982,
    "topics_json": (
        '["0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c",'
        '"0x78ba01729b1d3a3b5d6542237bad82ceddbedc7a5604fd47a3f6d40518371c39",'
        '"0x0000000000000000000000004c818c1965c5e544a8046ccea1d9d5052cea4e45"]'
    ),
    "data": (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000001"
        "bc682dbcd847ec49bfd347ee51526cd4f8226b65f539867506d9d5ba79957c10"
        "00000000000000000000000000000000000000000000000000000000001e847f"
        "0000000000000000000000000000000000000000000000000000000000631541"
    ),
}

REAL_FEE_CHARGED_LOG = {
    "chain_id": 137,
    "block_number": 87909517,
    "transaction_hash": "0x03beb7ad6347d3d73cd47eb7b620869786f659e0f995f9ff4c70b43987da9da1",
    "log_index": 978,
    "topics_json": (
        '["0x55bb3cade9d43b798a4fe5ffdd05024b2d7870df53920673bfc7e68047cd0ab1",'
        '"0x000000000000000000000000115f48dc2a731aa16251c6d6e1befc42f92accc9"]'
    ),
    "data": "0x0000000000000000000000000000000000000000000000000000000000017a66",
}

REAL_TRANSFER_SINGLE_LOG = {
    "chain_id": 137,
    "block_number": 87909517,
    "transaction_hash": "0x03beb7ad6347d3d73cd47eb7b620869786f659e0f995f9ff4c70b43987da9da1",
    "log_index": 976,
    "topics_json": (
        '["0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",'
        '"0x000000000000000000000000e111180000d2663c0091e4f400237545b87b996b",'
        '"0x000000000000000000000000e111180000d2663c0091e4f400237545b87b996b",'
        '"0x00000000000000000000000086a29f88fcc23ea2b0e01b4e186b043e26c873a8"]'
    ),
    "data": (
        "0x"
        "1be01ed2dbe761b31af76b05e195bbcb779f149f076710c4ea0ce184b453404a"
        "0000000000000000000000000000000000000000000000000000000000631541"
    ),
}

REAL_TRANSFER_BATCH_LOG = {
    "chain_id": 137,
    "block_number": 87909517,
    "transaction_hash": "0x03beb7ad6347d3d73cd47eb7b620869786f659e0f995f9ff4c70b43987da9da1",
    "log_index": 973,
    "topics_json": (
        '["0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb",'
        '"0x000000000000000000000000ada100874d00e3331d00f2007a9c336a65009718",'
        '"0x0000000000000000000000000000000000000000000000000000000000000000",'
        '"0x000000000000000000000000ada100874d00e3331d00f2007a9c336a65009718"]'
    ),
    "data": (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000040"
        "00000000000000000000000000000000000000000000000000000000000000a0"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "1bc682dbcd847ec49bfd347ee51526cd4f8226b65f539867506d9d5ba79957c11"
        "be01ed2dbe761b31af76b05e195bbcb779f149f076710c4ea0ce184b453404a"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "0000000000000000000000000000000000000000000000000000000000631541"
        "0000000000000000000000000000000000000000000000000000000000631541"
    ),
}

SYNTHETIC_POSITION_SPLIT_LOG = {
    "chain_id": 137,
    "block_number": 100,
    "transaction_hash": "0xtx",
    "log_index": 1,
    "topics_json": (
        '["0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298",'
        '"0x0000000000000000000000001111111111111111111111111111111111111111",'
        '"0x0000000000000000000000002222222222222222222222222222222222222222",'
        '"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]'
    ),
    "data": (
        "0x"
        "0000000000000000000000002222222222222222222222222222222222222222"
        "0000000000000000000000000000000000000000000000000000000000000060"
        "00000000000000000000000000000000000000000000000000000000000f4240"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "0000000000000000000000000000000000000000000000000000000000000001"
        "0000000000000000000000000000000000000000000000000000000000000002"
    ),
}


def test_order_filled_topic_constant_matches_source_signature() -> None:
    assert ORDER_FILLED_TOPIC == "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def test_decode_order_filled_real_log_sample() -> None:
    decoded = decode_order_filled(REAL_ORDER_FILLED_LOG)

    assert decoded is not None
    assert decoded.side == "SELL"
    assert decoded.maker == "0x86a29f88fcc23ea2b0e01b4e186b043e26c873a8"
    assert decoded.taker == "0x4c818c1965c5e544a8046ccea1d9d5052cea4e45"
    assert decoded.maker_amount == 71896096
    assert decoded.taker_amount == 103896080


def test_exchange_fill_row_derives_price_size_and_notional() -> None:
    row = exchange_fill_row(REAL_ORDER_FILLED_LOG)

    assert row["token_id"].startswith("859")
    assert row["side"] == "SELL"
    assert row["size"] == 71.896096
    assert row["notional"] == 103.89608
    assert round(row["price"], 4) == 1.4451


def test_decode_orders_matched_real_log_sample() -> None:
    decoded = decode_orders_matched(REAL_ORDERS_MATCHED_LOG)

    assert decoded is not None
    assert decoded.side == "SELL"
    assert decoded.taker_order_maker == "0x4c818c1965c5e544a8046ccea1d9d5052cea4e45"
    assert decoded.maker_amount == 1999999
    assert decoded.taker_amount == 6493505

    row = orders_matched_row(REAL_ORDERS_MATCHED_LOG)
    assert row["size"] == 1.999999
    assert row["notional"] == 6.493505


def test_decode_fee_charged_real_log_sample() -> None:
    decoded = decode_fee_charged(REAL_FEE_CHARGED_LOG)

    assert decoded is not None
    assert decoded.receiver == "0x115f48dc2a731aa16251c6d6e1befc42f92accc9"
    assert decoded.amount == 96870

    row = fee_charged_row(REAL_FEE_CHARGED_LOG)
    assert row["amount"] == 0.09687


def test_balance_movement_rows_decode_transfer_single_and_batch_samples() -> None:
    single = balance_movement_rows(REAL_TRANSFER_SINGLE_LOG)
    batch = balance_movement_rows(REAL_TRANSFER_BATCH_LOG)

    assert len(single) == 1
    assert single[0]["transfer_type"] == "single"
    assert single[0]["from_address"] == "0xe111180000d2663c0091e4f400237545b87b996b"
    assert single[0]["to_address"] == "0x86a29f88fcc23ea2b0e01b4e186b043e26c873a8"
    assert single[0]["amount"] == 6.493505

    assert len(batch) == 2
    assert [row["transfer_type"] for row in batch] == ["batch", "batch"]
    assert [row["amount"] for row in batch] == [6.493505, 6.493505]


def test_decode_uint_arrays_reads_abi_dynamic_arrays() -> None:
    ids, values = decode_uint_arrays(REAL_TRANSFER_BATCH_LOG["data"])

    assert len(ids) == 2
    assert values == [6493505, 6493505]


def test_lifecycle_event_row_decodes_synthetic_split() -> None:
    collateral_token, partition, amount = decode_split_merge_data(SYNTHETIC_POSITION_SPLIT_LOG["data"])
    row = lifecycle_event_row(SYNTHETIC_POSITION_SPLIT_LOG)

    assert collateral_token == "0x2222222222222222222222222222222222222222"
    assert partition == [1, 2]
    assert amount == 1_000_000
    assert row["event_type"] == "split"
    assert row["stakeholder"] == "0x1111111111111111111111111111111111111111"
    assert row["collateral_token"] == "0x2222222222222222222222222222222222222222"
    assert row["condition_id"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert row["amount"] == 1.0


def test_small_chain_decode_helpers() -> None:
    assert len(data_words("0x" + "0" * 128)) == 2
    assert topic_address("0x" + "0" * 24 + "a" * 40) == "0x" + "a" * 40
    assert side_name(0) == "BUY"
    assert side_name(1) == "SELL"
