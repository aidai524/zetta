from zetta.polycop_wallets import build_polycop_wallet_signal_result, score_polycop_wallets


def test_score_polycop_wallets_segments_stable_and_burst() -> None:
    rows = [
        {
            "address": "0x1111111111111111111111111111111111111111",
            "userName": "steady",
            "score": 90,
            "actualTotalPnl": 50000,
            "backtestTotalPnl": 48000,
            "recent20Pnl": 5000,
            "recent20BacktestPnl": 4500,
            "winRate": 62,
            "recent20WinRate": 65,
            "avgProfitLossRatio": 2.2,
            "avgMarketRoi": 0.2,
            "avgMarketProfitRate": 0.1,
            "slippageCostRate": 4,
            "recent20SlippageCostRate": 3,
            "totalMarkets": 120,
            "hedgedMarkets": 5,
        },
        {
            "address": "0x2222222222222222222222222222222222222222",
            "userName": "burst",
            "score": 80,
            "actualTotalPnl": 30000,
            "backtestTotalPnl": 29000,
            "recent20Pnl": 24000,
            "recent20BacktestPnl": 23000,
            "winRate": 55,
            "recent20WinRate": 70,
            "avgProfitLossRatio": 3,
            "slippageCostRate": 5,
            "totalMarkets": 15,
            "hedgedMarkets": 0,
        },
    ]

    wallets = score_polycop_wallets(rows)
    by_name = {wallet["user_name"]: wallet for wallet in wallets}

    assert "stable" in by_name["steady"]["segments"]
    assert "burst" in by_name["burst"]["segments"]
    assert by_name["steady"]["metrics"]["total_markets"] == 120


def test_build_polycop_wallet_signal_result_contains_summary_segments() -> None:
    rows = [
        {
            "address": "0x1111111111111111111111111111111111111111",
            "userName": "steady",
            "score": 90,
            "actualTotalPnl": 50000,
            "backtestTotalPnl": 48000,
            "recent20Pnl": 5000,
            "winRate": 62,
            "recent20WinRate": 65,
            "avgProfitLossRatio": 2.2,
            "slippageCostRate": 4,
            "totalMarkets": 120,
            "hedgedMarkets": 5,
        }
    ]

    result = build_polycop_wallet_signal_result(
        rows,
        metadata={"page_size": 50, "max_pages": 1, "pages_fetched": 1},
        limit=10,
    )

    assert result["status"] == "ok"
    assert result["summary"]["wallet_count"] == 1
    assert result["summary"]["stable_count"] == 1
    assert result["detail"]["segments"]["stable"][0]["user_name"] == "steady"
