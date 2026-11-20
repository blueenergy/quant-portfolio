"""Hold-band construction (``top_n_buffer``).

The buffer is the only change in this strategy that touches *which* names get
bought without touching the ranking, so the tests pin both halves: held names
survive inside the band, and nothing outside the plain top-N ranking can enter
because of the buffer.
"""

from __future__ import annotations

import pandas as pd
import pytest

from portfolio.construction import select_portfolio, select_trade_plan_portfolio


def _frame(scores: dict[str, float]) -> pd.DataFrame:
    """Ranked cross-section; higher score ranks first."""
    rows = [
        {
            "symbol": symbol,
            "score_value": score,
            "industry": "A" if symbol.startswith("A") else "B",
        }
        for symbol, score in scores.items()
    ]
    return pd.DataFrame(rows).sort_values("score_value", ascending=False).reset_index(drop=True)


def _prices(symbols) -> dict[str, float]:
    return {symbol: 10.0 for symbol in symbols}


def _config(**overrides) -> dict:
    config = {
        "top_n": 3,
        "construction_mode": "top_n_buffer",
        "holding_buffer_n": 5,
        "lot_size": 100,
        "cash_buffer": 0.0,
        "candidate_pool_multiplier": 3,
        "min_buy_lots": 1,
    }
    config.update(overrides)
    return config


def _scores() -> dict[str, float]:
    # 10 names, descending scores S01 > S02 > ... > S10
    return {f"S{i:02d}": float(100 - i) for i in range(1, 11)}


def test_buffer_keeps_a_held_name_that_is_still_ranked():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)

    plain = select_trade_plan_portfolio(frame, {**_config(), "construction_mode": "top_n"}, prices, 10_000)
    buffered = select_trade_plan_portfolio(frame, _config(), prices, 10_000, held_symbols=["S04"])

    assert list(plain["symbol"])[:3] == ["S01", "S02", "S03"]
    assert "S04" in set(buffered["symbol"])
    # The buffer displaces the worst-ranked new name, not the best ones, and
    # the result stays in rank order.
    assert list(buffered["symbol"]) == ["S01", "S02", "S04"]


def test_buffer_drops_a_held_name_outside_the_band():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)

    # S08 ranks 8th, outside a 5-name band.
    buffered = select_trade_plan_portfolio(
        frame, _config(), prices, 10_000, held_symbols=["S08"]
    )
    assert "S08" not in set(buffered["symbol"])
    assert set(buffered["symbol"]) == {"S01", "S02", "S03"}


def test_book_is_held_in_band_first_then_ranked_fill():
    """Characterization of the buffer contract, order included.

    Held names inside the band occupy slots first (in rank order), then the
    ranked list fills the rest. A held name ranked 5th therefore keeps a slot
    in a 3-name book with a 5-name band -- that survival *is* the buffer. What
    can never happen is a name outside the band, or outside the candidate pool,
    entering the book.
    """
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    ranked = list(frame["symbol"])
    top_n, hold_n = 3, 5
    band = set(ranked[:hold_n])

    for held in (["S02"], ["S01", "S02", "S03"], ["S01", "S02", "S05"], ["S04", "S05", "S06"]):
        buffered = select_trade_plan_portfolio(
            frame, _config(), prices, 10_000, held_symbols=held
        )
        held_set = set(held)
        kept = [s for s in ranked if s in held_set and s in band]
        rest = [s for s in ranked if s not in set(kept)]
        chosen = set((kept + rest)[:top_n])
        # The book is reported in rank order regardless of which slots the
        # buffer filled first.
        expected = [s for s in ranked if s in chosen]
        assert list(buffered["symbol"]) == expected, held
        # Nothing outside the band can enter through the buffer.
        assert set(buffered["symbol"]) <= set(ranked[: max(top_n, hold_n)])


def test_buffer_does_not_resurrect_an_excluded_symbol():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    buffered = select_trade_plan_portfolio(
        frame, _config(), prices, 10_000, excluded_symbols=["S02"], held_symbols=["S02"]
    )
    assert "S02" not in set(buffered["symbol"])


def test_buffer_does_not_resurrect_an_unbuyable_symbol():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    prices.pop("S02")
    buffered = select_trade_plan_portfolio(frame, _config(), prices, 10_000, held_symbols=["S02"])
    assert "S02" not in set(buffered["symbol"])


def test_a_band_narrower_than_the_book_means_no_buffer():
    """``holding_buffer_n < top_n`` must not become a stricter rule."""
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    config = _config(top_n=3, holding_buffer_n=2)
    buffered = select_trade_plan_portfolio(frame, config, prices, 10_000, held_symbols=["S09"])
    plain = select_trade_plan_portfolio(frame, {**config, "construction_mode": "top_n"}, prices, 10_000)
    assert set(buffered["symbol"]) == set(plain["symbol"])


def test_window_band_three_gives_window_band_five_shape():
    """The band is the search space for held names, not the book size.

    With a 3-name book and a 5-name band, a held name ranked 4th or 5th
    survives; one ranked 6th does not.
    """
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    for held, survives in (("S04", True), ("S05", True), ("S06", False)):
        selected = select_trade_plan_portfolio(frame, _config(), prices, 10_000, held_symbols=[held])
        assert (held in set(selected["symbol"])) is survives, held


def test_buffer_output_stays_rank_ordered():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    selected = select_trade_plan_portfolio(
        frame, _config(), prices, 10_000, held_symbols=["S01", "S03", "S05"]
    )
    values = list(selected["score_value"])
    assert values == sorted(values, reverse=True)


def test_buffer_is_a_no_op_without_holdings():
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    buffered = select_trade_plan_portfolio(frame, _config(), prices, 10_000)
    plain = select_trade_plan_portfolio(frame, {**_config(), "construction_mode": "top_n"}, prices, 10_000)
    assert list(buffered["symbol"]) == list(plain["symbol"])


def test_candidate_pool_treats_buffer_as_plain_top_n():
    """The pool slice must not reorder; buffer applies to the final book."""
    scores = _scores()
    frame = _frame(scores)
    plain = select_portfolio(frame, {"top_n": 5, "construction_mode": "top_n"})
    buffered = select_portfolio(frame, {"top_n": 5, "construction_mode": "top_n_buffer"})
    assert list(buffered["symbol"]) == list(plain["symbol"])


def test_buffer_with_a_perfectly_stable_book_holds_everything():
    """A book whose names all stay in band is unchanged period over period."""
    scores = _scores()
    frame = _frame(scores)
    prices = _prices(scores)
    first = select_trade_plan_portfolio(frame, _config(), prices, 10_000, held_symbols=[])
    held = list(first["symbol"])
    second = select_trade_plan_portfolio(frame, _config(), prices, 10_000, held_symbols=held)
    assert list(second["symbol"]) == held
