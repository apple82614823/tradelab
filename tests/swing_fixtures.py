from __future__ import annotations

from decimal import Decimal


INTERVAL_MS = 15 * 60 * 1000


def _d(value) -> Decimal:
    return Decimal(str(value))


def build_valid_swing_klines(
    pullback_bars: int = 5,
    second_up_bars: int = 7,
    h2_pullback_bars: int = 6,
    stable_bars: int = 5,
    current_in_zone: bool = True,
    p1_price: Decimal = Decimal("140"),
) -> list[list[str | int]]:
    h1_index = 37
    p1_index = h1_index + pullback_bars
    second_break_index = p1_index + second_up_bars
    h2_index = second_break_index + 5
    p2_index = h2_index + h2_pullback_bars
    stable_end_index = p2_index + stable_bars
    current_index = stable_end_index + 1
    h1_close = Decimal("159.8")
    p1_close = p1_price + (h1_close - p1_price) * Decimal("0.2")
    zone_upper = p1_price * Decimal("1.015")
    p2_low = zone_upper + Decimal("0.4")
    p2_close = p2_low + Decimal("0.5")
    controls = [
        (0, "156"),
        (2, "158"),
        (7, "142"),
        (12, "152"),
        (17, "136"),
        (22, "146"),
        (27, "130"),
        (h1_index, str(h1_close)),
        (p1_index, str(p1_close)),
        (second_break_index, "161"),
        (h2_index, "168"),
        (p2_index, str(p2_close)),
    ]
    closes: dict[int, Decimal] = {}
    noise = (Decimal("0"), Decimal("0.35"), Decimal("-0.20"), Decimal("0.25"), Decimal("-0.15"))
    for (start, start_value), (end, end_value) in zip(controls, controls[1:]):
        first = _d(start_value)
        last = _d(end_value)
        for index in range(start, end + 1):
            if index == start:
                value = first
            elif index == end:
                value = last
            else:
                fraction = Decimal(index - start) / Decimal(end - start)
                value = first + (last - first) * fraction + noise[index % len(noise)]
            closes[index] = value
    stable_offsets = ("1.0", "1.6", "1.3", "2.0", "2.5")
    for offset in range(1, stable_bars + 1):
        closes[p2_index + offset] = p2_low + _d(
            stable_offsets[(offset - 1) % len(stable_offsets)]
        )
    closes[current_index] = (
        p1_price * Decimal("1.012") if current_in_zone else p2_low + Decimal("3")
    )

    pivot_highs = {2: Decimal("160"), 12: Decimal("154"), 22: Decimal("148"), h1_index: Decimal("160"), h2_index: Decimal("170")}
    pivot_lows = {
        7: Decimal("140"),
        17: Decimal("134"),
        27: Decimal("128"),
        p1_index: p1_price,
        p2_index: p2_low,
    }
    rows: list[list[str | int]] = []
    previous_close = closes[0] - Decimal("0.2")
    for index in range(current_index + 1):
        close = closes[index]
        open_price = previous_close + (Decimal("0.18") if index % 3 == 0 else Decimal("-0.12"))
        high = max(open_price, close) + Decimal("0.6")
        low = min(open_price, close) - Decimal("0.6")
        if index in pivot_highs:
            high = pivot_highs[index]
        if h1_index < index <= h1_index + 2:
            high = min(high, Decimal("159.9"))
        if index in pivot_lows:
            low = pivot_lows[index]
        if p2_index < index <= stable_end_index:
            low = max(low, p2_low + Decimal("0.2"))
        if index == current_index:
            if current_in_zone:
                close = p1_price * Decimal("1.012")
                open_price = close - Decimal("0.18")
                high = close + Decimal("0.32")
                low = p1_price * Decimal("1.01")
            else:
                open_price = Decimal("146.5")
                close = Decimal("147")
                high = Decimal("147.5")
                low = Decimal("146")
        rows.append(
            [
                index * INTERVAL_MS,
                str(open_price),
                str(high),
                str(low),
                str(close),
                "0",
                index * INTERVAL_MS + INTERVAL_MS - 1,
            ]
        )
        previous_close = close
    return rows


def build_beatusdt_continued_rise() -> list[list[str | int]]:
    raw = build_valid_swing_klines()
    h1_index = 37
    p1_index = 38
    current_index = 57
    raw = raw[: current_index + 1]
    raw[p1_index][1:5] = ["157", "158", "150", "152"]
    for index in range(39, current_index + 1):
        close = Decimal("152") + Decimal(index - 38) * Decimal("0.67")
        open_price = close - Decimal("0.4")
        raw[index][1:5] = [
            str(open_price),
            str(close + Decimal("0.7")),
            str(open_price - Decimal("0.5")),
            str(close),
        ]
    base_open_ms = 11 * 60 * 60 * 1000 - h1_index * INTERVAL_MS
    for index, row in enumerate(raw):
        row[0] = base_open_ms + index * INTERVAL_MS
        row[6] = row[0] + INTERVAL_MS - 1
    return raw
