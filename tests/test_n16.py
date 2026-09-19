from __future__ import annotations

from copy import deepcopy
from contextlib import closing, contextmanager, nullcontext, redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from trading_bot.analyzer import AnalysisResult
from trading_bot.binance_client import BinanceAPIError, BinanceFuturesClient
from trading_bot.main import TradingBot
from trading_bot.monitor import FundingCandidate, StrategyMarketScan
from trading_bot.paper_trader import PaperTrader
import trading_bot.recorder as recorder_module
from trading_bot.n16_analyzer import (
    N16AnalysisResult,
    _APPROVED_N16_CONFIG,
    _atr_series,
    _confirmation_failure_reason,
    _ema_series,
    _latest_skeleton,
    _median,
    _path_efficiency,
    _structure_id,
    analyze_n16_mature_trend_support,
    decode_n16_state_envelope,
    parse_n16_klines,
)
from trading_bot.n16_claim_ledger import (
    N16ClaimLedgerError,
    N16PermanentClaimLedger,
    advance_review_claim_chain,
    review_claim_sha256,
)
from trading_bot.coverage_family_seal import family_seal_catalog_sha256
from trading_bot.recorder import (
    ReviewRecorder,
    StrategySignalBatchWriteResult,
    _N16_EVENTS_TABLE_SQL,
    _N16_INDEX_SQL,
    _N16_LEGACY_PAPER_TABLE_SQL,
    _N16_LEGACY_STRATEGY_STATES_TABLE_SQL,
    _N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL,
    _N16_MIGRATED_TRADE_REVIEWS_XINFO,
    _N16_MIGRATED_STRATEGY_STATES_TABLE_SQL,
    _N16_PRE_LASTCHECK_PAPER_TABLE_SQL,
    _N16_PREINSTALL_EVENTS_TABLE_SQL,
    _N16_SHARED_INDEX_SQL,
    _N16_TRIGGER_SQL,
    _n16_normalized_sql,
    _n16_review_claim_summary,
)
from trading_bot.signal_retention import (
    SignalRetentionMaintenanceError,
    _DatabaseFileScope,
    _capture_database_sidecars,
    _directory_identity,
    _install_n16_claim_boundary,
    _install_n17_lifecycle_boundary,
    _install_n18_lifecycle_boundary,
    _install_n19_lifecycle_boundary,
    _install_n20_lifecycle_boundary,
    _install_coverage_epoch_boundary,
    _install_micro_lifecycle_boundary,
    _regular_file_identity,
    _resolve_n16_publication_boundary,
    _signal_evidence_sha256,
    _table_full_hash,
    apply_signal_retention_maintenance,
    inspect_signal_retention,
    main as signal_retention_main,
    vacuum_signal_database_into,
)
from trading_bot.state import DryRunAccountStore, PositionState, StateStore
from trading_bot.strategies import N16_STRATEGY, load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler, StrategySignalDecision
from trading_bot.trader import (
    EntryWindowExpiredError,
    SyncResult,
    TradePlan,
    Trader,
)
from tests.test_trader import (
    FakeLiveExecutionClient,
    RuleConstrainedClient,
    live_test_config,
    test_config,
)
from tests.recorder_test_utils import make_test_recorder


BASE_TIME_MS = 1_780_000_000_000 // 900_000 * 900_000
INTERVAL_MS = 900_000


def kline(
    index: int,
    open_price,
    high,
    low,
    close,
    volume="100",
    taker="55",
):
    open_time = BASE_TIME_MS + index * INTERVAL_MS
    return [
        open_time,
        str(open_price),
        str(high),
        str(low),
        str(close),
        "1",
        open_time + INTERVAL_MS - 1,
        str(volume),
        "10",
        "1",
        str(taker),
        "0",
    ]


def n16_klines(*, elapsed_ms: int = 30_000):
    rows = []
    for index in range(90):
        close = Decimal("95") + Decimal(index) * Decimal("0.055")
        rows.append(
            kline(
                index,
                close - Decimal("0.15"),
                close + Decimal("0.45"),
                close - Decimal("0.45"),
                close,
            )
        )
    rows.extend(
        [
            kline(90, "100.1", "100.6", "99.0", "100.0"),
            kline(91, "100.0", "101.0", "99.7", "100.8"),
            kline(92, "100.8", "101.8", "100.5", "101.6"),
            kline(93, "101.6", "102.6", "101.3", "102.4"),
            kline(94, "102.4", "103.4", "102.1", "103.2"),
            kline(95, "103.2", "104.3", "102.9", "104.1"),
            kline(96, "104.1", "105.2", "103.8", "105.0"),
            kline(97, "105.0", "105.8", "104.7", "105.5"),
            kline(98, "105.5", "106.2", "105.0", "105.8"),
            kline(99, "105.7", "105.9", "104.8", "105.0"),
            kline(100, "105.0", "105.3", "104.1", "104.4"),
            kline(101, "104.4", "104.7", "103.5", "103.8"),
            kline(102, "103.8", "104.1", "102.9", "103.2"),
            kline(103, "103.2", "103.5", "102.4", "102.8"),
            kline(104, "102.8", "103.1", "102.1", "102.5"),
            kline(105, "102.5", "102.9", "102.0", "102.4"),
            kline(106, "102.4", "102.8", "100.2", "102.3"),
            kline(107, "102.3", "103.5", "102.1", "103.3"),
            kline(108, "103.3", "104.5", "103.1", "104.3"),
            kline(109, "104.3", "105.5", "104.1", "105.3"),
            kline(110, "105.3", "106.7", "105.1", "106.5"),
            kline(111, "106.5", "107.8", "106.3", "107.6"),
            kline(112, "107.6", "108.8", "107.4", "108.6"),
            kline(113, "108.6", "109.7", "108.4", "109.5"),
            kline(114, "109.5", "110.3", "109.1", "109.8"),
            kline(115, "109.7", "109.9", "108.6", "109.0", "80", "40"),
            kline(116, "109.0", "109.2", "107.9", "108.2", "80", "40"),
            kline(117, "108.2", "108.4", "106.9", "107.2", "80", "40"),
            kline(118, "107.1", "107.4", "105.8", "106.5", "80", "40"),
            kline(119, "106.5", "106.9", "106.0", "106.3", "80", "40"),
            kline(120, "106.2", "108.2", "106.0", "108.0", "100", "52"),
            kline(121, "108.0", "108.3", "107.8", "108.1", "100", "55"),
        ]
    )
    return rows, BASE_TIME_MS + 121 * INTERVAL_MS + elapsed_ms


def n16_early_metric_klines():
    baseline, _ = n16_klines()
    start = 27
    rows = []
    for index in range(start):
        close = Decimal("100.2") + Decimal(index) * Decimal("0.005")
        rows.append(
            kline(
                index,
                close - Decimal("0.05"),
                close + Decimal("0.15"),
                close - Decimal("0.15"),
                close,
            )
        )
    for target_index, source in enumerate(baseline[90:122], start=start):
        row = deepcopy(source)
        row[0] = BASE_TIME_MS + target_index * INTERVAL_MS
        row[6] = row[0] + INTERVAL_MS - 1
        rows.append(row)
    for index in range(start + 32, 122):
        close = Decimal("108.2") + Decimal(index - (start + 32)) * Decimal("0.05")
        rows.append(
            kline(
                index,
                close - Decimal("0.03"),
                close + Decimal("0.10"),
                close - Decimal("0.10"),
                close,
            )
        )
    return rows, rows[-1][0] + 30_000


def n16_constant_tr_threshold_klines():
    """Build an exact-Decimal fixture whose H2 ATR is exactly one."""
    rows = []
    for index in range(90):
        center = Decimal("99.86") + Decimal(index) * Decimal("0.01")
        rows.append(
            kline(
                index,
                center,
                center + Decimal("0.5"),
                center - Decimal("0.5"),
                center,
            )
        )
    centers = (
        "100.25", "100.75", "101.25", "101.75", "102.25",
        "102.75", "103.25", "103.75", "104.25", "104.1",
        "104", "103.9", "103.8", "103.7", "103.6", "103.55",
        "103.5", "103.625", "103.75", "103.875", "104",
        "104.125", "104.25", "104.375", "104.5",
    )
    for index, value in enumerate(centers, start=90):
        center = Decimal(value)
        rows.append(
            kline(
                index,
                center,
                center + Decimal("0.5"),
                center - Decimal("0.5"),
                center,
            )
        )
    rows.extend(
        (
            kline(115, "104.8", "104.9", "104.4", "104.8", "80", "40"),
            kline(116, "104.8", "104.9", "104.3", "104.8", "80", "40"),
            kline(117, "104.8", "104.9", "104.2", "104.8", "80", "40"),
            kline(118, "104.8", "104.9", "104.1", "104.8", "80", "40"),
            kline(119, "104.8", "104.9", "104.2", "104.7", "80", "40"),
            kline(120, "104.7", "105.5", "104.1", "105.2", "100", "52"),
            kline(121, "105.2", "105.5", "105", "105.3", "100", "55"),
        )
    )
    return rows, rows[-1][0] + 30_000


def n16_age_threshold_klines(age_bars: int):
    if age_bars not in {15, 16, 17}:
        raise ValueError("unsupported N16 test age")
    rows, checked_at_ms = n16_klines()
    rows = deepcopy(rows)
    replacements = {
        100: ("105", "106.2", "104.5", "105.8"),
        101: ("105.8", "107", "105.3", "106.7"),
        102: ("106.7", "107.6", "106.2", "107.3"),
        103: ("107.3", "108", "106.8", "107.6"),
        104: ("107", "107.4", "106", "106.8"),
        105: ("106.8", "107.1", "105.5", "106.2"),
        106: ("106.2", "106.5", "105.2", "105.8"),
        107: ("105.8", "106", "104.8", "105.3"),
        108: ("105", "105.5", "104", "104.5"),
        109: ("104.5", "106", "104.3", "105.8"),
        110: ("105.8", "107", "105.5", "106.8"),
        111: ("106.8", "108", "106.5", "107.8"),
        112: ("107.8", "109", "107.5", "108.8"),
        113: ("108.8", "109.8", "108.5", "109.5"),
        114: ("109.5", "110.3", "109.1", "109.8"),
    }
    for index, prices in replacements.items():
        rows[index] = kline(index, *prices)
    rows[97] = kline(97, "104.5", "105", "104", "104.5")
    rows[98] = kline(98, "104.5", "105", "104.2", "104.5")
    rows[99] = kline(99, "104.7", "105.5", "104.2", "105")
    if age_bars == 17:
        rows[97] = kline(97, "104", "105", "102.8", "104")
    elif age_bars == 16:
        rows[98] = kline(98, "104", "105", "103.5", "104")
    else:
        rows[99] = kline(99, "104", "105", "103.5", "104")
    for index, value in {
        109: "108.5",
        110: "108.8",
        111: "109.1",
        112: "109.4",
        113: "109.7",
    }.items():
        price = Decimal(value)
        rows[index] = kline(
            index,
            value,
            str(price + Decimal("0.05")),
            str(price - Decimal("0.6")),
            value,
            "100",
            "55",
        )
    rows[114] = kline(114, "110", "110.3", "109.1", "109.8", "100", "55")
    rows[115] = kline(115, "109.8", "110.1", "108.6", "110", "80", "40")
    rows[116] = kline(116, "110", "110.1", "107.9", "110", "80", "40")
    rows[117] = kline(117, "108.2", "108.4", "107.465", "107.6", "80", "40")
    rows[118] = kline(118, "108", "108.1", "107.5", "107.6", "80", "40")
    rows[119] = kline(119, "107.7", "107.9", "107.5", "107.6", "80", "40")
    rows[120] = kline(120, "107.6", "108.2", "107.465", "108", "100", "52")
    return rows, checked_at_ms


def n16_shallow_pullback_threshold_klines():
    rows, checked_at_ms = n16_constant_tr_threshold_klines()
    rows[98] = kline(98, "104.2", "104.7", "103.7", "104.2")
    for index in range(99, 115):
        rows[index][1] = rows[index][2]
        rows[index][4] = rows[index][2]
    rows[106] = kline(106, "104", "104", "101", "104")
    rows[115] = kline(115, "104.9", "105", "104.8", "105", "80", "40")
    rows[116] = kline(116, "104.9", "105", "104.7", "105", "80", "40")
    rows[117] = kline(117, "104.8", "105", "104.6", "104.9", "80", "40")
    rows[118] = kline(118, "104.6", "105", "104.4", "104.8", "80", "40")
    rows[119] = kline(119, "104.8", "105", "104.4", "104.7", "80", "40")
    rows[120] = kline(120, "104.7", "105.8", "104.5", "105.5", "100", "52")
    rows[121] = kline(121, "105.5", "105.8", "105.3", "105.6", "100", "55")
    return rows, checked_at_ms


def candidate(symbol="N16USDT", rank=7):
    return FundingCandidate(
        symbol=symbol,
        funding_rate=None,
        mark_price=Decimal("108.1"),
        quote_volume=Decimal("1000000"),
        quote_volume_rank=rank,
        candidate_universe="quote_volume_top",
    )


def n16_frozen_analyzer_config():
    """Bind every analyzer input to the independently frozen N16 definition."""
    return {
        "fixed_input_bars": N16_STRATEGY.fixed_input_bars,
        "closed_logic_bars": N16_STRATEGY.closed_logic_bars,
        "pivot_left": N16_STRATEGY.pivot_left,
        "pivot_right": N16_STRATEGY.pivot_right,
        "mature_min_bars": N16_STRATEGY.mature_min_bars,
        "h2_progress_atr_min": N16_STRATEGY.h2_progress_atr_min,
        "ema_fast_period": N16_STRATEGY.ema_fast_period,
        "ema_slow_period": N16_STRATEGY.ema_slow_period,
        "ema_slope_lookback_bars": N16_STRATEGY.ema_slope_lookback_bars,
        "atr_period": N16_STRATEGY.atr_period,
        "up_leg_atr_min": N16_STRATEGY.up_leg_atr_min,
        "up_leg_efficiency_min": N16_STRATEGY.up_leg_efficiency_min,
        "support_min_bars": N16_STRATEGY.support_min_bars,
        "support_max_bars": N16_STRATEGY.support_max_bars,
        "support_touch_upper_atr": N16_STRATEGY.support_touch_upper_atr,
        "support_close_lower_atr": N16_STRATEGY.support_close_lower_atr,
        "pullback_depth_min": N16_STRATEGY.pullback_depth_min,
        "pullback_depth_max": N16_STRATEGY.pullback_depth_max,
        "pullback_volume_ratio_max": N16_STRATEGY.pullback_volume_ratio_max,
        "confirmation_max_bars": N16_STRATEGY.confirmation_max_bars,
        "confirmation_close_location_min": (
            N16_STRATEGY.confirmation_close_location_min
        ),
        "confirmation_taker_buy_ratio_min": (
            N16_STRATEGY.confirmation_taker_buy_ratio_min
        ),
        "confirmation_volume_multiple_min": (
            N16_STRATEGY.confirmation_volume_multiple_min
        ),
        "entry_extension_atr_max": N16_STRATEGY.entry_extension_atr_max,
        "entry_window_seconds": N16_STRATEGY.entry_window_seconds,
    }


def resign_evidence(value):
    unsigned = deepcopy(value)
    unsigned.pop("canonical_sha256", None)
    value["canonical_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return value


def checkpoint_delete_mode(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
        if journal_mode == ("wal",):
            self_checkpoint = connection.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if self_checkpoint != (0, 0, 0):
                raise AssertionError("N16 test database checkpoint failed")
        elif journal_mode != ("delete",):
            raise AssertionError(
                f"N16 test database has unsupported journal mode: {journal_mode}"
            )
        if connection.execute("PRAGMA journal_mode=DELETE").fetchone() != (
            "delete",
        ):
            raise AssertionError("N16 test database did not enter DELETE mode")
    for suffix in ("-wal", "-journal"):
        if os.path.lexists(str(database) + suffix):
            raise AssertionError(
                f"N16 test database retained unsafe {suffix} sidecar"
            )
    shm = Path(str(database) + "-shm")
    if os.path.lexists(shm):
        details = shm.lstat()
        if shm.is_symlink() or not shm.is_file() or details.st_nlink != 1:
            raise AssertionError("N16 test database SHM sidecar is unsafe")
        shm.unlink()
    if any(
        os.path.lexists(str(database) + suffix)
        for suffix in ("-wal", "-shm", "-journal")
    ):
        raise AssertionError("N16 test database did not become sidecar-free")


def install_test_protected_generation_highwater(
    database: Path,
    ledger: Path,
) -> None:
    with closing(sqlite3.connect(database)) as connection:
        generation = connection.execute(
            "SELECT generation FROM history_coverage_protected_generation "
            "WHERE singleton_id=1"
        ).fetchone()
        schema_version = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()
        if generation is None or schema_version is None:
            raise AssertionError(
                "test protected generation commitment is unavailable"
            )
        catalog_sha256 = family_seal_catalog_sha256(connection)
    N16PermanentClaimLedger(
        ledger
    ).install_protected_generation_highwater(
        generation=generation[0],
        review_schema_version=schema_version[0],
        family_catalog_sha256=catalog_sha256,
        now="2026-07-27T00:00:00+00:00",
    )


def zero_write_database_fingerprint(database: Path):
    directory = tuple(
        (
            item.name,
            item.lstat().st_mode,
            item.lstat().st_size,
            item.lstat().st_mtime_ns,
            item.lstat().st_ino,
            item.lstat().st_nlink,
        )
        for item in sorted(database.parent.iterdir(), key=lambda path: path.name)
    )
    main_sha256 = hashlib.sha256(database.read_bytes()).hexdigest()
    uri = database.resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        sqlite_identity = (
            connection.execute("PRAGMA journal_mode").fetchone(),
            connection.execute("PRAGMA user_version").fetchone(),
            connection.execute("PRAGMA application_id").fetchone(),
            tuple(
                connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                    "ORDER BY type, name, tbl_name, sql"
                ).fetchall()
            ),
        )
    return main_sha256, directory, sqlite_identity


def downgrade_test_pair_to_pre_n16(
    database: Path,
    ledger: Path,
) -> None:
    """Build an exact pre-N16 fixture from a freshly certified empty pair."""

    checkpoint_delete_mode(database)
    checkpoint_delete_mode(ledger)
    with closing(sqlite3.connect(database)) as connection:
        for trigger_name in _N16_TRIGGER_SQL:
            connection.execute('DROP TRIGGER "%s"' % trigger_name)
        for index_name in _N16_INDEX_SQL:
            connection.execute('DROP INDEX "%s"' % index_name)
        for table in (
            "n16_consumption_seals",
            "n16_first_claim_witness",
            "n16_lifecycle_guard",
            "n16_trend_support_states",
            "strategy_lifecycle_installations",
        ):
            connection.execute('DROP TABLE "%s"' % table)
        connection.execute("ALTER TABLE events RENAME TO events_n16_old")
        connection.execute(_N16_PREINSTALL_EVENTS_TABLE_SQL)
        connection.execute(
            "INSERT INTO events(id,occurred_at,event_type,symbol,payload_json) "
            "SELECT id,occurred_at,event_type,symbol,payload_json "
            "FROM events_n16_old ORDER BY id"
        )
        connection.execute("DROP TABLE events_n16_old")
        connection.execute(
            "CREATE INDEX idx_events_type_time "
            "ON events(event_type, occurred_at)"
        )
        connection.commit()
    checkpoint_delete_mode(database)
    ledger.unlink()


_HISTORICAL_TRADE_REVIEWS_BASE_SQL = """
CREATE TABLE trade_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER,
    opened_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity TEXT,
    entry_price TEXT,
    stop_loss_price TEXT,
    take_profit_price TEXT,
    amplitude_24h_pct TEXT,
    high_24h_price TEXT,
    low_24h_price TEXT,
    stop_loss_pct TEXT,
    take_profit_pct TEXT,
    risk_amount TEXT,
    notional_value TEXT,
    required_margin TEXT,
    balance TEXT,
    leverage INTEGER,
    dry_run INTEGER NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    orders_json TEXT NOT NULL,
    FOREIGN KEY(scan_id) REFERENCES scans(id)
)
""".strip()
_HISTORICAL_TRADE_REVIEWS_ALTERS = (
    ("target_risk_amount", "TEXT"),
    ("actual_risk_amount", "TEXT"),
    ("risk_capped_by_margin", "INTEGER"),
    ("pretrade_quantity", "TEXT"),
    ("executed_quantity", "TEXT"),
    ("final_protected_quantity", "TEXT"),
    ("post_fill_actual_risk_amount", "TEXT"),
    ("post_fill_required_margin", "TEXT"),
    ("reduced_after_fill", "INTEGER"),
    ("closed_at", "TEXT"),
    ("exit_reason", "TEXT"),
    ("exit_price", "TEXT"),
    ("close_mark_price", "TEXT"),
    ("realized_pnl", "TEXT"),
    ("realized_pnl_pct", "TEXT"),
    ("balance_after_close", "TEXT"),
)


def rebuild_trade_reviews_as_historical_migration(
    connection: sqlite3.Connection,
) -> None:
    """Reproduce the exact production CREATE-then-ALTER column order."""

    connection.execute("DROP TABLE trade_reviews")
    connection.execute(_HISTORICAL_TRADE_REVIEWS_BASE_SQL)
    connection.execute(
        "CREATE INDEX idx_trade_reviews_symbol_time "
        "ON trade_reviews(symbol, opened_at)"
    )
    for column, definition in _HISTORICAL_TRADE_REVIEWS_ALTERS:
        connection.execute(
            "ALTER TABLE trade_reviews ADD COLUMN %s %s"
            % (column, definition)
        )
    actual_sql = connection.execute(
        "SELECT sql FROM sqlite_schema "
        "WHERE type='table' AND name='trade_reviews'"
    ).fetchone()[0]
    actual_xinfo = tuple(
        tuple(row[1:7])
        for row in connection.execute(
            "PRAGMA table_xinfo(trade_reviews)"
        ).fetchall()
    )
    if (
        _n16_normalized_sql(actual_sql)
        != _n16_normalized_sql(_N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL)
        or actual_xinfo != _N16_MIGRATED_TRADE_REVIEWS_XINFO
    ):
        raise AssertionError("historical trade_reviews fixture drifted")


def tamper_shared_execution_table(
    connection: sqlite3.Connection,
    table: str,
    attack: str,
) -> None:
    key_columns = {
        "trade_reviews": "symbol",
        "strategy_live_links": "symbol",
        "strategy_paper_trades": "symbol",
        "strategy_states": "strategy_id",
        "symbol_cooldowns": "symbol",
    }
    if attack == "extra_unique":
        connection.execute(
            'CREATE UNIQUE INDEX "hostile_%s_unique" '
            'ON "%s"("%s")'
            % (table, table, key_columns[table])
        )
        return
    if attack == "expression_index":
        connection.execute(
            'CREATE INDEX "hostile_%s_expression" '
            'ON "%s"(lower("%s"))'
            % (table, table, key_columns[table])
        )
        return
    if attack == "trigger":
        connection.execute(
            'CREATE TRIGGER "hostile_%s_insert" BEFORE INSERT ON "%s" '
            "BEGIN SELECT RAISE(ABORT, 'hostile shared trigger'); END"
            % (table, table)
        )
        return

    table_sql = connection.execute(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
        (table,),
    ).fetchone()[0]
    explicit_indexes = tuple(
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='index' "
            "AND tbl_name=? AND sql IS NOT NULL ORDER BY name",
            (table,),
        ).fetchall()
    )
    self_closing = table_sql.rstrip()
    if not self_closing.endswith(")"):
        raise AssertionError("shared table SQL is not closed")
    if attack == "check":
        forged_sql = self_closing[:-1] + ", CHECK(1 = 1))"
    elif attack == "foreign_key":
        forged_sql = (
            self_closing[:-1]
            + ', FOREIGN KEY("%s") REFERENCES strategy_definitions(strategy_id))'
            % key_columns[table]
        )
    elif attack == "generated":
        foreign_key = self_closing.upper().rfind("FOREIGN KEY")
        if foreign_key >= 0:
            line_start = self_closing.rfind("\n", 0, foreign_key) + 1
            forged_sql = (
                self_closing[:line_start]
                + "hostile_generated TEXT GENERATED ALWAYS AS ('x') VIRTUAL,\n"
                + self_closing[line_start:]
            )
        else:
            forged_sql = (
                self_closing[:-1]
                + ", hostile_generated TEXT GENERATED ALWAYS AS ('x') VIRTUAL)"
            )
    else:
        raise AssertionError("unknown shared table attack: %s" % attack)
    connection.execute('DROP TABLE "%s"' % table)
    connection.execute(forged_sql)
    for index_sql in explicit_indexes:
        connection.execute(index_sql)


def corrupt_n16_audit_graph_with_catalog_generation_preserved(
    database: Path,
) -> None:
    """Create one internally catalog-consistent but invalid Review graph."""

    triggers = (
        "trg_n16_audit_no_update_after_active",
        "trg_n16_guard_no_update",
        "trg_n16_root_no_update",
    )
    with closing(sqlite3.connect(database)) as connection:
        initial_cookie = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()[0]
        for trigger in triggers:
            connection.execute("DROP TRIGGER " + trigger)
        changed = connection.execute(
            "UPDATE strategy_passed_signal_audits SET detail_json='{}' "
            "WHERE id=(SELECT id FROM strategy_passed_signal_audits "
            "WHERE strategy_id='N16' ORDER BY source_signal_id LIMIT 1)"
        )
        if changed.rowcount != 1:
            raise AssertionError("N16 test audit graph was not changed")
        final_cookie = initial_cookie + 2 * len(triggers)
        connection.execute(
            "UPDATE n16_lifecycle_guard SET catalog_schema_version=? "
            "WHERE singleton_id=1",
            (final_cookie,),
        )
        connection.execute(
            "UPDATE strategy_lifecycle_installations "
            "SET catalog_schema_version=? "
            "WHERE singleton_id=1 AND strategy_id='N16'",
            (final_cookie,),
        )
        for trigger in triggers:
            connection.execute(_N16_TRIGGER_SQL[trigger])
        if connection.execute("PRAGMA schema_version").fetchone() != (
            final_cookie,
        ):
            raise AssertionError("N16 test catalog generation did not converge")
        connection.commit()


class N16DefinitionTests(unittest.TestCase):
    def test_n01_n15_json_is_unchanged_and_n16_is_independent(self):
        strategies = load_all_strategies()
        self.assertEqual([item.strategy_id for item in strategies[:16]], [
            "N%02d" % number for number in range(1, 17)
        ])
        original_json = json.dumps(
            [item.to_jsonable() for item in strategies[:15]],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(
            hashlib.sha256(original_json).hexdigest(),
            "6f505d318a0f9bd830cc938b5553a47d6251fb1bee83dca821d836be9d6aef87",
        )
        self.assertEqual(N16_STRATEGY.risk_reward_ratio, Decimal("5"))
        self.assertEqual(N16_STRATEGY.fixed_input_bars, 122)
        self.assertEqual(N16_STRATEGY.closed_logic_bars, 96)
        self.assertEqual(
            n16_frozen_analyzer_config(),
            {
                "fixed_input_bars": 122,
                "closed_logic_bars": 96,
                "pivot_left": 2,
                "pivot_right": 2,
                "mature_min_bars": 16,
                "h2_progress_atr_min": Decimal("0.25"),
                "ema_fast_period": 20,
                "ema_slow_period": 50,
                "ema_slope_lookback_bars": 8,
                "atr_period": 14,
                "up_leg_atr_min": Decimal("2"),
                "up_leg_efficiency_min": Decimal("0.40"),
                "support_min_bars": 2,
                "support_max_bars": 8,
                "support_touch_upper_atr": Decimal("0.25"),
                "support_close_lower_atr": Decimal("0.20"),
                "pullback_depth_min": Decimal("0.15"),
                "pullback_depth_max": Decimal("0.45"),
                "pullback_volume_ratio_max": Decimal("0.90"),
                "confirmation_max_bars": 3,
                "confirmation_close_location_min": Decimal("0.65"),
                "confirmation_taker_buy_ratio_min": Decimal("0.52"),
                "confirmation_volume_multiple_min": Decimal("0.90"),
                "entry_extension_atr_max": Decimal("0.50"),
                "entry_window_seconds": 120,
            },
        )

    def test_readme_documents_only_the_frozen_n16_release(self):
        readme = (Path(__file__).resolve().parents[1] / "docs" / "LEGACY_ENGINEERING.md").read_text(
            encoding="utf-8"
        )
        for required in (
            "N06-N25",
            "n16_analyzer.py",
            "n16_trend_support_states",
            "quote_volume_rank→symbol→structure_id",
            "N16_ACTUAL_FILL_OUTSIDE_ENTRY_RANGE",
            "不保证信号频率、胜率或实盘收益",
        ):
            self.assertIn(required, readme)
        for stale in ("N06-N15", "N07-N15", "N10-N15", "N12-N15"):
            self.assertNotIn(stale, readme)


class N16AnalyzerTests(unittest.TestCase):
    def analyze(self, rows, checked_at_ms, **kwargs):
        config = n16_frozen_analyzer_config()
        config.update(kwargs)
        return analyze_n16_mature_trend_support(
            "N16USDT",
            rows,
            quote_volume_rank=7,
            checked_at_ms=checked_at_ms,
            **config,
        )

    def test_authenticated_unicode_symbol_survives_state_evidence_round_trip(self):
        rows, checked_at_ms = n16_klines()
        result = analyze_n16_mature_trend_support(
            "龙虾USDT",
            rows,
            quote_volume_rank=74,
            checked_at_ms=checked_at_ms,
            **n16_frozen_analyzer_config(),
        )

        self.assertEqual(result.symbol, "龙虾USDT")
        self.assertIsNotNone(result.state_record)
        decoded = decode_n16_state_envelope(
            result.state_record.evidence,
            expected_symbol="龙虾USDT",
        )
        self.assertEqual(decoded.symbol, "龙虾USDT")
        with self.assertRaises(ValueError):
            decode_n16_state_envelope(
                result.state_record.evidence,
                expected_symbol="龍蝦USDT",
            )

    def test_wilder_atr_uses_each_true_range_exactly_once(self):
        rows = [
            kline(0, "10", "12", "9", "11"),
            kline(1, "11", "14", "10", "13"),
            kline(2, "13", "15", "12", "14"),
            kline(3, "14", "18", "13", "17"),
            kline(4, "17", "19", "16", "18"),
        ]
        atr = _atr_series(parse_n16_klines(rows), 3)
        seed = (Decimal("3") + Decimal("4") + Decimal("3")) / Decimal("3")
        next_atr = (seed * Decimal("2") + Decimal("5")) / Decimal("3")
        final_atr = (next_atr * Decimal("2") + Decimal("3")) / Decimal("3")
        self.assertEqual(atr[:2], [None, None])
        self.assertEqual(atr[2], seed)
        self.assertEqual(atr[3], next_atr)
        self.assertEqual(atr[4], final_atr)

    def test_complete_mature_trend_support_passes_and_identity_uses_times(self):
        rows, checked = n16_klines()
        result = self.analyze(rows, checked)
        self.assertTrue(result.passed, result.reason)
        self.assertEqual(result.reason, "PASSED")
        structure = result.structure
        self.assertEqual(
            (
                structure.l1.index,
                structure.h1.index,
                structure.l2.index,
                structure.h2.index,
                structure.a.index,
                structure.c.index,
                structure.entry.index,
            ),
            (90, 98, 106, 114, 118, 120, 121),
        )
        self.assertGreaterEqual(structure.h2.high - structure.h1.high,
                                Decimal("0.25") * structure.atr_h2)
        self.assertGreater(structure.l2.low, structure.l1.low)
        self.assertEqual(structure.entry_min_price, structure.c.close)
        self.assertEqual(
            structure.entry_max_price,
            structure.c.close + Decimal("0.50") * structure.atr_c,
        )
        self.assertNotIn(str(structure.atr_c), structure.structure_id)
        self.assertEqual(structure.trend_id, "d8ea4ba76aa8f5c42c0b941c")
        self.assertEqual(structure.episode_id, "3fe87c916cf0a9dfbae5f38c")
        self.assertEqual(structure.structure_id, "a70ebc165a381c3887d6126b")
        self.assertLess(len(json.dumps(result.detail_json()).encode("utf-8")), 16_384)
        decoded = decode_n16_state_envelope(result.state_record.evidence)
        self.assertEqual(
            decoded.qualified_observation,
            {
                "entry": structure.entry.to_jsonable(False),
                "observed_at_ms": checked,
                "elapsed_ms": 30_000,
                "entry_deadline_ms": (
                    structure.entry.open_time_ms + 120_000
                ),
            },
        )

    def test_entry_closed_boundaries_and_deadline(self):
        rows, checked = n16_klines(elapsed_ms=0)
        baseline = self.analyze(rows, checked)
        self.assertTrue(baseline.passed)
        for elapsed in (0, 119_999):
            sample, sample_checked = n16_klines(elapsed_ms=elapsed)
            self.assertTrue(self.analyze(sample, sample_checked).passed)
        sample, sample_checked = n16_klines(elapsed_ms=120_000)
        self.assertEqual(
            self.analyze(sample, sample_checked).reason,
            "N16_ENTRY_WINDOW_EXPIRED",
        )
        sample, sample_checked = n16_klines(elapsed_ms=120_001)
        self.assertEqual(
            self.analyze(sample, sample_checked).reason,
            "N16_ENTRY_WINDOW_EXPIRED",
        )

        for delta, expected in (
            (Decimal("-0.001"), "N16_ENTRY_LOW_BROKE_P"),
            (Decimal("0"), "PASSED"),
            (Decimal("0.001"), "PASSED"),
        ):
            with self.subTest(boundary="p", delta=str(delta)):
                sample = deepcopy(rows)
                sample[-1][3] = str(baseline.structure.p + delta)
                self.assertEqual(self.analyze(sample, checked).reason, expected)

        for delta, expected in (
            (Decimal("-0.001"), "N16_ENTRY_WAITING_PRICE"),
            (Decimal("0"), "PASSED"),
            (Decimal("0.001"), "PASSED"),
        ):
            with self.subTest(boundary="entry_min", delta=str(delta)):
                sample = deepcopy(rows)
                sample[-1][4] = str(
                    baseline.structure.entry_min_price + delta
                )
                result = self.analyze(sample, checked)
                self.assertEqual(result.reason, expected)
                self.assertEqual(result.consume_current, False)

        for delta, expected in (
            (Decimal("-0.001"), "PASSED"),
            (Decimal("0"), "PASSED"),
            (Decimal("0.001"), "N16_ENTRY_PRICE_TOO_EXTENDED"),
        ):
            with self.subTest(boundary="entry_max", delta=str(delta)):
                sample = deepcopy(rows)
                price = baseline.structure.entry_max_price + delta
                sample[-1][2] = sample[-1][4] = str(price)
                result = self.analyze(sample, checked)
                self.assertEqual(result.reason, expected)
                self.assertEqual(
                    result.consume_current,
                    expected == "N16_ENTRY_PRICE_TOO_EXTENDED",
                )

    def test_first_touch_and_first_confirmation_are_locked(self):
        rows, checked = n16_klines()
        first = self.analyze(rows, checked)
        self.assertEqual(first.structure.a.index, 118)
        later_better_a = deepcopy(rows)
        later_better_a[119][3] = "105.7"
        later_better_a[119][4] = "106.8"
        replay = self.analyze(later_better_a, checked)
        self.assertEqual(replay.structure.a.index, 118)

        first_c_is_valid = deepcopy(rows)
        first_c_is_valid[119][1:5] = ["106.0", "107.8", "105.9", "107.6"]
        first_c_is_valid[119][10] = "52"
        first_c_is_valid[119][7] = "100"
        result = self.analyze(first_c_is_valid, checked)
        self.assertEqual(result.structure.c.index, 119)

    def test_frozen_definition_rejects_runtime_threshold_substitution(self):
        rows, checked = n16_klines()
        baseline = self.analyze(rows, checked)
        self.assertTrue(baseline.passed)
        exact = self.analyze(
            rows,
            checked,
            h2_progress_atr_min=(
                (baseline.structure.h2.high - baseline.structure.h1.high)
                / baseline.structure.atr_h2
            ),
            up_leg_atr_min=(
                (baseline.structure.h2.high - baseline.structure.l2.low)
                / baseline.structure.atr_h2
            ),
            up_leg_efficiency_min=baseline.structure.up_leg_efficiency,
            pullback_depth_min=baseline.structure.pullback_depth,
            pullback_depth_max=baseline.structure.pullback_depth,
            pullback_volume_ratio_max=baseline.structure.pullback_volume_ratio,
            confirmation_close_location_min=baseline.structure.c_close_location,
            confirmation_taker_buy_ratio_min=baseline.structure.c_taker_buy_ratio,
            confirmation_volume_multiple_min=baseline.structure.c_volume_multiple,
        )
        # N16 is a frozen rule definition; callers may not silently substitute
        # per-run thresholds, even when a fixture sits exactly on them.
        self.assertFalse(exact.passed)
        self.assertEqual(exact.reason, "N16_DEFINITION_INVALID")

    def test_default_trend_threshold_edges(self):
        for age, expected in (
            (15, "N16_TREND_AGE_TOO_SHORT"),
            (16, "PASSED"),
            (17, "PASSED"),
        ):
            with self.subTest(boundary="mature_age", age=age):
                rows, checked = n16_age_threshold_klines(age)
                parsed = parse_n16_klines(rows)
                skeleton = _latest_skeleton(parsed[:-1][-96:], 2, 2)
                self.assertIsNotNone(skeleton)
                self.assertEqual(skeleton[3].index - skeleton[0].index, age)
                result = self.analyze(rows, checked)
                self.assertEqual(result.reason, expected)

        rows, checked = n16_klines()
        for low, relation, expected in (
            ("98.999", "below", "N16_HIGHER_LOW_NOT_CONFIRMED"),
            ("99", "equal", "N16_HIGHER_LOW_NOT_CONFIRMED"),
            ("99.001", "above", "PASSED"),
        ):
            with self.subTest(boundary="higher_low", relation=relation):
                sample = deepcopy(rows)
                sample[106][3] = low
                parsed = parse_n16_klines(sample)
                skeleton = _latest_skeleton(parsed[:-1][-96:], 2, 2)
                comparison = skeleton[2].low.compare(skeleton[0].low)
                self.assertEqual(comparison, {"below": -1, "equal": 0, "above": 1}[relation])
                self.assertEqual(self.analyze(sample, checked).reason, expected)

        exact_rows, exact_checked = n16_constant_tr_threshold_klines()
        for delta, comparison, expected in (
            (Decimal("0.001"), -1, "N16_HIGHER_HIGH_TOO_SMALL"),
            (Decimal("0"), 0, "PASSED"),
            (Decimal("-0.001"), 1, "PASSED"),
        ):
            with self.subTest(boundary="higher_high_atr", delta=str(delta)):
                sample = deepcopy(exact_rows)
                for offset in range(1, 5):
                    sample[98][offset] = str(
                        Decimal(sample[98][offset]) + delta
                    )
                parsed = parse_n16_klines(sample)
                skeleton = _latest_skeleton(parsed[:-1][-96:], 2, 2)
                atr_h2 = _atr_series(parsed, 14)[skeleton[3].index]
                ratio = (skeleton[3].high - skeleton[1].high) / atr_h2
                self.assertEqual(ratio.compare(Decimal("0.25")), comparison)
                result = self.analyze(sample, exact_checked)
                self.assertEqual(result.reason, expected)

        for delta, comparison, expected in (
            (Decimal("0.001"), -1, "N16_UP_LEG_ATR_TOO_SMALL"),
            (Decimal("0"), 0, "PASSED"),
            (Decimal("-0.001"), 1, "PASSED"),
        ):
            with self.subTest(boundary="up_leg_atr", delta=str(delta)):
                sample = deepcopy(exact_rows)
                for offset in range(1, 5):
                    sample[106][offset] = str(
                        Decimal(sample[106][offset]) + delta
                    )
                parsed = parse_n16_klines(sample)
                skeleton = _latest_skeleton(parsed[:-1][-96:], 2, 2)
                atr_h2 = _atr_series(parsed, 14)[skeleton[3].index]
                ratio = (skeleton[3].high - skeleton[2].low) / atr_h2
                self.assertEqual(ratio.compare(Decimal("2")), comparison)
                result = self.analyze(sample, exact_checked)
                self.assertEqual(result.reason, expected)

        efficient_closes = (
            "103.5", "104.125", "103.375", "103.875", "104",
            "104.125", "104.25", "104.375", "104.5",
        )
        for mode, expected_comparison, expected in (
            ("below", -1, "N16_UP_LEG_EFFICIENCY_TOO_LOW"),
            ("equal", 0, "PASSED"),
            ("above", 1, "PASSED"),
        ):
            with self.subTest(boundary="up_leg_efficiency", mode=mode):
                sample = deepcopy(exact_rows)
                for index, value in enumerate(efficient_closes, start=106):
                    sample[index][1] = sample[index][4] = value
                if mode == "below":
                    sample[108][1] = sample[108][4] = "103.374"
                    sample[109][2] = str(Decimal(sample[109][2]) - Decimal("0.001"))
                    sample[109][3] = str(Decimal(sample[109][3]) - Decimal("0.001"))
                elif mode == "above":
                    sample[108][1] = sample[108][4] = "103.376"
                parsed = parse_n16_klines(sample)
                skeleton = _latest_skeleton(parsed[:-1][-96:], 2, 2)
                efficiency = _path_efficiency(
                    parsed[skeleton[2].index : skeleton[3].index + 1]
                )
                self.assertEqual(
                    efficiency.compare(Decimal("0.40")), expected_comparison
                )
                result = self.analyze(sample, exact_checked)
                self.assertEqual(result.reason, expected)

    def test_default_support_and_pullback_threshold_edges(self):
        rows, checked = n16_klines()
        ignored_offset_one = deepcopy(rows)
        ignored_offset_one[115] = kline(
            115, "109.7", "109.9", "105.9", "109", "80", "40"
        )
        parsed_offset_one = parse_n16_klines(ignored_offset_one)
        ema20_offset_one = _ema_series(parsed_offset_one, 20)
        atr_offset_one = _atr_series(parsed_offset_one, 14)
        self.assertLessEqual(
            parsed_offset_one[115].low,
            ema20_offset_one[115] + Decimal("0.25") * atr_offset_one[115],
        )
        self.assertGreaterEqual(
            parsed_offset_one[115].close,
            ema20_offset_one[115] - Decimal("0.20") * atr_offset_one[115],
        )
        ignored_result = self.analyze(ignored_offset_one, checked)
        self.assertTrue(ignored_result.passed, ignored_result.reason)
        self.assertEqual(ignored_result.structure.a.index, 118)

        for low, expected_relation, expected_a in (
            ("106.079", -1, 116),
            ("106.08", 0, 116),
            ("106.081", 1, 118),
        ):
            with self.subTest(boundary="support_touch", low=low):
                sample = deepcopy(rows)
                sample[116] = kline(
                    116,
                    "109",
                    "110.1029908457826913698490600",
                    low,
                    "107",
                    "80",
                    "40",
                )
                parsed = parse_n16_klines(sample)
                ema20 = _ema_series(parsed, 20)[116]
                atr = _atr_series(parsed, 14)[116]
                upper = ema20 + Decimal("0.25") * atr
                self.assertEqual(Decimal(low).compare(upper), expected_relation)
                result = self.analyze(sample, checked)
                self.assertEqual(result.structure.a.index, expected_a)
                if expected_a == 116:
                    self.assertEqual(result.structure.a.index - result.structure.h2.index, 2)

        max_offset = deepcopy(rows)
        max_offset[112][2] = "110.8"
        max_offset[113][2] = "110.5"
        max_offset[114][2] = "110.3"
        max_offset[118][3] = max_offset[118][4] = "106.5"
        max_offset[119] = kline(119, "107", "107.3", "106.8", "107", "80", "40")
        max_offset[120][3] = "106.1"
        max_result = self.analyze(max_offset, checked)
        self.assertEqual(max_result.reason, "N16_TOUCH_LOCKED")
        self.assertEqual(max_result.structure.a.index - max_result.structure.h2.index, 8)

        offset_nine = deepcopy(rows)
        offset_nine[111][2] = "110.8"
        offset_nine[112][2] = "110.5"
        offset_nine[113][2] = "110.3"
        offset_nine[114][2] = "110.1"
        offset_nine[118][3] = offset_nine[118][4] = "106.5"
        offset_nine[119] = kline(
            119, "107", "107.3", "106.8", "107", "80", "40"
        )
        offset_nine[120][3] = "106.1"
        parsed_nine = parse_n16_klines(offset_nine)
        skeleton_nine = _latest_skeleton(parsed_nine[:-1][-96:], 2, 2)
        self.assertEqual(skeleton_nine[3].index, 111)
        ema20_nine = _ema_series(parsed_nine, 20)
        atr_nine = _atr_series(parsed_nine, 14)
        self.assertLessEqual(
            parsed_nine[120].low,
            ema20_nine[120] + Decimal("0.25") * atr_nine[120],
        )
        self.assertGreaterEqual(
            parsed_nine[120].close,
            ema20_nine[120] - Decimal("0.20") * atr_nine[120],
        )
        self.assertEqual(120 - skeleton_nine[3].index, 9)
        self.assertEqual(
            self.analyze(offset_nine, checked).reason,
            "N16_PULLBACK_WINDOW_EXPIRED",
        )

        for close, expected_relation, expected_a in (
            ("105.6518603972146062936609614", -1, 119),
            ("105.6528603972146062936609614", 0, 118),
            ("105.6538603972146062936609614", 1, 118),
        ):
            with self.subTest(boundary="support_close", close=close):
                sample = deepcopy(rows)
                sample[106][3] = "99.5"
                sample[118][3] = "105.5"
                sample[118][4] = close
                parsed = parse_n16_klines(sample)
                ema20 = _ema_series(parsed, 20)[118]
                atr = _atr_series(parsed, 14)[118]
                lower = ema20 - Decimal("0.20") * atr
                self.assertEqual(Decimal(close).compare(lower), expected_relation)
                self.assertEqual(self.analyze(sample, checked).structure.a.index, expected_a)

        for low, comparison, expected in (
            ("105.754", 1, "N16_PULLBACK_DEPTH_OUT_OF_RANGE"),
            ("105.755", 0, "PASSED"),
            ("105.756", -1, "PASSED"),
        ):
            with self.subTest(boundary="pullback_depth_max", low=low):
                sample = deepcopy(rows)
                sample[118][3] = low
                result = self.analyze(sample, checked)
                self.assertEqual(
                    result.structure.pullback_depth.compare(Decimal("0.45")),
                    comparison,
                )
                self.assertEqual(result.reason, expected)

        shallow, shallow_checked = n16_shallow_pullback_threshold_klines()
        for low, comparison, expected in (
            ("104.399", 1, "PASSED"),
            ("104.4", 0, "PASSED"),
            ("104.401", -1, "N16_PULLBACK_DEPTH_OUT_OF_RANGE"),
        ):
            with self.subTest(boundary="pullback_depth_min", low=low):
                sample = deepcopy(shallow)
                sample[118][3] = low
                result = self.analyze(sample, shallow_checked)
                self.assertEqual(
                    result.structure.pullback_depth.compare(Decimal("0.15")),
                    comparison,
                )
                self.assertEqual(result.reason, expected)

        for volume, comparison, expected in (
            ("89.999", -1, "PASSED"),
            ("90", 0, "PASSED"),
            ("90.001", 1, "N16_PULLBACK_VOLUME_TOO_HIGH"),
        ):
            with self.subTest(boundary="pullback_volume", volume=volume):
                sample = deepcopy(rows)
                for index in range(115, 119):
                    sample[index][7] = volume
                    sample[index][10] = str(Decimal(volume) * Decimal("0.5"))
                result = self.analyze(sample, checked)
                self.assertEqual(
                    result.structure.pullback_volume_ratio.compare(Decimal("0.90")),
                    comparison,
                )
                self.assertEqual(result.reason, expected)

    def test_default_confirmation_threshold_edges(self):
        rows, checked = n16_klines()
        first_confirmation = deepcopy(rows)
        first_confirmation[119][1:5] = ["106.0", "107.8", "105.9", "107.6"]
        first_confirmation[119][7] = "100"
        first_confirmation[119][10] = "52"
        first = self.analyze(first_confirmation, checked)
        self.assertEqual(first.structure.c.index - first.structure.a.index, 1)
        self.assertEqual(first.reason, "N16_HISTORICAL_ENTRY_MISSED")

        third_confirmation = deepcopy(rows)
        third_confirmation[117][3] = "106.2"
        third = self.analyze(third_confirmation, checked)
        self.assertTrue(third.passed, third.reason)
        self.assertEqual(third.structure.c.index - third.structure.a.index, 3)

        fourth_confirmation = deepcopy(rows)
        fourth_confirmation[116] = kline(
            116,
            "109",
            "110.1029908457826913698490600",
            "106.08",
            "107",
            "80",
            "40",
        )
        parsed_fourth = parse_n16_klines(fourth_confirmation)
        self.assertIsNone(
            _confirmation_failure_reason(
                parsed_fourth,
                120,
                _ema_series(parsed_fourth, 20),
                _ema_series(parsed_fourth, 50),
                _atr_series(parsed_fourth, 14),
                _APPROVED_N16_CONFIG,
            )
        )
        fourth = self.analyze(fourth_confirmation, checked)
        self.assertEqual(fourth.structure.a.index, 116)
        self.assertIsNone(fourth.structure.c)
        self.assertEqual(fourth.state_record.stage, "EXPIRED")
        self.assertEqual(fourth.reason, "N16_CONFIRMATION_NOT_BULLISH")

        for close, comparison, expected in (
            ("107.429", -1, "N16_CONFIRMATION_PENDING"),
            ("107.43", 0, "PASSED"),
            ("107.431", 1, "PASSED"),
        ):
            with self.subTest(boundary="confirmation_close_location", close=close):
                sample = deepcopy(rows)
                sample[120][4] = close
                location = (
                    (Decimal(close) - Decimal(sample[120][3]))
                    / (Decimal(sample[120][2]) - Decimal(sample[120][3]))
                )
                self.assertEqual(location.compare(Decimal("0.65")), comparison)
                self.assertEqual(self.analyze(sample, checked).reason, expected)

        for taker, comparison, expected in (
            ("51.999", -1, "N16_CONFIRMATION_PENDING"),
            ("52", 0, "PASSED"),
            ("52.001", 1, "PASSED"),
        ):
            with self.subTest(boundary="confirmation_taker", taker=taker):
                sample = deepcopy(rows)
                sample[120][10] = taker
                ratio = Decimal(taker) / Decimal(sample[120][7])
                self.assertEqual(ratio.compare(Decimal("0.52")), comparison)
                self.assertEqual(self.analyze(sample, checked).reason, expected)

        for volume, comparison, expected in (
            ("89.999", -1, "N16_CONFIRMATION_PENDING"),
            ("90", 0, "PASSED"),
            ("90.001", 1, "PASSED"),
        ):
            with self.subTest(boundary="confirmation_volume", volume=volume):
                sample = deepcopy(rows)
                sample[120][7] = volume
                sample[120][10] = str(Decimal(volume) * Decimal("0.52"))
                multiple = Decimal(volume) / Decimal("100")
                self.assertEqual(multiple.compare(Decimal("0.90")), comparison)
                self.assertEqual(self.analyze(sample, checked).reason, expected)

    def test_fixed_122_window_and_invalid_data_fail_closed(self):
        rows, checked = n16_klines()
        self.assertTrue(self.analyze(rows, checked).passed)
        self.assertEqual(
            self.analyze(rows[1:], checked).reason,
            "N16_NOT_ENOUGH_HISTORY",
        )
        extra = [kline(-1, "94", "95", "93", "94"), *deepcopy(rows)]
        shifted = self.analyze(extra, checked)
        self.assertTrue(shifted.passed, shifted.reason)
        broken = deepcopy(rows)
        broken[60][0] += INTERVAL_MS
        self.assertEqual(
            self.analyze(broken, checked).reason,
            "N16_KLINE_SEQUENCE_INVALID",
        )

    def test_incomplete_ema_slope_history_is_plain_rejection_without_state(self):
        rows, checked = n16_early_metric_klines()
        result = self.analyze(rows, checked)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "N16_METRIC_HISTORY_INCOMPLETE")
        self.assertIsNone(result.state_record)
        self.assertEqual(result.state_records, ())

    def test_current_axis_not_ready_waits_without_consuming_frozen_episode(self):
        rows, checked = n16_klines(elapsed_ms=30_000)
        for invalid_checked_at in (
            rows[-1][0] - 1,
            rows[-1][0] + INTERVAL_MS,
        ):
            with self.subTest(checked_at=invalid_checked_at):
                waiting = self.analyze(rows, invalid_checked_at)
                self.assertFalse(waiting.passed)
                self.assertEqual(waiting.reason, "N16_KLINE_AXIS_NOT_READY")
                self.assertIsNone(waiting.state_record)

        first = self.analyze(rows, checked)
        self.assertTrue(first.passed)
        next_candle = kline(122, "108.1", "108.4", "107.9", "108.2", "105", "57")
        rolled = deepcopy(rows[1:]) + [next_candle]
        premature = self.analyze(
            rolled,
            rows[-1][0] + 30_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertEqual(premature.reason, "N16_KLINE_AXIS_NOT_READY")
        self.assertIsNone(premature.state_record)
        recovered = self.analyze(
            rolled,
            next_candle[0] + 30_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertEqual(recovered.reason, "N16_HISTORICAL_ENTRY_MISSED")
        self.assertIsNotNone(recovered.state_record)

    def test_first_qualified_entry_is_permanent_and_live_e_may_only_extend(self):
        rows, checked = n16_klines(elapsed_ms=15_000)
        first = self.analyze(rows, checked)
        self.assertTrue(first.passed)
        first_hash = first.state_record.evidence_sha256
        first_observation = deepcopy(
            first.state_record.evidence["qualified_observation"]
        )

        evolved = deepcopy(rows)
        evolved[-1][2] = "108.5"
        evolved[-1][3] = "107.7"
        evolved[-1][4] = "108.2"
        evolved[-1][7] = "125"
        evolved[-1][10] = "65"
        replay = self.analyze(
            evolved,
            rows[-1][0] + 60_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertTrue(replay.passed, replay.reason)
        self.assertEqual(replay.state_record.evidence_sha256, first_hash)
        self.assertEqual(
            replay.state_record.evidence["qualified_observation"],
            first_observation,
        )

        waiting_rows = deepcopy(evolved)
        waiting_rows[-1][4] = "107.9"
        waiting = self.analyze(
            waiting_rows,
            rows[-1][0] + 61_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertFalse(waiting.passed)
        self.assertEqual(waiting.reason, "N16_ENTRY_WAITING_PRICE")
        self.assertEqual(
            waiting.state_record.evidence["qualified_observation"],
            first_observation,
        )
        recovered_rows = deepcopy(waiting_rows)
        recovered_rows[-1][2] = "108.6"
        recovered_rows[-1][3] = "107.6"
        recovered_rows[-1][4] = "108.2"
        recovered_rows[-1][7] = "130"
        recovered_rows[-1][10] = "68"
        recovered = self.analyze(
            recovered_rows,
            rows[-1][0] + 62_000,
            frozen_evidence=waiting.state_record.evidence_json,
        )
        self.assertTrue(recovered.passed, recovered.reason)
        self.assertEqual(
            recovered.state_record.evidence["qualified_observation"],
            first_observation,
        )

        rewritten = deepcopy(evolved)
        rewritten[-1][2] = "108.25"
        invalid = self.analyze(
            rewritten,
            rows[-1][0] + 61_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertFalse(invalid.passed)
        self.assertEqual(invalid.reason, "N16_FROZEN_EVIDENCE_INVALID")

        impossible_flow = deepcopy(rows)
        impossible_flow[-1][7] = "101"
        impossible_flow[-1][10] = "100"
        invalid_flow = self.analyze(
            impossible_flow,
            rows[-1][0] + 62_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertFalse(invalid_flow.passed)
        self.assertEqual(invalid_flow.reason, "N16_FROZEN_EVIDENCE_INVALID")

    def test_qualified_entry_survives_terminal_progression_and_is_strict(self):
        rows, checked = n16_klines(elapsed_ms=30_000)
        first = self.analyze(rows, checked)
        terminal_rows = deepcopy(rows)
        terminal_rows[-1][2] = "108.5"
        terminal_rows[-1][7] = "120"
        terminal_rows[-1][10] = "60"
        terminal = self.analyze(
            terminal_rows,
            rows[-1][0] + 120_000,
            frozen_evidence=first.state_record.evidence_json,
        )
        self.assertEqual(terminal.reason, "N16_ENTRY_WINDOW_EXPIRED")
        decoded = decode_n16_state_envelope(terminal.state_record.evidence)
        self.assertEqual(
            decoded.qualified_observation,
            first.state_record.evidence["qualified_observation"],
        )

        tampered = deepcopy(first.state_record.evidence)
        tampered["qualified_observation"]["elapsed_ms"] += 1
        resign_evidence(tampered)
        with self.assertRaisesRegex(ValueError, "qualified observation"):
            decode_n16_state_envelope(tampered)

    def test_trimmed_metric_seed_cannot_be_re_signed(self):
        rows, checked = n16_klines()
        first = self.analyze(rows, checked)
        forged = deepcopy(first.state_record.evidence)
        forged["metric_source"] = forged["metric_source"][1:]
        resign_evidence(forged)
        with self.assertRaisesRegex(ValueError, "envelope identity|fixed input"):
            decode_n16_state_envelope(forged)

    def test_final_depth_invalid_state_still_proves_first_valid_confirmation(self):
        rows, checked = n16_klines()
        rows = deepcopy(rows)
        rows[119][3] = "100"
        result = self.analyze(rows, checked)
        self.assertEqual(result.reason, "N16_PULLBACK_DEPTH_OUT_OF_RANGE")
        self.assertEqual(result.state_record.stage, "INVALID")
        self.assertEqual(result.structure.c.index, 120)
        decode_n16_state_envelope(result.state_record.evidence)

        decoded = decode_n16_state_envelope(result.state_record.evidence)
        candles = decoded.metric_source
        fake_c = candles[119]
        forged = deepcopy(result.state_record.evidence)
        summary = forged["summary"]
        forged_structure_id = _structure_id(
            "N16USDT",
            (
                result.structure.l1,
                result.structure.h1,
                result.structure.l2,
                result.structure.h2,
                result.structure.a,
                fake_c,
            ),
        )
        ema20 = _ema_series(candles, 20)
        ema50 = _ema_series(candles, 50)
        atr = _atr_series(candles, 14)
        h2_index = result.structure.h2.index
        p_value = min(item.low for item in candles[h2_index + 1 : 120])
        prior_volume = _median(item.quote_volume for item in candles[99:119])
        summary.update(
            {
                "c": fake_c.to_jsonable(False),
                "structure_id": forged_structure_id,
                "atr_c": str(atr[119]),
                "ema20_c": str(ema20[119]),
                "ema50_c": str(ema50[119]),
                "p": str(p_value),
                "pullback_depth": str(
                    (result.structure.h2.high - p_value)
                    / (
                        result.structure.h2.high
                        - result.structure.l2.low
                    )
                ),
                "c_close_location": str(
                    (fake_c.close - fake_c.low) / (fake_c.high - fake_c.low)
                ),
                "c_taker_buy_ratio": str(
                    fake_c.taker_buy_quote_volume / fake_c.quote_volume
                ),
                "c_volume_multiple": str(fake_c.quote_volume / prior_volume),
                "entry_min_price": str(fake_c.close),
                "entry_max_price": str(fake_c.close + Decimal("0.50") * atr[119]),
            }
        )
        forged["structure_id"] = forged_structure_id
        resign_evidence(forged)
        with self.assertRaisesRegex(ValueError, "frozen confirmation"):
            decode_n16_state_envelope(forged)

    def test_symbol_identity_rejects_subclasses_and_noncanonical_values(self):
        class HostileString(str):
            pass

        rows, checked = n16_klines()
        for value in (HostileString("N16USDT"), "n16USDT", " N16USDT"):
            with self.subTest(value=repr(value)), self.assertRaisesRegex(
                ValueError, "symbol"
            ):
                analyze_n16_mature_trend_support(
                    value,
                    rows,
                    quote_volume_rank=7,
                    checked_at_ms=checked,
                )


class N16RecorderSchedulerTests(unittest.TestCase):
    def make_recorder(self, root: str):
        database = Path(root) / "review.sqlite3"
        ledger = Path(root) / "n16_claim_ledger.sqlite3"
        if not database.exists():
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
        if not database.exists() or not ledger.exists():
            _install_n16_claim_boundary(database, ledger)
        _install_n17_lifecycle_boundary(database, ledger)
        _install_n19_lifecycle_boundary(database, ledger)
        _install_n18_lifecycle_boundary(database, ledger)
        _install_n20_lifecycle_boundary(database, ledger)
        _install_micro_lifecycle_boundary(database, ledger)
        _install_coverage_epoch_boundary(database, ledger)
        return ReviewRecorder(
            database,
            logging.getLogger("n16"),
            n16_claim_ledger_file=ledger,
        )

    def test_unicode_symbol_state_persists_and_reopens_without_identity_rewrite(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "龙虾USDT",
            rows,
            quote_volume_rank=74,
            checked_at_ms=checked,
        )
        self.assertTrue(analysis.passed, analysis.reason)
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(
                recorder.record_n16_state(analysis.state_record),
                "INSERTED",
            )
            reopened = ReviewRecorder(
                Path(root) / "review.sqlite3",
                logging.getLogger("n16-unicode-reopen"),
                n16_claim_ledger_file=(
                    Path(root) / "n16_claim_ledger.sqlite3"
                ),
            )
            states = reopened.get_active_n16_states()
            self.assertEqual(len(states), 1)
            self.assertEqual(states[0].symbol, "龙虾USDT")
            self.assertEqual(
                states[0].evidence_sha256,
                analysis.state_record.evidence_sha256,
            )

    def stage_n16_pass(self, root: str):
        rows, checked = n16_klines()
        recorder = self.make_recorder(root)
        recorder.upsert_strategy_definitions(load_all_strategies())
        scan_id = recorder.begin_scan(1, [candidate()], True)
        publish = recorder.publish_strategy_signal_batch
        recorder.publish_strategy_signal_batch = lambda *_args: False
        try:
            staged = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16-stage")
            ).evaluate(
                scan_id,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
        finally:
            recorder.publish_strategy_signal_batch = publish
        self.assertFalse(staged.signal_batch_published)
        self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")
        return recorder, scan_id, publish

    def publish_n16_claims(self, root: str, symbols):
        rows, checked = n16_klines()
        recorder = self.make_recorder(root)
        recorder.upsert_strategy_definitions(load_all_strategies())
        candidates = [
            candidate(symbol, index + 1)
            for index, symbol in enumerate(symbols)
        ]
        scan_id = recorder.begin_scan(len(candidates), candidates, True)
        scheduler = StrategyScheduler(
            (N16_STRATEGY,), 5, recorder, logging.getLogger("n16-multi")
        )
        signal_ids = []
        for item in candidates:
            analysis = analyze_n16_mature_trend_support(
                item.symbol,
                rows,
                quote_volume_rank=item.quote_volume_rank,
                checked_at_ms=checked,
            )
            self.assertTrue(analysis.passed, analysis.reason)
            self.assertIn(
                recorder.record_n16_state(analysis.state_record),
                {"INSERTED", "UNCHANGED"},
            )
            signal_ids.append(
                scheduler._record_signal(
                    scan_id,
                    StrategySignalDecision(
                        strategy=N16_STRATEGY,
                        candidate=item,
                        analysis=analysis,
                        passed=True,
                        decision="PASSED",
                        reason="PASSED",
                    ),
                )
            )
        self.assertTrue(all(type(value) is int for value in signal_ids))
        self.assertTrue(
            recorder.publish_strategy_signal_batch(scan_id, len(candidates))
        )
        return recorder, Path(recorder.db_file), signal_ids

    def test_round_scope_commits_n16_pair_before_next_short_write(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            item = candidate()
            scan_id = recorder.begin_scan(1, [item], True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16-round-pair")
            )
            analysis = analyze_n16_mature_trend_support(
                item.symbol,
                rows,
                quote_volume_rank=item.quote_volume_rank,
                checked_at_ms=checked,
            )
            self.assertTrue(analysis.passed, analysis.reason)

            with recorder.strategy_round_runtime_scope():
                self.assertEqual(
                    recorder.record_n16_state(analysis.state_record),
                    "INSERTED",
                )
                signal_id = scheduler._record_signal(
                    scan_id,
                    StrategySignalDecision(
                        strategy=N16_STRATEGY,
                        candidate=item,
                        analysis=analysis,
                        passed=True,
                        decision="PASSED",
                        reason="PASSED",
                    ),
                )
                self.assertIsInstance(signal_id, int)
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(scan_id, 1)
                )
                self.assertEqual(
                    recorder.n16_claim_ledger.metadata_phase(),
                    "READY",
                )
                self.assertTrue(
                    recorder.record_event("n16_round_pair_followup", {})
                )

            recorder.assert_n16_execution_ready()
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(),
                scan_id,
            )

    def strict_n16_live_state(
        self,
        signal_id: int,
        structure_id: str,
        *,
        dry_run: bool = False,
        opened_at: str = "2026-07-16T00:00:00+00:00",
    ) -> PositionState:
        context = {
            "strategy_id": "N16",
            "rule_version": "N16_V1",
            "signal_id": signal_id,
            "structure_id": structure_id,
        }
        plan = {
            "stop_mode": "trend_support_continuation_margin_capped",
            "structure_id": structure_id,
            "structure_context": context,
            "entry_price": "100",
            "stop_loss_price": "95",
            "take_profit_price": "125",
            "executed_quantity": "10",
            "final_protected_quantity": "10",
        }
        if dry_run:
            open_order = {
                "symbol": "N16USDT",
                "side": "BUY",
                "type": "MARKET",
                "origQty": "10",
                "clientOrderId": "mkt-n16-dry-recovery",
                "dryRun": True,
            }
            stop_order = {
                "symbol": "N16USDT",
                "type": "STOP_MARKET",
                "triggerPrice": "95",
                "clientAlgoId": "sl-n16-dry-recovery",
                "dryRun": True,
            }
            take_profit_order = {
                "symbol": "N16USDT",
                "type": "TAKE_PROFIT_MARKET",
                "triggerPrice": "125",
                "clientAlgoId": "tp-n16-dry-recovery",
                "dryRun": True,
            }
        else:
            open_order = {
                "orderId": 7001,
                "clientOrderId": "mkt-n16-recovery",
                "symbol": "N16USDT",
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
                "executedQty": "10",
            }
            stop_order = {
                "algoId": 7002,
                "clientAlgoId": "sl-n16-recovery",
                "symbol": "N16USDT",
                "algoType": "CONDITIONAL",
                "side": "SELL",
                "orderType": "STOP_MARKET",
                "closePosition": True,
                "algoStatus": "NEW",
            }
            take_profit_order = {
                "algoId": 7003,
                "clientAlgoId": "tp-n16-recovery",
                "symbol": "N16USDT",
                "algoType": "CONDITIONAL",
                "side": "SELL",
                "orderType": "TAKE_PROFIT_MARKET",
                "closePosition": True,
                "algoStatus": "NEW",
            }
        return PositionState(
            symbol="N16USDT",
            quantity="10",
            entry_price="100",
            stop_loss_price="95",
            take_profit_price="125",
            leverage=10,
            opened_at=opened_at,
            dry_run=dry_run,
            orders={
                "plan": deepcopy(plan),
                "pretrade_plan": deepcopy(plan),
                "open": open_order,
                "stop": stop_order,
                "take_profit": take_profit_order,
                "strategy": {
                    "strategy_id": "N16",
                    "signal_id": signal_id,
                    "structure_id": structure_id,
                },
            },
        )

    def publish_and_rotate_n16(self, root: str):
        rows, checked = n16_klines()
        database = Path(root) / "review.sqlite3"
        recorder = self.make_recorder(root)
        recorder.upsert_strategy_definitions(load_all_strategies())
        first_scan = recorder.begin_scan(1, [candidate()], True)
        first = StrategyScheduler(
            (N16_STRATEGY,), 5, recorder, logging.getLogger("n16-graph")
        ).evaluate(
            first_scan,
            {"quote_volume_top": [candidate()]},
            {"N16USDT": rows},
            checked_at_ms=checked,
        )
        self.assertTrue(first.signal_batch_published)
        structure_id = first.passed_signals[0].analysis.structure_id
        episode_id = first.passed_signals[0].analysis.structure.episode_id
        second_scan = recorder.begin_scan(1, [candidate("OTHERUSDT")], True)
        self.assertIsNotNone(
            recorder.record_strategy_signal(
                second_scan,
                "N01",
                "OTHERUSDT",
                "-0.02",
                (),
                "0",
                False,
                False,
                "REJECTED",
                "NO_MATCH",
                detail={"rotation": True},
            )
        )
        self.assertTrue(recorder.publish_strategy_signal_batch(second_scan, 1))
        with closing(sqlite3.connect(database)) as connection:
            counts = tuple(
                connection.execute(statement).fetchone()[0]
                for statement in (
                    "SELECT COUNT(*) FROM n16_trend_support_states",
                    "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                    "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'",
                    "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                    "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'",
                    "SELECT COUNT(*) FROM n16_consumption_seals",
                    "SELECT sealed_claim_count FROM n16_lifecycle_guard",
                    "SELECT COUNT(*) FROM n16_first_claim_witness",
                    "SELECT COUNT(*) FROM strategy_signals "
                    "WHERE strategy_id = 'N16'",
                )
            )
        self.assertEqual(counts, (1, 1, 1, 1, 1, 1, 0))
        return recorder, database, structure_id, episode_id, second_scan

    def test_state_round_trip_conflict_and_publish_consumes_atomically(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=7, checked_at_ms=checked
        )
        self.assertTrue(analysis.passed, analysis.reason)
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            result = recorder.record_n16_state(analysis.state_record)
            self.assertEqual(result, "INSERTED")
            self.assertEqual(recorder.record_n16_state(analysis.state_record), "UNCHANGED")
            state = recorder.get_n16_state("N16", analysis.state_record.episode_id)
            self.assertEqual(state.stage, "CONFIRMED")
            decoded = decode_n16_state_envelope(
                json.loads(state.evidence_json), "N16", "N16USDT"
            )
            self.assertEqual(decoded.structure_id, analysis.structure_id)

            forged = deepcopy(analysis.state_record.evidence)
            forged["summary"]["p"] = str(
                Decimal(forged["summary"]["p"]) - Decimal("0.01")
            )
            forged["canonical_sha256"] = "0" * 64
            self.assertEqual(
                recorder.record_n16_state(
                    analysis.state_record.with_evidence(forged)
                ),
                "N16_STATE_INCONSISTENT",
            )

            scan_id = recorder.begin_scan(1, [candidate()], True)
            scheduler = StrategyScheduler((N16_STRATEGY,), 5, recorder, logging.getLogger("n16"))
            scheduled = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertTrue(scheduled.signal_batch_published)
            self.assertEqual(
                recorder.get_n16_state("N16", analysis.state_record.episode_id).stage,
                "CONSUMED",
            )

    def test_active_claim_remains_bound_to_permanent_n16_lifecycle(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            scan_id = recorder.begin_scan(1, [candidate()], True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertTrue(
                recorder.publish_strategy_signal_batch(scan_id, 1)
            )
            structure_id = result.passed_signals[0].analysis.structure_id
            episode_id = result.passed_signals[0].analysis.structure.episode_id
            self.assertEqual(
                recorder.inspect_passed_structure(
                    "N16", "N16USDT", structure_id
                ),
                "CONSUMED",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT sealed_claim_count FROM n16_lifecycle_guard"
                    ).fetchone(),
                    (1,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*), MIN(seal_ordinal), "
                        "MIN(structure_id) FROM n16_first_claim_witness"
                    ).fetchone(),
                    (1, 1, structure_id),
                )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "N16 active audit is immutable"
                ):
                    connection.execute(
                        "UPDATE strategy_passed_signal_audits "
                        "SET detail_json = '{}' WHERE strategy_id = 'N16'"
                    )
                connection.rollback()
            restarted = ReviewRecorder(
                recorder.db_file,
                logging.getLogger("n16-restart"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                restarted.inspect_passed_structure(
                    "N16", "N16USDT", structure_id
                ),
                "CONSUMED",
            )
            self.assertEqual(
                restarted.get_n16_state("N16", episode_id).stage,
                "CONSUMED",
            )

    def test_active_paper_and_live_claims_are_exactly_cross_attested(self):
        for mode in ("paper", "live"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                recorder, database, signal_ids = self.publish_n16_claims(
                    root, ("N16USDT",)
                )
                with closing(sqlite3.connect(database)) as connection:
                    structure_id = connection.execute(
                        "SELECT structure_id "
                        "FROM n16_consumption_seals WHERE seal_ordinal=1"
                    ).fetchone()[0]
                if mode == "paper":
                    trade_id = recorder.open_strategy_paper_trade(
                        "N16",
                        "N16USDT",
                        "100",
                        "99",
                        "105",
                        "",
                        {"plan": {}},
                        {
                            "signal_id": signal_ids[0],
                            "structure_id": structure_id,
                        },
                    )
                    self.assertIsNotNone(trade_id)
                else:
                    state = self.strict_n16_live_state(
                        signal_ids[0], structure_id
                    )
                    claim = recorder.claim_strategy_live_open_audit(state, "N16")
                    self.assertIsNotNone(claim)
                    review_id = claim.trade_review_id
                recorder.assert_n16_execution_ready()

                with closing(sqlite3.connect(database)) as connection:
                    if mode == "paper":
                        changed = connection.execute(
                            "UPDATE strategy_paper_trades SET strategy_id='N15' "
                            "WHERE id=?",
                            (trade_id,),
                        )
                    else:
                        changed = connection.execute(
                            "UPDATE strategy_live_links SET strategy_id='N15' "
                            "WHERE trade_review_id=?",
                            (review_id,),
                        )
                    self.assertEqual(changed.rowcount, 1)
                    connection.commit()
                    reverse_marker_before = (
                        tuple(
                            connection.execute(
                                "SELECT * FROM strategy_paper_trades ORDER BY id"
                            ).fetchall()
                        ),
                        tuple(
                            connection.execute(
                                "SELECT * FROM strategy_live_links ORDER BY id"
                            ).fetchall()
                        ),
                        tuple(
                            connection.execute(
                                "SELECT * FROM trade_reviews ORDER BY id"
                            ).fetchall()
                        ),
                    )
                with self.assertRaises(RuntimeError):
                    recorder.assert_n16_execution_ready()
                with closing(sqlite3.connect(database)) as connection:
                    reverse_marker_after = (
                        tuple(
                            connection.execute(
                                "SELECT * FROM strategy_paper_trades ORDER BY id"
                            ).fetchall()
                        ),
                        tuple(
                            connection.execute(
                                "SELECT * FROM strategy_live_links ORDER BY id"
                            ).fetchall()
                        ),
                        tuple(
                            connection.execute(
                                "SELECT * FROM trade_reviews ORDER BY id"
                            ).fetchall()
                        ),
                    )
                    if mode == "paper":
                        restored = connection.execute(
                            "UPDATE strategy_paper_trades SET strategy_id='N16' "
                            "WHERE id=?",
                            (trade_id,),
                        )
                    else:
                        restored = connection.execute(
                            "UPDATE strategy_live_links SET strategy_id='N16' "
                            "WHERE trade_review_id=?",
                            (review_id,),
                        )
                    self.assertEqual(restored.rowcount, 1)
                    connection.commit()
                self.assertEqual(reverse_marker_after, reverse_marker_before)
                recorder.assert_n16_execution_ready()

                with closing(sqlite3.connect(database)) as connection:
                    if mode == "paper":
                        changed = connection.execute(
                            "UPDATE strategy_paper_trades SET detail_json=? "
                            "WHERE id=?",
                            (
                                json.dumps(
                                    {
                                        "signal_id": signal_ids[0],
                                        "structure_id": "f" * 24,
                                    }
                                ),
                                trade_id,
                            ),
                        )
                    else:
                        changed = connection.execute(
                            "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                            (
                                json.dumps(
                                    {
                                        "strategy": {
                                            "strategy_id": "N16",
                                            "signal_id": signal_ids[0],
                                            "structure_id": "f" * 24,
                                        }
                                    }
                                ),
                                review_id,
                            ),
                        )
                    self.assertEqual(changed.rowcount, 1)
                    connection.commit()
                with self.assertRaises(RuntimeError):
                    recorder.assert_n16_execution_ready()
                if mode == "live":
                    with closing(sqlite3.connect(database)) as connection:
                        changed = connection.execute(
                            "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                            (
                                json.dumps(
                                    {
                                        "strategy": {
                                            "strategy_id": "N15",
                                            "signal_id": signal_ids[0],
                                            "structure_id": structure_id,
                                        }
                                    }
                                ),
                                review_id,
                            ),
                        )
                        self.assertEqual(changed.rowcount, 1)
                        connection.commit()
                    with self.assertRaises(RuntimeError):
                        recorder.assert_n16_execution_ready()

    def test_recovered_live_claim_wrapper_is_exactly_cross_attested(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, database, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            with closing(sqlite3.connect(database)) as connection:
                structure_id = connection.execute(
                    "SELECT structure_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=1"
                ).fetchone()[0]
            state = PositionState(
                symbol="N16USDT",
                quantity="10",
                entry_price="100",
                stop_loss_price="95",
                take_profit_price="125",
                leverage=10,
                opened_at="2026-07-16T00:00:00+00:00",
                dry_run=False,
                orders={
                    "plan": {
                        "executed_quantity": "10",
                        "final_protected_quantity": "10",
                    },
                    "open": {
                        "orderId": 7001,
                        "clientOrderId": "mkt-n16-recovery",
                        "symbol": "N16USDT",
                        "side": "BUY",
                        "type": "MARKET",
                        "status": "FILLED",
                        "executedQty": "10",
                    },
                    "stop": {
                        "algoId": 7002,
                        "clientAlgoId": "sl-n16-recovery",
                        "symbol": "N16USDT",
                        "algoType": "CONDITIONAL",
                        "side": "SELL",
                        "orderType": "STOP_MARKET",
                        "closePosition": True,
                        "algoStatus": "NEW",
                    },
                    "take_profit": {
                        "algoId": 7003,
                        "clientAlgoId": "tp-n16-recovery",
                        "symbol": "N16USDT",
                        "algoType": "CONDITIONAL",
                        "side": "SELL",
                        "orderType": "TAKE_PROFIT_MARKET",
                        "closePosition": True,
                        "algoStatus": "NEW",
                    },
                    "strategy": {
                        "strategy_id": "N16",
                        "signal_id": signal_ids[0],
                        "structure_id": structure_id,
                    },
                },
            )
            claim = recorder.claim_strategy_live_open_audit(state, "N16")
            self.assertIsNotNone(claim)
            self.assertTrue(claim.review_created)
            self.assertTrue(claim.link_created)
            recorder.assert_n16_execution_ready()

            with closing(sqlite3.connect(database)) as connection:
                original_raw = connection.execute(
                    "SELECT orders_json FROM trade_reviews WHERE id=?",
                    (claim.trade_review_id,),
                ).fetchone()[0]
                wrapped = json.loads(original_raw)
                self.assertEqual(wrapped.keys(), {"state_orders"})
            orders_json = json.dumps(
                wrapped["state_orders"],
                sort_keys=True,
                separators=(",", ":"),
            )
            canonical_wrapper = (
                '{"state_orders":' + orders_json + "}"
            )
            signal_token = '"signal_id":' + str(signal_ids[0])
            self.assertIn(signal_token, canonical_wrapper)
            ledger_path = Path(recorder.n16_claim_ledger_file)

            def logical_snapshot():
                with closing(sqlite3.connect(database)) as connection:
                    review_rows = tuple(
                        connection.execute(
                            "SELECT * FROM trade_reviews ORDER BY id"
                        ).fetchall()
                    )
                    link_rows = tuple(
                        connection.execute(
                            "SELECT * FROM strategy_live_links ORDER BY id"
                        ).fetchall()
                    )
                    event_rows = tuple(
                        connection.execute(
                            "SELECT * FROM events ORDER BY id"
                        ).fetchall()
                    )
                with closing(sqlite3.connect(ledger_path)) as connection:
                    ledger_rows = tuple(
                        connection.execute(
                            "SELECT * FROM n16_permanent_claims ORDER BY claim_ordinal"
                        ).fetchall()
                    )
                    metadata_rows = tuple(
                        connection.execute(
                            "SELECT * FROM n16_claim_ledger_meta"
                        ).fetchall()
                    )
                return review_rows, link_rows, event_rows, ledger_rows, metadata_rows

            invalid_payloads = {
                "wrong_strategy": canonical_wrapper.replace(
                    '"strategy_id":"N16"',
                    '"strategy_id":"N15"',
                    1,
                ),
                "wrong_signal": canonical_wrapper.replace(
                    signal_token,
                    '"signal_id":999999',
                    1,
                ),
                "wrong_structure": canonical_wrapper.replace(
                    '"structure_id":"' + structure_id + '"',
                    '"structure_id":"' + ("f" * 24) + '"',
                    1,
                ),
                "ambiguous_top_level": (
                    '{"state_orders":'
                    + orders_json
                    + ',"strategy":'
                    + json.dumps(
                        wrapped["state_orders"]["strategy"],
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "}"
                ),
                "duplicate_wrapper": (
                    '{"state_orders":'
                    + orders_json
                    + ',"state_orders":'
                    + orders_json
                    + "}"
                ),
                "duplicate_signal_id": canonical_wrapper.replace(
                    signal_token,
                    '"signal_id":999999,' + signal_token,
                    1,
                ),
            }
            for mode, invalid_raw in invalid_payloads.items():
                with self.subTest(mode=mode):
                    with closing(sqlite3.connect(database)) as connection:
                        changed = connection.execute(
                            "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                            (invalid_raw, claim.trade_review_id),
                        )
                        self.assertEqual(changed.rowcount, 1)
                        connection.commit()
                    before = logical_snapshot()
                    with self.assertRaises(RuntimeError):
                        recorder.assert_n16_execution_ready()
                    self.assertEqual(logical_snapshot(), before)

            with closing(sqlite3.connect(database)) as connection:
                changed = connection.execute(
                    "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                    (original_raw, claim.trade_review_id),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()
            recorder.assert_n16_execution_ready()

            pending_trade_id = recorder.record_trade_close(
                state,
                "LIVE_RESULT_PENDING",
                "",
                "",
                "",
                "",
                "",
            )
            self.assertEqual(pending_trade_id, claim.trade_review_id)
            recorder.assert_n16_execution_ready()
            with closing(sqlite3.connect(database)) as connection:
                pending_raw = connection.execute(
                    "SELECT orders_json FROM trade_reviews WHERE id=?",
                    (claim.trade_review_id,),
                ).fetchone()[0]
            pending_payload = json.loads(pending_raw)
            self.assertEqual(set(pending_payload), {"state_orders", "close"})
            invalid_pending_payloads = {
                "missing_close": json.dumps(
                    {"state_orders": pending_payload["state_orders"]},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "wrong_close_reason": json.dumps(
                    {
                        "state_orders": pending_payload["state_orders"],
                        "close": {
                            **pending_payload["close"],
                            "exit_reason": "STOP_LOSS",
                        },
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "nonempty_close_exit_price": json.dumps(
                    {
                        "state_orders": pending_payload["state_orders"],
                        "close": {
                            **pending_payload["close"],
                            "exit_price": "123",
                        },
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "nonempty_close_pnl": json.dumps(
                    {
                        "state_orders": pending_payload["state_orders"],
                        "close": {
                            **pending_payload["close"],
                            "pnl_amount": "999",
                        },
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "extra_wrapper_key": json.dumps(
                    {**pending_payload, "unexpected": {}},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
            for mode, invalid_raw in invalid_pending_payloads.items():
                with self.subTest(mode="pending_" + mode):
                    with closing(sqlite3.connect(database)) as connection:
                        changed = connection.execute(
                            "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                            (invalid_raw, claim.trade_review_id),
                        )
                        self.assertEqual(changed.rowcount, 1)
                        connection.commit()
                    before = logical_snapshot()
                    with self.assertRaises(RuntimeError):
                        recorder.assert_n16_execution_ready()
                    self.assertEqual(logical_snapshot(), before)
                    with closing(sqlite3.connect(database)) as connection:
                        restored = connection.execute(
                            "UPDATE trade_reviews SET orders_json=? WHERE id=?",
                            (pending_raw, claim.trade_review_id),
                        )
                        self.assertEqual(restored.rowcount, 1)
                        connection.commit()
                    recorder.assert_n16_execution_ready()
            for column, invalid_value in (
                ("exit_price", "123"),
                ("realized_pnl", "999"),
            ):
                with self.subTest(mode="pending_review_" + column):
                    with closing(sqlite3.connect(database)) as connection:
                        changed = connection.execute(
                            "UPDATE trade_reviews SET " + column + "=? WHERE id=?",
                            (invalid_value, claim.trade_review_id),
                        )
                        self.assertEqual(changed.rowcount, 1)
                        connection.commit()
                    before = logical_snapshot()
                    with self.assertRaises(RuntimeError):
                        recorder.assert_n16_execution_ready()
                    self.assertEqual(logical_snapshot(), before)
                    with closing(sqlite3.connect(database)) as connection:
                        restored = connection.execute(
                            "UPDATE trade_reviews SET " + column + "='' WHERE id=?",
                            (claim.trade_review_id,),
                        )
                        self.assertEqual(restored.rowcount, 1)
                        connection.commit()
                    recorder.assert_n16_execution_ready()

    def test_active_live_link_and_review_identity_is_exact(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, database, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            with closing(sqlite3.connect(database)) as connection:
                structure_id = connection.execute(
                    "SELECT structure_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=1"
                ).fetchone()[0]
            state = self.strict_n16_live_state(
                signal_ids[0], structure_id
            )
            claim = recorder.claim_strategy_live_open_audit(state, "N16")
            self.assertIsNotNone(claim)
            review_id = claim.trade_review_id
            self.assertTrue(claim.link_created)
            with closing(sqlite3.connect(database)) as connection:
                link_rows = connection.execute(
                    "SELECT id,opened_at FROM strategy_live_links "
                    "WHERE trade_review_id=? ORDER BY id",
                    (review_id,),
                ).fetchall()
            self.assertEqual(len(link_rows), 1)
            link_id, opened_at = link_rows[0]
            self.assertEqual(opened_at, state.opened_at)
            recorder.assert_n16_execution_ready()

            with closing(sqlite3.connect(database)) as connection:
                changed = connection.execute(
                    "UPDATE trade_reviews SET dry_run=1 WHERE id=?",
                    (review_id,),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()
            recorder.assert_n16_execution_ready()
            with closing(sqlite3.connect(database)) as connection:
                changed = connection.execute(
                    "UPDATE trade_reviews SET dry_run=0 WHERE id=?",
                    (review_id,),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()

            ledger_path = Path(recorder.n16_claim_ledger_file)

            def logical_snapshot():
                with closing(sqlite3.connect(database)) as connection:
                    review_rows = tuple(
                        connection.execute(
                            "SELECT * FROM trade_reviews ORDER BY id"
                        ).fetchall()
                    )
                    link_rows = tuple(
                        connection.execute(
                            "SELECT * FROM strategy_live_links ORDER BY id"
                        ).fetchall()
                    )
                with closing(sqlite3.connect(ledger_path)) as connection:
                    ledger_rows = tuple(
                        connection.execute(
                            "SELECT * FROM n16_permanent_claims ORDER BY claim_ordinal"
                        ).fetchall()
                    )
                    metadata_rows = tuple(
                        connection.execute(
                            "SELECT * FROM n16_claim_ledger_meta"
                        ).fetchall()
                    )
                return review_rows, link_rows, ledger_rows, metadata_rows

            cases = (
                (
                    "review_symbol",
                    "UPDATE trade_reviews SET symbol=? WHERE id=?",
                    ("OTHERUSDT", review_id),
                    "UPDATE trade_reviews SET symbol=? WHERE id=?",
                    (state.symbol, review_id),
                ),
                (
                    "review_opened_at",
                    "UPDATE trade_reviews SET opened_at=? WHERE id=?",
                    ("2026-07-16T00:15:00+00:00", review_id),
                    "UPDATE trade_reviews SET opened_at=? WHERE id=?",
                    (opened_at, review_id),
                ),
                (
                    "link_symbol",
                    "UPDATE strategy_live_links SET symbol=? WHERE id=?",
                    ("OTHERUSDT", link_id),
                    "UPDATE strategy_live_links SET symbol=? WHERE id=?",
                    (state.symbol, link_id),
                ),
                (
                    "link_opened_at",
                    "UPDATE strategy_live_links SET opened_at=? WHERE id=?",
                    ("2026-07-16T00:15:00+00:00", link_id),
                    "UPDATE strategy_live_links SET opened_at=? WHERE id=?",
                    (opened_at, link_id),
                ),
                (
                    "terminal_review_with_active_link",
                    "UPDATE trade_reviews SET status='CLOSED_STOP_LOSS', "
                    "closed_at=?,exit_reason='STOP_LOSS' WHERE id=?",
                    ("2026-07-16T01:00:00+00:00", review_id),
                    "UPDATE trade_reviews SET status='OPENED',closed_at=NULL,"
                    "exit_reason=NULL WHERE id=?",
                    (review_id,),
                ),
                (
                    "review_dry_run_out_of_range",
                    "UPDATE trade_reviews SET dry_run=2 WHERE id=?",
                    (review_id,),
                    "UPDATE trade_reviews SET dry_run=0 WHERE id=?",
                    (review_id,),
                ),
                (
                    "review_dry_run_text",
                    "UPDATE trade_reviews SET dry_run='invalid' WHERE id=?",
                    (review_id,),
                    "UPDATE trade_reviews SET dry_run=0 WHERE id=?",
                    (review_id,),
                ),
            )
            for mode, mutate_sql, mutate_args, restore_sql, restore_args in cases:
                with self.subTest(mode=mode):
                    with closing(sqlite3.connect(database)) as connection:
                        changed = connection.execute(mutate_sql, mutate_args)
                        self.assertEqual(changed.rowcount, 1)
                        connection.commit()
                    before = logical_snapshot()
                    with self.assertRaises(RuntimeError):
                        recorder.assert_n16_execution_ready()
                    self.assertEqual(logical_snapshot(), before)
                    with closing(sqlite3.connect(database)) as connection:
                        restored = connection.execute(restore_sql, restore_args)
                        self.assertEqual(restored.rowcount, 1)
                        connection.commit()
                    recorder.assert_n16_execution_ready()

    def test_real_trader_dry_run_live_orders_are_exactly_attested(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT",
            rows,
            quote_volume_rank=1,
            checked_at_ms=checked,
        )
        self.assertTrue(analysis.passed, analysis.reason)
        structure = analysis.structure
        self.assertIsNotNone(structure)

        class ForbiddenReconcileTrader:
            def __init__(self):
                self.calls = 0

            def close_dry_run_position_if_triggered(self):
                self.calls += 1
                raise AssertionError("invalid N16 dry-run state reached close")

            def sync_state_with_exchange(self):
                self.calls += 1
                raise AssertionError("invalid N16 dry-run state reached sync")

        class ForbiddenPaper:
            def __init__(self):
                self.calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.calls += 1
                raise AssertionError("invalid N16 dry-run state reached paper")

        with tempfile.TemporaryDirectory() as root:
            recorder, _database, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            config = test_config(str(Path(root) / "dry_run_account.json"))
            client = BinanceFuturesClient(
                config,
                logging.getLogger("n16-real-dry-run-client"),
            )
            client._exchange_info = {
                "symbols": [
                    {
                        "symbol": "N16USDT",
                        "contractType": "PERPETUAL",
                        "filters": [
                            {
                                "filterType": "PRICE_FILTER",
                                "tickSize": "0.01",
                            },
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "0.001",
                                "maxQty": "100000",
                                "stepSize": "0.001",
                            },
                            {
                                "filterType": "MARKET_LOT_SIZE",
                                "minQty": "0.001",
                                "maxQty": "100000",
                                "stepSize": "0.001",
                            },
                            {
                                "filterType": "MIN_NOTIONAL",
                                "notional": "5",
                            },
                        ],
                    }
                ]
            }
            state_store = StateStore(Path(root) / "position.json")
            trader = Trader(
                client,
                config,
                state_store,
                logging.getLogger("n16-real-dry-run-trader"),
                clock_ms=lambda: checked,
            )
            plan = trader.build_trend_support_continuation_margin_capped_trade_plan(
                "N16USDT",
                structure.entry.close,
                structure.p,
                N16_STRATEGY.risk_reward_ratio,
                structure_id=structure.structure_id,
                entry_min_price=structure.entry_min_price,
                entry_max_price=structure.entry_max_price,
            )
            plan = replace(
                plan,
                entry_candle_open_time_ms=structure.entry.open_time_ms,
                entry_deadline_ms=(
                    structure.entry.open_time_ms
                    + N16_STRATEGY.entry_window_seconds * 1000
                ),
                structure_context={
                    "strategy_id": "N16",
                    "rule_version": "N16_V1",
                    "signal_id": signal_ids[0],
                    "structure_id": structure.structure_id,
                },
            )
            state = trader.open_long_plan_with_protection(plan)
            self.assertTrue(state.dry_run)
            self.assertEqual(
                set(state.orders["open"]),
                {
                    "symbol",
                    "side",
                    "type",
                    "origQty",
                    "clientOrderId",
                    "dryRun",
                },
            )
            self.assertEqual(
                set(state.orders["stop"]),
                {"symbol", "type", "triggerPrice", "clientAlgoId", "dryRun"},
            )
            self.assertEqual(
                set(state.orders["take_profit"]),
                {"symbol", "type", "triggerPrice", "clientAlgoId", "dryRun"},
            )
            client_ids = {
                state.orders["open"]["clientOrderId"],
                state.orders["stop"]["clientAlgoId"],
                state.orders["take_profit"]["clientAlgoId"],
            }
            self.assertEqual(len(client_ids), 3)
            claim = recorder.claim_strategy_live_open_audit(state, "N16")
            self.assertIsNotNone(claim)
            review_id = claim.trade_review_id
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                query_plans = {
                    "link_key": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT id FROM "
                        "strategy_live_links INDEXED BY "
                        "idx_n16_live_link_state_key "
                        "WHERE symbol=? AND opened_at=? ORDER BY id LIMIT 2",
                        (state.symbol, state.opened_at),
                    ).fetchall(),
                    "review_key": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT id FROM trade_reviews "
                        "INDEXED BY idx_n16_trade_review_state_key "
                        "WHERE symbol=? AND opened_at=? ORDER BY id LIMIT 2",
                        (state.symbol, state.opened_at),
                    ).fetchall(),
                    "review_link": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT id FROM "
                        "strategy_live_links INDEXED BY "
                        "idx_n16_live_link_review WHERE trade_review_id=? "
                        "ORDER BY id LIMIT 2",
                        (review_id,),
                    ).fetchall(),
                    "active_review": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT id,symbol,opened_at,dry_run,"
                        "status,closed_at,exit_reason,exit_price,"
                        "close_mark_price,realized_pnl,realized_pnl_pct,"
                        "balance_after_close,orders_json FROM trade_reviews "
                        "INDEXED BY idx_n16_review_pending_rows WHERE status "
                        "IN ('OPENED','CLOSED_LIVE_RESULT_PENDING') AND id>0 "
                        "ORDER BY id"
                    ).fetchall(),
                    "open_paper": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT strategy_id,symbol,detail_json "
                        "FROM strategy_paper_trades INDEXED BY "
                        "idx_n16_paper_open_rows WHERE result='OPEN' AND id>0 "
                        "ORDER BY id"
                    ).fetchall(),
                    "open_live": connection.execute(
                        "EXPLAIN QUERY PLAN SELECT link.id,link.strategy_id,"
                        "link.trade_review_id,link.symbol,link.opened_at,"
                        "link.result,review.id,review.symbol,review.opened_at,"
                        "review.dry_run,review.status,review.closed_at,"
                        "review.exit_reason,review.exit_price,"
                        "review.close_mark_price,review.realized_pnl,"
                        "review.realized_pnl_pct,review.balance_after_close,"
                        "review.orders_json FROM strategy_live_links AS link "
                        "INDEXED BY idx_n16_live_open_rows LEFT JOIN "
                        "trade_reviews AS review ON review.id="
                        "link.trade_review_id WHERE link.closed_at IS NULL "
                        "AND link.id>0 ORDER BY link.id"
                    ).fetchall(),
                }
            expected_indexes = {
                "link_key": "idx_n16_live_link_state_key",
                "review_key": "idx_n16_trade_review_state_key",
                "review_link": "idx_n16_live_link_review",
                "active_review": "idx_n16_review_pending_rows",
                "open_paper": "idx_n16_paper_open_rows",
                "open_live": "idx_n16_live_open_rows",
            }
            for label, plan_rows in query_plans.items():
                details = " | ".join(str(row[3]) for row in plan_rows)
                self.assertIn(expected_indexes[label], details)
                upper_details = details.upper()
                self.assertIn("SEARCH", upper_details)
                self.assertNotIn("SCAN ", upper_details)
                self.assertNotIn("AUTOMATIC", upper_details)
                self.assertNotIn("TEMP", upper_details)
            self.assertEqual(
                recorder.inspect_strategy_live_finalization(
                    state,
                    "N16",
                    allow_dry_run_pending=True,
                ).status,
                "PENDING",
            )
            bot = TradingBot.__new__(TradingBot)
            bot.config = config
            bot.logger = logging.getLogger("n16-real-dry-run-main")
            bot.client = client
            bot.state = state_store
            bot.recorder = recorder
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = SimpleNamespace()
            self.assertTrue(bot._n16_local_execution_state_ready(state))

            def mutate_open_extra(orders):
                orders["open"]["status"] = "FILLED"

            def mutate_open_quantity(orders):
                orders["open"]["origQty"] = "0.001"

            def mutate_stop_trigger(orders):
                orders["stop"]["triggerPrice"] = "1"

            def mutate_client_identity(orders):
                orders["stop"]["clientAlgoId"] = orders["open"][
                    "clientOrderId"
                ]

            def mutate_plan_quantity(orders):
                orders["plan"]["executed_quantity"] = "0.001"

            mutations = {
                "open_extra_real_field": mutate_open_extra,
                "open_quantity": mutate_open_quantity,
                "stop_trigger": mutate_stop_trigger,
                "duplicate_client_id": mutate_client_identity,
                "plan_executed_quantity": mutate_plan_quantity,
            }
            for label, mutator in mutations.items():
                with self.subTest(label=label):
                    orders = deepcopy(state.orders)
                    mutator(orders)
                    tampered = replace(state, orders=orders)
                    state_store.save(tampered)
                    forbidden_trader = ForbiddenReconcileTrader()
                    forbidden_paper = ForbiddenPaper()
                    bot.trader = forbidden_trader
                    bot.paper_trader = forbidden_paper
                    self.assertIsNone(
                        bot._reconcile_multi_strategy_after_publication()
                    )
                    self.assertEqual(forbidden_trader.calls, 0)
                    self.assertEqual(forbidden_paper.calls, 0)

            state_store.clear()
            forbidden_trader = ForbiddenReconcileTrader()
            forbidden_paper = ForbiddenPaper()
            bot.trader = forbidden_trader
            bot.paper_trader = forbidden_paper
            self.assertIsNone(bot._reconcile_multi_strategy_after_publication())
            self.assertEqual(forbidden_trader.calls, 0)
            self.assertEqual(forbidden_paper.calls, 0)

            wiped_orders = deepcopy(state.orders)
            replacement_structure = "f" * 24
            wiped_orders["strategy"] = {
                "strategy_id": "N15",
                "signal_id": 999_999,
                "structure_id": replacement_structure,
            }
            for plan_key in ("plan", "pretrade_plan"):
                wiped_orders[plan_key]["stop_mode"] = (
                    "breadth_recovery_margin_capped"
                )
                wiped_orders[plan_key]["structure_id"] = replacement_structure
                wiped_orders[plan_key]["structure_context"] = {
                    "strategy_id": "N15",
                    "signal_id": 999_999,
                    "structure_id": replacement_structure,
                }
            wiped = replace(state, orders=wiped_orders)
            state_store.save(wiped)
            self.assertIsNone(bot._reconcile_multi_strategy_after_publication())
            self.assertEqual(forbidden_trader.calls, 0)
            self.assertEqual(forbidden_paper.calls, 0)
            state_store.save(state)

            with closing(sqlite3.connect(recorder.db_file)) as connection:
                changed = connection.execute(
                    "UPDATE strategy_live_links SET symbol='OTHERUSDT' "
                    "WHERE trade_review_id=?",
                    (review_id,),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()
            wiped_orders = deepcopy(wiped.orders)
            state_store.save(replace(wiped, orders=wiped_orders))
            self.assertIsNone(bot._reconcile_multi_strategy_after_publication())
            self.assertEqual(forbidden_trader.calls, 0)
            self.assertEqual(forbidden_paper.calls, 0)
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                restored = connection.execute(
                    "UPDATE strategy_live_links SET symbol=? "
                    "WHERE trade_review_id=?",
                    (state.symbol, review_id),
                )
                self.assertEqual(restored.rowcount, 1)
                changed = connection.execute(
                    "UPDATE trade_reviews SET symbol='OTHERUSDT' WHERE id=?",
                    (review_id,),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()
            self.assertIsNone(bot._reconcile_multi_strategy_after_publication())
            self.assertEqual(forbidden_trader.calls, 0)
            self.assertEqual(forbidden_paper.calls, 0)
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                restored = connection.execute(
                    "UPDATE trade_reviews SET symbol=? WHERE id=?",
                    (state.symbol, review_id),
                )
                self.assertEqual(restored.rowcount, 1)
                connection.commit()
            state_store.save(state)

    def test_n16_terminal_event_trigger_accepts_real_writers_and_rejects_drift(self):
        for writer, dry_run in (
            ("record_trade_close", False),
            ("atomic_finalizer", False),
            ("atomic_finalizer", True),
        ):
            with self.subTest(writer=writer, dry_run=dry_run), tempfile.TemporaryDirectory() as root:
                recorder, database, signal_ids = self.publish_n16_claims(
                    root, ("N16USDT",)
                )
                with closing(sqlite3.connect(database)) as connection:
                    structure_id = connection.execute(
                        "SELECT structure_id FROM n16_consumption_seals "
                        "WHERE seal_ordinal=1"
                    ).fetchone()[0]
                state = self.strict_n16_live_state(
                    signal_ids[0], structure_id, dry_run=dry_run
                )
                claim = recorder.claim_strategy_live_open_audit(state, "N16")
                self.assertIsNotNone(claim)
                if writer == "record_trade_close":
                    result = recorder.record_trade_close(
                        state,
                        "TAKE_PROFIT",
                        "125",
                        "125",
                        "25",
                        "25",
                        "1025",
                        {"source": "record-close"},
                    )
                    self.assertEqual(result, claim.trade_review_id)
                else:
                    result = recorder.finalize_strategy_live_result(
                        state,
                        "N16",
                        "WIN",
                        "TAKE_PROFIT",
                        "125",
                        {"source": "atomic-finalizer"},
                        datetime.now(timezone.utc) + timedelta(minutes=15),
                        mark_price="125",
                        pnl_amount="25",
                        pnl_pct="25",
                        balance_after="1025",
                        _allow_dry_run=dry_run,
                    )
                    self.assertIsNotNone(result)
                    self.assertEqual(result.trade_id, claim.trade_review_id)
                with closing(sqlite3.connect(database)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT event_type,trade_review_id FROM events "
                            "WHERE trade_review_id=?",
                            (claim.trade_review_id,),
                        ).fetchall(),
                        [
                            (
                                "n16_dry_run_position_closed"
                                if dry_run
                                else "n16_live_position_closed",
                                claim.trade_review_id,
                            )
                        ],
                    )

        with tempfile.TemporaryDirectory() as root:
            recorder, database, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            with closing(sqlite3.connect(database)) as connection:
                structure_id = connection.execute(
                    "SELECT structure_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=1"
                ).fetchone()[0]
            state = self.strict_n16_live_state(signal_ids[0], structure_id)
            claim = recorder.claim_strategy_live_open_audit(state, "N16")
            self.assertIsNotNone(claim)
            close_time = datetime.now(timezone.utc).isoformat()
            close_payload = {
                "exit_reason": "TAKE_PROFIT",
                "exit_price": "125",
                "mark_price": "125",
                "pnl_amount": "25",
                "pnl_pct": "25",
                "balance_after": "1025",
                "resolution_detail": {"nested": {"code": "ok"}},
            }
            canonical = lambda value: json.dumps(
                value, sort_keys=True, separators=(",", ":")
            )
            with closing(sqlite3.connect(database)) as connection:
                orders = json.loads(
                    connection.execute(
                        "SELECT orders_json FROM trade_reviews WHERE id=?",
                        (claim.trade_review_id,),
                    ).fetchone()[0]
                )
                orders["close"] = close_payload
                connection.execute(
                    "UPDATE trade_reviews SET status='CLOSED_TAKE_PROFIT',"
                    "closed_at=?,exit_reason='TAKE_PROFIT',exit_price='125',"
                    "close_mark_price='125',realized_pnl='25',"
                    "realized_pnl_pct='25',balance_after_close='1025',"
                    "orders_json=? WHERE id=?",
                    (close_time, canonical(orders), claim.trade_review_id),
                )
                connection.execute(
                    "INSERT INTO events(occurred_at,event_type,symbol,payload_json) "
                    "VALUES(?,?,?,?)",
                    (close_time, "ordinary_event", state.symbol, "{}"),
                )
                connection.commit()
                event_payload = {
                    **close_payload,
                    "trade_id": claim.trade_review_id,
                }
                invalid_payloads = {}
                for key in (
                    "trade_id",
                    "exit_reason",
                    "exit_price",
                    "mark_price",
                    "pnl_amount",
                    "pnl_pct",
                    "balance_after",
                ):
                    invalid = deepcopy(event_payload)
                    del invalid[key]
                    invalid_payloads["missing_" + key] = canonical(invalid)
                without_detail = deepcopy(event_payload)
                del without_detail["resolution_detail"]
                invalid_payloads["missing_resolution_detail"] = canonical(
                    without_detail
                )
                changed_detail = deepcopy(event_payload)
                changed_detail["resolution_detail"]["nested"]["code"] = "other"
                invalid_payloads["different_resolution_detail"] = canonical(
                    changed_detail
                )
                extra_detail = deepcopy(event_payload)
                extra_detail["unexpected"] = "value"
                invalid_payloads["extra_top_level_key"] = canonical(extra_detail)
                nested_duplicate = canonical(event_payload).replace(
                    '"nested":{"code":"ok"}',
                    '"nested":{"code":"ok","code":"ok"}',
                )
                self.assertNotEqual(nested_duplicate, canonical(event_payload))
                invalid_payloads["nested_duplicate_key"] = nested_duplicate
                for label, raw_payload in invalid_payloads.items():
                    with self.subTest(label=label):
                        with self.assertRaises(sqlite3.IntegrityError):
                            connection.execute(
                                "INSERT INTO events(occurred_at,event_type,symbol,"
                                "payload_json,trade_review_id) VALUES(?,?,?,?,?)",
                                (
                                    close_time,
                                    "n16_live_position_closed",
                                    state.symbol,
                                    raw_payload,
                                    claim.trade_review_id,
                                ),
                            )
                        connection.rollback()
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM events "
                                "WHERE trade_review_id=?",
                                (claim.trade_review_id,),
                            ).fetchone(),
                            (0,),
                        )
                connection.execute(
                    "INSERT INTO events(occurred_at,event_type,symbol,payload_json,"
                    "trade_review_id) VALUES(?,?,?,?,?)",
                    (
                        close_time,
                        "n16_live_position_closed",
                        state.symbol,
                        canonical(event_payload),
                        claim.trade_review_id,
                    ),
                )
                connection.commit()
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM events WHERE trade_review_id=?",
                        (claim.trade_review_id,),
                    ).fetchone(),
                    (1,),
                )

    def test_n16_missing_and_review_only_audits_repair_before_side_effects(self):
        class ForbiddenSideEffects:
            def __init__(self):
                self.calls = 0

            def close_dry_run_position_if_triggered(self, *_args, **_kwargs):
                self.calls += 1
                raise AssertionError("recovery reached dry-run side effects")

            def sync_state_with_exchange(self, *_args, **_kwargs):
                self.calls += 1
                raise AssertionError("recovery reached exchange side effects")

        class ForbiddenPaper:
            def __init__(self):
                self.calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.calls += 1
                raise AssertionError("recovery reached paper side effects")

        for dry_run in (False, True):
            for crash_edge in ("MISSING_OPEN_AUDIT", "REVIEW_ONLY_OPENED"):
                with self.subTest(
                    dry_run=dry_run, crash_edge=crash_edge
                ), tempfile.TemporaryDirectory() as root:
                    recorder, database, signal_ids = self.publish_n16_claims(
                        root, ("N16USDT",)
                    )
                    with closing(sqlite3.connect(database)) as connection:
                        structure_id = connection.execute(
                            "SELECT structure_id FROM n16_consumption_seals "
                            "WHERE seal_ordinal=1"
                        ).fetchone()[0]
                    state = self.strict_n16_live_state(
                        signal_ids[0],
                        structure_id,
                        dry_run=dry_run,
                    )
                    state_store = StateStore(Path(root) / "position.json")
                    state_store.save(state)
                    if crash_edge == "REVIEW_ONLY_OPENED":
                        review_id = recorder.record_trade_open(None, state)
                        self.assertIsNotNone(review_id)
                    self.assertEqual(
                        recorder.inspect_strategy_live_finalization(
                            state,
                            "N16",
                            allow_dry_run_pending=True,
                        ).status,
                        crash_edge,
                    )
                    bot = TradingBot.__new__(TradingBot)
                    bot.config = test_config(
                        str(Path(root) / "dry_run_account.json")
                    )
                    bot.logger = logging.getLogger("n16-live-audit-repair")
                    bot.recorder = recorder
                    bot.state = state_store
                    bot.strategies = (N16_STRATEGY,)
                    bot.trader = ForbiddenSideEffects()
                    bot.paper_trader = ForbiddenPaper()
                    self.assertTrue(bot._n16_local_execution_state_ready(state))
                    repaired, reloaded = (
                        bot._repair_n16_live_audit_before_reconciliation(state)
                    )
                    self.assertTrue(repaired)
                    self.assertEqual(reloaded, state)
                    self.assertEqual(
                        recorder.n16_active_live_claim_identity(),
                        (signal_ids[0], "N16USDT", structure_id),
                    )
                    self.assertEqual(
                        recorder.inspect_strategy_live_finalization(
                            state,
                            "N16",
                            allow_dry_run_pending=True,
                        ).status,
                        "PENDING",
                    )
                    with closing(sqlite3.connect(database)) as connection:
                        reviews = connection.execute(
                            "SELECT id,dry_run,status FROM trade_reviews "
                            "WHERE symbol=? AND opened_at=?",
                            (state.symbol, state.opened_at),
                        ).fetchall()
                        links = connection.execute(
                            "SELECT strategy_id,trade_review_id,symbol,opened_at "
                            "FROM strategy_live_links WHERE symbol=? "
                            "AND opened_at=?",
                            (state.symbol, state.opened_at),
                        ).fetchall()
                    self.assertEqual(len(reviews), 1)
                    self.assertEqual(
                        reviews[0][1:], (int(dry_run), "OPENED")
                    )
                    self.assertEqual(
                        links,
                        [
                            (
                                "N16",
                                reviews[0][0],
                                state.symbol,
                                state.opened_at,
                            )
                        ],
                    )
                    self.assertEqual(bot.trader.calls, 0)
                    self.assertEqual(bot.paper_trader.calls, 0)

        with tempfile.TemporaryDirectory() as root:
            recorder, database, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            with closing(sqlite3.connect(database)) as connection:
                structure_id = connection.execute(
                    "SELECT structure_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=1"
                ).fetchone()[0]
            state = self.strict_n16_live_state(
                signal_ids[0], structure_id, dry_run=True
            )
            state_store = StateStore(Path(root) / "position.json")
            state_store.save(state)
            review_id = recorder.record_trade_open(None, state)
            self.assertIsNotNone(review_id)
            bot = TradingBot.__new__(TradingBot)
            bot.config = test_config(str(Path(root) / "dry_run_account.json"))
            bot.logger = logging.getLogger("n16-live-audit-block")
            bot.recorder = recorder
            bot.state = state_store
            bot.strategies = (N16_STRATEGY,)
            bot.trader = ForbiddenSideEffects()
            bot.paper_trader = ForbiddenPaper()

            class ForbiddenMonitor:
                def __init__(self):
                    self.calls = 0

                def scan(self, *_args, **_kwargs):
                    self.calls += 1
                    raise AssertionError("repair failure reached single scan")

                def scan_for_strategies(self, *_args, **_kwargs):
                    self.calls += 1
                    raise AssertionError("repair failure reached multi scan")

            bot.monitor = ForbiddenMonitor()

            with patch.object(
                recorder,
                "claim_strategy_live_open_audit",
                return_value=None,
            ):
                self.assertIsNone(
                    bot._reconcile_multi_strategy_after_publication()
                )
                bot._run_once_single_strategy()
                bot._run_once_multi_strategy()
            self.assertEqual(bot.trader.calls, 0)
            self.assertEqual(bot.paper_trader.calls, 0)
            self.assertEqual(bot.monitor.calls, 0)
            self.assertEqual(state_store.load(), state)
            with closing(sqlite3.connect(database)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone(),
                    (0,),
                )

            state_store.clear()
            self.assertFalse(bot._n16_local_execution_state_ready())
            wiped = deepcopy(state.orders)
            wiped["strategy"] = {
                "strategy_id": "N15",
                "signal_id": 999999,
                "structure_id": "f" * 24,
            }
            for key in ("plan", "pretrade_plan"):
                wiped[key]["stop_mode"] = "breadth_recovery_margin_capped"
                wiped[key]["structure_id"] = "f" * 24
                wiped[key]["structure_context"] = {
                    "strategy_id": "N15",
                    "signal_id": 999999,
                    "structure_id": "f" * 24,
                }
            state_store.save(replace(state, orders=wiped))
            self.assertFalse(bot._n16_local_execution_state_ready())

    def test_non_n16_review_only_null_structure_remains_recoverable(self):
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            state = self.strict_n16_live_state(1, "a" * 24)
            orders = deepcopy(state.orders)
            orders["strategy"] = {
                "strategy_id": "N15",
                "signal_id": None,
                "structure_id": None,
            }
            for key in ("plan", "pretrade_plan"):
                orders[key]["stop_mode"] = "breadth_recovery_margin_capped"
                orders[key]["structure_id"] = None
                orders[key]["structure_context"] = {
                    "strategy_id": "N15",
                    "signal_id": None,
                    "structure_id": None,
                }
            state = replace(state, orders=orders)
            review_id = recorder.record_trade_open(None, state)
            self.assertIsNotNone(review_id)
            self.assertFalse(
                recorder.n16_live_link_marker_for_state(
                    state.symbol, state.opened_at
                )
            )
            bot = TradingBot.__new__(TradingBot)
            bot.logger = logging.getLogger("non-n16-review-only")
            bot.recorder = recorder
            bot.state = StateStore(Path(root) / "position.json")
            bot.state.save(state)
            self.assertTrue(bot._n16_local_execution_state_ready(state))
            claim = recorder.claim_strategy_live_open_audit(state, "N15")
            self.assertIsNotNone(claim)
            self.assertEqual(claim.trade_review_id, review_id)
            self.assertTrue(claim.link_created)

    def test_independent_claim_ledger_is_ready_and_required_before_review_write(self):
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            db = Path(root) / "review.sqlite3"
            claim_ledger = Path(recorder.n16_claim_ledger_file)
            self.assertTrue(claim_ledger.is_file())
            self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")

            checkpoint_delete_mode(db)
            checkpoint_delete_mode(claim_ledger)
            moved = claim_ledger.with_suffix(".withheld")
            claim_ledger.replace(moved)
            try:
                before = zero_write_database_fingerprint(db)
                with self.assertRaisesRegex(RuntimeError, "claim ledger is missing"):
                    ReviewRecorder(
                        db,
                        logging.getLogger("n16-anchor-missing"),
                        n16_claim_ledger_file=claim_ledger,
                    )
                self.assertEqual(zero_write_database_fingerprint(db), before)
            finally:
                moved.replace(claim_ledger)

            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-anchor-restored"),
                n16_claim_ledger_file=claim_ledger,
            )
            self.assertEqual(restarted.n16_claim_ledger.metadata_phase(), "READY")

    def test_prepared_publication_exact_rollback_is_safely_aborted(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)
            with patch.object(
                recorder,
                "_advance_n16_claim_chain",
                side_effect=RuntimeError("review activation rejected"),
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")
            self.assertIsNone(recorder.n16_claim_ledger.prepared_publication())
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_consumption_seals"
                    ).fetchone(),
                    (0,),
                )

    def test_prepared_publication_commit_ack_loss_requires_safe_commit_resolution(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)
            original_connect = recorder._connect
            acknowledgement_lost = {"raised": False}

            @contextmanager
            def commit_then_raise():
                with original_connect() as connection:
                    yield connection
                if not acknowledgement_lost["raised"]:
                    acknowledgement_lost["raised"] = True
                    raise RuntimeError("review commit acknowledgement unavailable")

            with patch.object(recorder, "_connect", commit_then_raise):
                self.assertFalse(publish(scan_id, 1))
            self.assertTrue(acknowledgement_lost["raised"])
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            self.assertIsNotNone(
                recorder.n16_claim_ledger.prepared_publication()
            )
            resolution, summary = _resolve_n16_publication_boundary(
                Path(recorder.db_file), Path(recorder.n16_claim_ledger_file)
            )
            self.assertEqual(resolution, "SAFE_COMMIT")
            self.assertEqual(summary.confirmed_claim_count, 1)
            self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")

    def test_prepare_publication_write_failure_leaves_review_and_ledger_unpublished(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)
            with patch.object(
                recorder.n16_claim_ledger,
                "prepare_publication",
                side_effect=RuntimeError("prepare write rejected"),
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "READY"
            )
            self.assertIsNone(
                recorder.n16_claim_ledger.prepared_publication()
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_consumption_seals"
                    ).fetchone(),
                    (0,),
                )

    def test_prepare_publication_commit_ack_loss_preserves_prepared_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)
            original_prepare = recorder.n16_claim_ledger.prepare_publication
            acknowledgement_lost = []

            def prepare_then_raise(*args, **kwargs):
                prepared = original_prepare(*args, **kwargs)
                acknowledgement_lost.append(prepared)
                raise RuntimeError("prepare acknowledgement unavailable")

            with patch.object(
                recorder.n16_claim_ledger,
                "prepare_publication",
                side_effect=prepare_then_raise,
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(len(acknowledgement_lost), 1)
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            self.assertIsNotNone(
                recorder.n16_claim_ledger.prepared_publication()
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM strategy_signal_batches "
                        "WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    ("STAGING",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_consumption_seals"
                    ).fetchone(),
                    (0,),
                )

    def test_ledger_commit_ack_loss_keeps_the_committed_pair(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)
            original_commit = recorder.n16_claim_ledger.commit_prepared
            acknowledgement_lost = []

            def commit_then_raise(*args, **kwargs):
                original_commit(*args, **kwargs)
                acknowledgement_lost.append(True)
                raise RuntimeError("ledger commit acknowledgement unavailable")

            with patch.object(
                recorder.n16_claim_ledger,
                "commit_prepared",
                side_effect=commit_then_raise,
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(acknowledgement_lost, [True])
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "READY"
            )
            self.assertIsNone(
                recorder.n16_claim_ledger.prepared_publication()
            )
            with recorder._read_only_runtime_snapshot() as connection:
                summary = _n16_review_claim_summary(connection)
                current = connection.execute(
                    "SELECT current_scan_id FROM strategy_signal_current"
                ).fetchone()
            self.assertEqual(current, (scan_id,))
            self.assertEqual(summary.confirmed_claim_count, 1)
            recorder.n16_claim_ledger.attest(summary)
            self.assertTrue(publish(scan_id, 1))

    def test_ambiguous_publication_blocks_same_and_next_round_side_effects(self):
        rows, checked = n16_klines()

        class CountingMonitor:
            def __init__(self):
                self.calls = 0

            def scan_for_strategies(self, _top_n):
                self.calls += 1
                return StrategyMarketScan(1, [], [candidate()])

        class CountingClient:
            def __init__(self):
                self.kline_calls = 0
                self.interval_calls = 0
                self.aggregate_calls = 0

            def get_klines(self, _symbol):
                self.kline_calls += 1
                return deepcopy(rows)

            def get_klines_for_interval(self, *_args, **_kwargs):
                self.interval_calls += 1
                return []

            def get_aggregate_trades(self, *_args, **_kwargs):
                self.aggregate_calls += 1
                return []

        class CountingTrader:
            def __init__(self):
                self.close_calls = 0
                self.sync_calls = 0
                self.plan_calls = 0
                self.open_calls = 0

            def close_dry_run_position_if_triggered(self):
                self.close_calls += 1
                return None

            def sync_state_with_exchange(self):
                self.sync_calls += 1
                return SyncResult(has_position=False)

            def build_trend_support_continuation_margin_capped_trade_plan(
                self, *_args, **_kwargs
            ):
                self.plan_calls += 1
                raise AssertionError("unresolved publication built a plan")

            def open_long_plan_with_protection(self, _plan):
                self.open_calls += 1
                raise AssertionError("unresolved publication opened live")

        class CountingPaperTrader:
            def __init__(self):
                self.close_calls = 0
                self.open_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                return []

            def open_trade(self, *_args, **_kwargs):
                self.open_calls += 1
                raise AssertionError("unresolved publication opened paper")

        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions((N16_STRATEGY,))
            monitor = CountingMonitor()
            client = CountingClient()
            trader = CountingTrader()
            paper = CountingPaperTrader()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("n16-ambiguous-main")
            bot.client = client
            bot.state = StateStore(Path(root) / "position.json")
            bot.recorder = recorder
            bot.monitor = monitor
            bot.trader = trader
            bot.paper_trader = paper
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = StrategyScheduler(
                bot.strategies, 5, recorder, bot.logger
            )

            with patch.object(
                recorder.n16_claim_ledger,
                "commit_prepared",
                side_effect=RuntimeError("confirmation acknowledgement unavailable"),
            ), patch("trading_bot.main.time.time", return_value=checked / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            self.assertEqual(monitor.calls, 1)
            self.assertEqual(client.kline_calls, 1)
            self.assertEqual(
                (
                    trader.close_calls,
                    trader.sync_calls,
                    trader.plan_calls,
                    trader.open_calls,
                    paper.close_calls,
                    paper.open_calls,
                    client.interval_calls,
                    client.aggregate_calls,
                ),
                (0, 0, 0, 0, 0, 0, 0, 0),
            )

            bot._run_once_multi_strategy()
            self.assertEqual(monitor.calls, 1)
            self.assertEqual(
                (
                    trader.close_calls,
                    trader.sync_calls,
                    trader.plan_calls,
                    trader.open_calls,
                    paper.close_calls,
                    paper.open_calls,
                ),
                (0, 0, 0, 0, 0, 0),
            )

    def test_local_n16_pending_claim_is_attested_before_exchange_sync(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, _db, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )

            class ForbiddenMonitor:
                def __init__(self):
                    self.calls = 0

                def scan_for_strategies(self, _top_n):
                    self.calls += 1
                    raise AssertionError("invalid local claim reached market scan")

            class ForbiddenTrader:
                def __init__(self):
                    self.close_calls = 0
                    self.sync_calls = 0

                def close_dry_run_position_if_triggered(self):
                    self.close_calls += 1
                    raise AssertionError("invalid local claim reached cleanup")

                def sync_state_with_exchange(self):
                    self.sync_calls += 1
                    raise AssertionError("invalid local claim reached exchange sync")

            class ForbiddenPaper:
                def __init__(self):
                    self.close_calls = 0

                def close_triggered_open_trades(self, *_args, **_kwargs):
                    self.close_calls += 1
                    raise AssertionError("invalid local claim reached paper cleanup")

            state_store = StateStore(Path(root) / "position.json")
            wrong_plan = TradePlan(
                symbol="N16USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("99"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("10"),
                balance=Decimal("1000"),
                stop_mode="trend_support_continuation_margin_capped",
                structure_id="f" * 24,
                structure_context={
                    "strategy_id": "N16",
                    "signal_id": signal_ids[0],
                    "structure_id": "f" * 24,
                },
            )
            Trader(
                SimpleNamespace(),
                test_config(str(Path(root) / "pending-account.json")),
                state_store,
                logging.getLogger("n16-local-wrong-claim-writer"),
            )._save_market_order_execution_pending(
                wrong_plan,
                "mkt-n16-wrong-claim",
                RuntimeError("submission acknowledgement unavailable"),
                [],
                phase="MARKET_ORDER_SUBMITTING",
            )
            monitor = ForbiddenMonitor()
            trader = ForbiddenTrader()
            paper = ForbiddenPaper()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=False)
            bot.logger = logging.getLogger("n16-local-claim-gate")
            bot.client = SimpleNamespace()
            bot.state = state_store
            bot.recorder = recorder
            bot.monitor = monitor
            bot.trader = trader
            bot.paper_trader = paper
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = SimpleNamespace()

            bot._run_once_multi_strategy()
            self.assertEqual(monitor.calls, 0)
            self.assertEqual(trader.close_calls, 0)
            self.assertEqual(trader.sync_calls, 0)
            self.assertEqual(paper.close_calls, 0)

    def test_local_n16_redundant_identity_conflicts_block_before_side_effects(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=1, checked_at_ms=checked
        )
        self.assertTrue(analysis.passed, analysis.reason)

        class ForbiddenMonitor:
            def __init__(self):
                self.calls = 0

            def scan_for_strategies(self, _top_n):
                self.calls += 1
                raise AssertionError("identity conflict reached market scan")

        class ForbiddenTrader:
            def __init__(self):
                self.close_calls = 0
                self.sync_calls = 0

            def close_dry_run_position_if_triggered(self, **_kwargs):
                self.close_calls += 1
                raise AssertionError("identity conflict reached cleanup")

            def sync_state_with_exchange(self, **_kwargs):
                self.sync_calls += 1
                raise AssertionError("identity conflict reached exchange sync")

        class ForbiddenPaper:
            def __init__(self):
                self.close_calls = 0
                self.open_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                raise AssertionError("identity conflict reached paper cleanup")

            def open_trade(self, *_args, **_kwargs):
                self.open_calls += 1
                raise AssertionError("identity conflict reached paper open")

        def context_structure_conflict(orders):
            orders["plan"]["structure_context"]["structure_id"] = "f" * 24

        def context_bool_signal(orders):
            orders["plan"]["structure_context"]["signal_id"] = True

        def context_float_signal(orders):
            orders["plan"]["structure_context"]["signal_id"] = 1.0

        def phase_bool_signal(orders):
            orders["execution_pending"]["signal_id"] = True

        def phase_float_signal(orders):
            orders["execution_pending"]["signal_id"] = 1.0

        def pretrade_plan_conflict(orders):
            orders.pop("execution_pending")
            orders["pretrade_plan"] = deepcopy(orders["plan"])
            orders["pretrade_plan"]["structure_context"][
                "structure_id"
            ] = "f" * 24
            orders["open"] = {}
            orders["stop"] = {}
            orders["take_profit"] = {}

        def multiple_phase_conflict(orders):
            orders["emergency_cleanup_pending"] = deepcopy(
                orders["execution_pending"]
            )

        def incomplete_final_state(orders):
            orders.pop("execution_pending")

        def phase_with_final_orders(orders):
            orders["pretrade_plan"] = deepcopy(orders["plan"])
            orders["open"] = {}
            orders["stop"] = {}
            orders["take_profit"] = {}

        def reverse_n16_markers(orders, *, signal_value=None):
            orders["strategy"]["strategy_id"] = "N15"
            if signal_value is not None:
                orders["strategy"]["signal_id"] = signal_value
            for key in (
                "plan",
                "pretrade_plan",
                "execution_pending",
                "execution_cleanup_resolved",
                "emergency_cleanup_pending",
            ):
                payload = orders.get(key)
                if type(payload) is not dict:
                    continue
                payload["strategy_id"] = "N15"
                payload["stop_mode"] = "not_n16"
                if signal_value is not None and "signal_id" in payload:
                    payload["signal_id"] = signal_value
                context = payload.get("structure_context")
                if type(context) is dict:
                    context["strategy_id"] = "N15"
                    if signal_value is not None:
                        context["signal_id"] = signal_value

        def reverse_pending_markers(orders):
            reverse_n16_markers(orders)

        def reverse_pending_bool_signal(orders):
            reverse_n16_markers(orders, signal_value=True)

        def reverse_final_markers(orders):
            orders.pop("execution_pending")
            orders["pretrade_plan"] = deepcopy(orders["plan"])
            orders["open"] = {}
            orders["stop"] = {}
            orders["take_profit"] = {}
            reverse_n16_markers(orders)

        def dry_run_integer(_orders):
            return {"dry_run": 0}

        def leverage_boolean(_orders):
            return {"leverage": True}

        def quantity_integer(_orders):
            return {"quantity": 1}

        def quantity_negative(_orders):
            return {"quantity": "-1"}

        def quantity_nonfinite(_orders):
            return {"quantity": "NaN"}

        def quantity_noncanonical(_orders):
            return {"quantity": "1.0"}

        def opened_at_noncanonical(_orders):
            return {"opened_at": "2026-07-16T08:00:00+08:00"}

        mutations = {
            "context_structure": context_structure_conflict,
            "context_signal_bool": context_bool_signal,
            "context_signal_float": context_float_signal,
            "phase_signal_bool": phase_bool_signal,
            "phase_signal_float": phase_float_signal,
            "pretrade_plan": pretrade_plan_conflict,
            "multiple_phases": multiple_phase_conflict,
            "incomplete_final": incomplete_final_state,
            "phase_with_final": phase_with_final_orders,
            "reverse_pending": reverse_pending_markers,
            "reverse_pending_bool_signal": reverse_pending_bool_signal,
            "reverse_final": reverse_final_markers,
            "dry_run_integer": dry_run_integer,
            "leverage_boolean": leverage_boolean,
            "quantity_integer": quantity_integer,
            "quantity_negative": quantity_negative,
            "quantity_nonfinite": quantity_nonfinite,
            "quantity_noncanonical": quantity_noncanonical,
            "opened_at_noncanonical": opened_at_noncanonical,
        }
        for name, mutate_orders in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                recorder, _db, signal_ids = self.publish_n16_claims(
                    root, ("N16USDT",)
                )
                state_store = StateStore(Path(root) / "position.json")
                plan = TradePlan(
                    symbol="N16USDT",
                    leverage=10,
                    quantity=Decimal("1"),
                    entry_price=Decimal("100"),
                    stop_loss_price=Decimal("99"),
                    take_profit_price=Decimal("105"),
                    stop_loss_pct=Decimal("0.01"),
                    take_profit_pct=Decimal("0.05"),
                    amplitude_24h_pct=Decimal("0"),
                    high_24h_price=Decimal("101"),
                    low_24h_price=Decimal("99"),
                    risk_amount=Decimal("1"),
                    notional_value=Decimal("100"),
                    required_margin=Decimal("10"),
                    balance=Decimal("1000"),
                    stop_mode="trend_support_continuation_margin_capped",
                    structure_id=analysis.structure_id,
                    structure_context={
                        "strategy_id": "N16",
                        "signal_id": signal_ids[0],
                        "structure_id": analysis.structure_id,
                    },
                )
                Trader(
                    SimpleNamespace(),
                    test_config(str(Path(root) / "pending-account.json")),
                    state_store,
                    logging.getLogger("n16-local-redundant-writer"),
                )._save_market_order_execution_pending(
                    plan,
                    "mkt-n16-redundant",
                    RuntimeError("submission acknowledgement unavailable"),
                    [],
                    phase="MARKET_ORDER_SUBMITTING",
                )
                pending_state = state_store.load()
                changed_orders = deepcopy(pending_state.orders)
                state_changes = mutate_orders(changed_orders) or {}
                state_store.save(
                    replace(
                        pending_state,
                        orders=changed_orders,
                        **state_changes,
                    )
                )
                state_file = Path(state_store.path)
                before_state = state_file.read_bytes()

                monitor = ForbiddenMonitor()
                trader = ForbiddenTrader()
                paper = ForbiddenPaper()
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=False)
                bot.logger = logging.getLogger(
                    "n16-local-redundant-%s" % name
                )
                bot.client = SimpleNamespace()
                bot.state = state_store
                bot.recorder = recorder
                bot.monitor = monitor
                bot.trader = trader
                bot.paper_trader = paper
                bot.strategies = (N16_STRATEGY,)
                bot.strategy_scheduler = SimpleNamespace()

                bot._run_once_multi_strategy()
                self.assertEqual(
                    (
                        monitor.calls,
                        trader.close_calls,
                        trader.sync_calls,
                        paper.close_calls,
                        paper.open_calls,
                    ),
                    (0, 0, 0, 0, 0),
                )
                self.assertEqual(state_file.read_bytes(), before_state)

    def test_n16_pre_submit_pending_persists_signal_and_reaches_sync_when_attested(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT",
            rows,
            quote_volume_rank=1,
            checked_at_ms=checked,
        )
        self.assertTrue(analysis.passed, analysis.reason)

        class CountingMonitor:
            def __init__(self):
                self.calls = 0

            def scan_for_strategies(self, _top_n):
                self.calls += 1
                return StrategyMarketScan(1, [], [candidate()])

        class CountingClient:
            def __init__(self):
                self.kline_calls = 0

            def get_klines(self, _symbol):
                self.kline_calls += 1
                return deepcopy(rows)

            def get_klines_for_interval(self, *_args, **_kwargs):
                return []

            def get_aggregate_trades(self, *_args, **_kwargs):
                return []

        class CountingTrader:
            def __init__(self):
                self.close_calls = 0
                self.sync_calls = 0

            def close_dry_run_position_if_triggered(self):
                self.close_calls += 1
                return None

            def sync_state_with_exchange(self):
                self.sync_calls += 1
                return SyncResult(has_position=True)

        class CountingPaper:
            def __init__(self):
                self.close_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                return []

        with tempfile.TemporaryDirectory() as root:
            recorder, _db, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            state_store = StateStore(Path(root) / "position.json")
            state_writer = Trader(
                SimpleNamespace(),
                test_config(str(Path(root) / "account.json")),
                state_store,
                logging.getLogger("n16-pre-submit-state"),
            )
            plan = TradePlan(
                symbol="N16USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("99"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("10"),
                balance=Decimal("1000"),
                stop_mode="trend_support_continuation_margin_capped",
                structure_id=analysis.structure_id,
                structure_context={
                    "strategy_id": "N16",
                    "signal_id": signal_ids[0],
                    "structure_id": analysis.structure_id,
                },
            )
            state_writer._save_market_order_execution_pending(
                plan,
                "mkt-n16-test",
                RuntimeError("submission acknowledgement unavailable"),
                [],
                phase="MARKET_ORDER_SUBMITTING",
            )
            pending_state = state_store.load()
            self.assertEqual(
                pending_state.orders["strategy"],
                {
                    "strategy_id": "N16",
                    "structure_id": analysis.structure_id,
                    "signal_id": signal_ids[0],
                },
            )

            monitor = CountingMonitor()
            client = CountingClient()
            trader = CountingTrader()
            paper = CountingPaper()
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=False)
            bot.logger = logging.getLogger("n16-pre-submit-main")
            bot.client = client
            bot.state = state_store
            bot.recorder = recorder
            bot.monitor = monitor
            bot.trader = trader
            bot.paper_trader = paper
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = StrategyScheduler(
                bot.strategies, 5, recorder, bot.logger
            )

            with patch("trading_bot.main.time.time", return_value=checked / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(monitor.calls, 1)
            self.assertEqual(client.kline_calls, 1)
            self.assertEqual(trader.close_calls, 1)
            self.assertEqual(trader.sync_calls, 1)
            self.assertEqual(paper.close_calls, 0)

    def test_n16_cleanup_resolved_zero_quantity_is_valid_without_exchange_io(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=1, checked_at_ms=checked
        )
        self.assertTrue(analysis.passed, analysis.reason)

        class NoExchangeClient:
            def __init__(self):
                self.calls = 0

            def __getattr__(self, name):
                self.calls += 1
                raise AssertionError(
                    "cleanup-resolved state reached exchange method %s" % name
                )

        class ForbiddenMonitor:
            def __init__(self):
                self.calls = 0

            def scan_for_strategies(self, _top_n):
                self.calls += 1
                raise AssertionError("cleanup-resolved state reached scan")

        class ForbiddenPaper:
            def __init__(self):
                self.close_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                raise AssertionError("cleanup-resolved state reached paper")

        with tempfile.TemporaryDirectory() as root:
            recorder, _db, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            state_store = StateStore(Path(root) / "position.json")
            client = NoExchangeClient()
            config = test_config(str(Path(root) / "account.json"))
            trader = Trader(
                client,
                config,
                state_store,
                logging.getLogger("n16-cleanup-resolved-writer"),
            )
            plan = TradePlan(
                symbol="N16USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("99"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("10"),
                balance=Decimal("1000"),
                stop_mode="trend_support_continuation_margin_capped",
                structure_id=analysis.structure_id,
                structure_context={
                    "strategy_id": "N16",
                    "signal_id": signal_ids[0],
                    "structure_id": analysis.structure_id,
                },
            )
            trader._save_execution_cleanup_resolved(
                plan,
                "TEST_CLEANUP_RESOLVED",
                {},
                {},
                [],
            )
            self.assertEqual(state_store.load().quantity, "0")
            bot = TradingBot.__new__(TradingBot)
            bot.config = config
            bot.logger = logging.getLogger("n16-cleanup-resolved-main")
            bot.client = client
            bot.state = state_store
            bot.recorder = recorder
            bot.monitor = ForbiddenMonitor()
            bot.trader = trader
            bot.paper_trader = ForbiddenPaper()
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = SimpleNamespace()
            self.assertIsNone(bot._reconcile_multi_strategy_after_publication())
            self.assertEqual(client.calls, 0)
            self.assertEqual(bot.monitor.calls, 0)
            self.assertEqual(bot.paper_trader.close_calls, 0)
            self.assertIsNone(state_store.load())

    def test_n16_local_state_change_immediately_before_sync_is_zero_side_effect(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT",
            rows,
            quote_volume_rank=1,
            checked_at_ms=checked,
        )
        self.assertTrue(analysis.passed, analysis.reason)

        class NoExchangeClient:
            def __init__(self):
                self.calls = 0

            def __getattr__(self, name):
                self.calls += 1
                raise AssertionError(
                    "changed local state reached exchange method %s" % name
                )

        class CountingPaper:
            def __init__(self):
                self.close_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                raise AssertionError("changed local state reached paper cleanup")

        class MutatingTrader(Trader):
            def __init__(self, *args, replacement_state, **kwargs):
                super().__init__(*args, **kwargs)
                self.replacement_state = replacement_state

            def close_dry_run_position_if_triggered(
                self, *, expected_local_state, defer_settlement=False
            ):
                self._load_expected_local_state(expected_local_state)
                return None

            def sync_state_with_exchange(self, *, expected_local_state):
                self.state.save(self.replacement_state)
                return super().sync_state_with_exchange(
                    expected_local_state=expected_local_state
                )

        with tempfile.TemporaryDirectory() as root:
            recorder, _db, signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            state_store = StateStore(Path(root) / "position.json")
            plan = TradePlan(
                symbol="N16USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("99"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("10"),
                balance=Decimal("1000"),
                stop_mode="trend_support_continuation_margin_capped",
                structure_id=analysis.structure_id,
                structure_context={
                    "strategy_id": "N16",
                    "signal_id": signal_ids[0],
                    "structure_id": analysis.structure_id,
                },
            )
            writer = Trader(
                SimpleNamespace(),
                test_config(str(Path(root) / "writer-account.json")),
                state_store,
                logging.getLogger("n16-state-toctou-writer"),
            )
            writer._save_market_order_execution_pending(
                plan,
                "mkt-n16-toctou",
                RuntimeError("submission acknowledgement unavailable"),
                [],
                phase="MARKET_ORDER_SUBMITTING",
            )
            original_state = state_store.load()
            self.assertIsNotNone(original_state)
            for mode, replacement_value in (
                ("structure", "f" * 24),
                ("signal_bool", True),
                ("signal_float", 1.0),
            ):
                with self.subTest(mode=mode):
                    state_store.save(original_state)
                    changed_orders = deepcopy(original_state.orders)
                    if mode == "structure":
                        changed_orders["strategy"]["structure_id"] = (
                            replacement_value
                        )
                        changed_orders["plan"]["structure_id"] = (
                            replacement_value
                        )
                        changed_orders["plan"]["structure_context"][
                            "structure_id"
                        ] = replacement_value
                        changed_orders["execution_pending"][
                            "structure_id"
                        ] = replacement_value
                    else:
                        changed_orders["strategy"]["signal_id"] = (
                            replacement_value
                        )
                        changed_orders["plan"]["structure_context"][
                            "signal_id"
                        ] = replacement_value
                        changed_orders["execution_pending"]["signal_id"] = (
                            replacement_value
                        )
                    replacement_state = replace(
                        original_state,
                        orders=changed_orders,
                    )
                    client = NoExchangeClient()
                    trader = MutatingTrader(
                        client,
                        test_config(str(Path(root) / (mode + "-account.json"))),
                        state_store,
                        logging.getLogger("n16-state-toctou-" + mode),
                        replacement_state=replacement_state,
                    )
                    paper = CountingPaper()
                    bot = TradingBot.__new__(TradingBot)
                    bot.config = SimpleNamespace(dry_run=False)
                    bot.logger = logging.getLogger(
                        "n16-state-toctou-main-" + mode
                    )
                    bot.client = client
                    bot.state = state_store
                    bot.recorder = recorder
                    bot.trader = trader
                    bot.paper_trader = paper

                    self.assertIsNone(
                        bot._reconcile_multi_strategy_after_publication()
                    )
                    self.assertEqual(client.calls, 0)
                    self.assertEqual(paper.close_calls, 0)

    def test_prepared_publication_unknown_result_is_never_guessed(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)

            @contextmanager
            def unavailable_snapshot():
                raise RuntimeError("review classification unavailable")
                yield

            with patch.object(
                recorder,
                "_advance_n16_claim_chain",
                side_effect=RuntimeError("review activation rejected"),
            ), patch.object(
                recorder,
                "_read_only_runtime_snapshot",
                unavailable_snapshot,
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            resolution, summary = _resolve_n16_publication_boundary(
                Path(recorder.db_file), Path(recorder.n16_claim_ledger_file)
            )
            self.assertEqual(resolution, "SAFE_ABORT")
            self.assertEqual(summary.confirmed_claim_count, 0)
            self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")

    def test_prepared_abort_requires_exact_full_staging_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, scan_id, publish = self.stage_n16_pass(root)

            @contextmanager
            def unavailable_snapshot():
                raise RuntimeError("review classification unavailable")
                yield

            with patch.object(
                recorder,
                "_advance_n16_claim_chain",
                side_effect=RuntimeError("review activation rejected"),
            ), patch.object(
                recorder,
                "_read_only_runtime_snapshot",
                unavailable_snapshot,
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                changed = connection.execute(
                    "UPDATE strategy_signal_batches SET updated_at=updated_at||'x' "
                    "WHERE scan_id=? AND state='STAGING'",
                    (scan_id,),
                )
                self.assertEqual(changed.rowcount, 1)
                connection.commit()
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "ambiguous"
            ):
                _resolve_n16_publication_boundary(
                    Path(recorder.db_file),
                    Path(recorder.n16_claim_ledger_file),
                )
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )

    def test_prepared_resolution_full_graph_failure_is_zero_write(self):
        with tempfile.TemporaryDirectory() as root:
            self.publish_n16_claims(root, ("BASEUSDT",))
            recorder, scan_id, publish = self.stage_n16_pass(root)
            with patch.object(
                recorder.n16_claim_ledger,
                "commit_prepared",
                side_effect=RuntimeError("commit confirmation unavailable"),
            ):
                self.assertFalse(publish(scan_id, 1))
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
            )
            review_db = Path(recorder.db_file)
            ledger_db = Path(recorder.n16_claim_ledger_file)
            checkpoint_delete_mode(review_db)
            checkpoint_delete_mode(ledger_db)
            corrupt_n16_audit_graph_with_catalog_generation_preserved(
                review_db
            )
            before = (
                zero_write_database_fingerprint(review_db),
                zero_write_database_fingerprint(ledger_db),
            )
            with self.assertRaises(SignalRetentionMaintenanceError):
                _resolve_n16_publication_boundary(review_db, ledger_db)
            self.assertEqual(
                (
                    zero_write_database_fingerprint(review_db),
                    zero_write_database_fingerprint(ledger_db),
                ),
                before,
            )
            self.assertEqual(
                N16PermanentClaimLedger(ledger_db).metadata_phase(),
                "PREPARED",
            )

    def test_idempotent_install_full_graph_failure_is_zero_write(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, review_db, _signal_ids = self.publish_n16_claims(
                root, ("N16USDT",)
            )
            ledger_db = Path(recorder.n16_claim_ledger_file)
            checkpoint_delete_mode(review_db)
            checkpoint_delete_mode(ledger_db)
            corrupt_n16_audit_graph_with_catalog_generation_preserved(
                review_db
            )
            before = (
                zero_write_database_fingerprint(review_db),
                zero_write_database_fingerprint(ledger_db),
            )
            with self.assertRaises(SignalRetentionMaintenanceError):
                _install_n16_claim_boundary(review_db, ledger_db)
            self.assertEqual(
                (
                    zero_write_database_fingerprint(review_db),
                    zero_write_database_fingerprint(ledger_db),
                ),
                before,
            )

    def test_stopped_service_ledger_catalog_and_integrity_fail_zero_write(self):
        for mode in ("extra_catalog", "wrong_index", "integrity"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                recorder, review_db, _signal_ids = self.publish_n16_claims(
                    root, ("N16USDT",)
                )
                ledger_db = Path(recorder.n16_claim_ledger_file)
                checkpoint_delete_mode(review_db)
                checkpoint_delete_mode(ledger_db)
                if mode != "integrity":
                    with closing(sqlite3.connect(ledger_db)) as connection:
                        if mode == "extra_catalog":
                            connection.execute(
                                "CREATE VIEW n16_unexpected_view AS SELECT 1 AS value"
                            )
                        else:
                            connection.execute(
                                "DROP INDEX idx_n16_permanent_claim_structure"
                            )
                            connection.execute(
                                "CREATE INDEX idx_n16_permanent_claim_structure "
                                "ON n16_permanent_claims(symbol)"
                            )
                        connection.commit()
                before = (
                    zero_write_database_fingerprint(review_db),
                    zero_write_database_fingerprint(ledger_db),
                )
                integrity_patch = (
                    patch.object(
                        N16PermanentClaimLedger,
                        "_maintenance_integrity_check",
                        side_effect=N16ClaimLedgerError(
                            "simulated ledger integrity rejection"
                        ),
                    )
                    if mode == "integrity"
                    else nullcontext()
                )
                expected_error = (
                    SignalRetentionMaintenanceError
                    if mode == "integrity"
                    else N16ClaimLedgerError
                )
                with integrity_patch, self.assertRaises(expected_error):
                    _install_n16_claim_boundary(review_db, ledger_db)
                self.assertEqual(
                    (
                        zero_write_database_fingerprint(review_db),
                        zero_write_database_fingerprint(ledger_db),
                    ),
                    before,
                )

    def test_multiple_n16_claims_activate_in_canonical_signal_order(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            candidates = [candidate("ZZZUSDT", 9), candidate("AAAUSDT", 3)]
            scan_id = recorder.begin_scan(2, candidates, True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16-multi")
            )
            signal_ids = []
            for item in candidates:
                analysis = analyze_n16_mature_trend_support(
                    item.symbol,
                    rows,
                    quote_volume_rank=item.quote_volume_rank,
                    checked_at_ms=checked,
                )
                self.assertTrue(analysis.passed, analysis.reason)
                self.assertIn(
                    recorder.record_n16_state(analysis.state_record),
                    {"INSERTED", "UNCHANGED"},
                )
                signal_ids.append(
                    scheduler._record_signal(
                        scan_id,
                        StrategySignalDecision(
                            strategy=N16_STRATEGY,
                            candidate=item,
                            analysis=analysis,
                            passed=True,
                            decision="PASSED",
                            reason="PASSED",
                        ),
                    )
                )
            self.assertTrue(all(type(value) is int for value in signal_ids))
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 2))
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                review_rows = connection.execute(
                    "SELECT seal_ordinal,source_signal_id,symbol,structure_id "
                    "FROM n16_consumption_seals ORDER BY seal_ordinal"
                ).fetchall()
            with recorder.n16_claim_ledger._open(read_only=True) as connection:
                external_rows = connection.execute(
                    "SELECT claim_ordinal,source_signal_id,symbol,structure_id "
                    "FROM n16_permanent_claims ORDER BY claim_ordinal"
                ).fetchall()
            self.assertEqual(review_rows, external_rows)
            self.assertEqual(
                [row[1] for row in review_rows], sorted(signal_ids)
            )
            self.assertEqual(
                recorder.n16_claim_ledger.metadata_phase(), "READY"
            )

    def test_multi_claim_confirmation_revalidates_every_prepared_chain_link(self):
        rows, checked = n16_klines()
        for corruption in (
            "payload_only",
            "digest_only",
            "chain_only",
            "resigned_middle_only",
        ):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as root:
                recorder = self.make_recorder(root)
                recorder.upsert_strategy_definitions(load_all_strategies())
                candidates = [
                    candidate("AAAUSDT", 1),
                    candidate("BBBUSDT", 2),
                    candidate("CCCUSDT", 3),
                ]
                scan_id = recorder.begin_scan(3, candidates, True)
                scheduler = StrategyScheduler(
                    (N16_STRATEGY,),
                    5,
                    recorder,
                    logging.getLogger("n16-prepared-chain"),
                )
                for item in candidates:
                    analysis = analyze_n16_mature_trend_support(
                        item.symbol,
                        rows,
                        quote_volume_rank=item.quote_volume_rank,
                        checked_at_ms=checked,
                    )
                    self.assertTrue(analysis.passed, analysis.reason)
                    self.assertIn(
                        recorder.record_n16_state(analysis.state_record),
                        {"INSERTED", "UNCHANGED"},
                    )
                    self.assertIsNotNone(
                        scheduler._record_signal(
                            scan_id,
                            StrategySignalDecision(
                                strategy=N16_STRATEGY,
                                candidate=item,
                                analysis=analysis,
                                passed=True,
                                decision="PASSED",
                                reason="PASSED",
                            ),
                        )
                    )

                captured = {}
                real_commit = recorder.n16_claim_ledger.commit_prepared

                def hold_confirmation(prepared, summary, now):
                    captured["prepared"] = prepared
                    captured["summary"] = summary
                    raise RuntimeError("confirmation acknowledgement unavailable")

                with patch.object(
                    recorder.n16_claim_ledger,
                    "commit_prepared",
                    side_effect=hold_confirmation,
                ):
                    self.assertFalse(
                        recorder.publish_strategy_signal_batch(scan_id, 3)
                    )
                self.assertEqual(
                    recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
                )
                self.assertEqual(set(captured), {"prepared", "summary"})

                with recorder.n16_claim_ledger._open(
                    read_only=False
                ) as connection:
                    middle = list(
                        connection.execute(
                            "SELECT claim_ordinal,source_signal_id,"
                            "schema_version,rule_version,strategy_id,"
                            "source_scan_id,audit_id,review_ledger_id,state_id,"
                            "symbol,episode_id,structure_id,"
                            "signal_evidence_sha256,state_evidence_sha256,"
                            "signal_created_at,claim_created_at,"
                            "claim_sha256,chain_sha256 "
                            "FROM n16_permanent_claims WHERE claim_ordinal=2"
                        ).fetchone()
                    )
                    previous_chain = connection.execute(
                        "SELECT chain_sha256 FROM n16_permanent_claims "
                        "WHERE claim_ordinal=1"
                    ).fetchone()[0]
                    if corruption in {"payload_only", "resigned_middle_only"}:
                        middle[15] += "x"
                    if corruption == "digest_only":
                        middle[16] = "0" * 64
                    elif corruption == "chain_only":
                        middle[17] = "0" * 64
                    elif corruption == "resigned_middle_only":
                        middle[16] = review_claim_sha256(tuple(middle[:16]))
                        middle[17] = advance_review_claim_chain(
                            previous_chain,
                            tuple(middle[:16]),
                        )
                    updated = connection.execute(
                        "UPDATE n16_permanent_claims SET "
                        "claim_created_at=?,claim_sha256=?,chain_sha256=? "
                        "WHERE claim_ordinal=2 AND claim_state='PREPARED'",
                        (middle[15], middle[16], middle[17]),
                    )
                    self.assertEqual(updated.rowcount, 1)
                    connection.commit()

                def ledger_snapshot():
                    with recorder.n16_claim_ledger._open(
                        read_only=True
                    ) as connection:
                        return (
                            connection.execute(
                                "SELECT * FROM n16_claim_ledger_meta"
                            ).fetchall(),
                            connection.execute(
                                "SELECT * FROM n16_permanent_claims "
                                "ORDER BY claim_ordinal"
                            ).fetchall(),
                        )

                before = ledger_snapshot()
                with self.assertRaisesRegex(
                    N16ClaimLedgerError, "prepared claim"
                ):
                    real_commit(
                        captured["prepared"],
                        captured["summary"],
                        "2026-07-16T12:00:00+00:00",
                    )
                self.assertEqual(ledger_snapshot(), before)
                self.assertEqual(
                    recorder.n16_claim_ledger.metadata_phase(), "PREPARED"
                )
                with recorder.n16_claim_ledger._open(
                    read_only=True
                ) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM n16_permanent_claims "
                            "WHERE claim_state='COMMITTED'"
                        ).fetchone(),
                        (0,),
                    )

    def test_middle_review_claim_change_is_rejected_by_chain_head_before_write(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, db, _signal_ids = self.publish_n16_claims(
                root, ("AAAUSDT", "BBBUSDT", "CCCUSDT")
            )
            checkpoint_delete_mode(db)
            with closing(sqlite3.connect(db)) as connection:
                triggers = (
                    "trg_n16_seal_no_update",
                    "trg_n16_audit_no_update_after_active",
                    "trg_n16_ledger_no_update_after_active",
                )
                for trigger in triggers:
                    connection.execute("DROP TRIGGER " + trigger)
                source_signal_id = connection.execute(
                    "SELECT source_signal_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=2"
                ).fetchone()[0]
                changed_seal = connection.execute(
                    "UPDATE n16_consumption_seals "
                    "SET claim_created_at='2026-07-16T23:59:59+00:00' "
                    "WHERE seal_ordinal=2"
                )
                changed_audit = connection.execute(
                    "UPDATE strategy_passed_signal_audits "
                    "SET created_at='2026-07-16T23:59:59+00:00' "
                    "WHERE strategy_id='N16' AND source_signal_id=?",
                    (source_signal_id,),
                )
                changed_ledger = connection.execute(
                    "UPDATE strategy_passed_structure_ledger "
                    "SET created_at='2026-07-16T23:59:59+00:00' "
                    "WHERE strategy_id='N16' AND source_signal_id=?",
                    (source_signal_id,),
                )
                self.assertEqual(
                    (
                        changed_seal.rowcount,
                        changed_audit.rowcount,
                        changed_ledger.rowcount,
                    ),
                    (1, 1, 1),
                )
                for trigger in triggers:
                    connection.execute(_N16_TRIGGER_SQL[trigger])
                connection.commit()
            before = zero_write_database_fingerprint(db)
            with self.assertRaisesRegex(RuntimeError, "claim chain"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-middle-chain"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)

    def test_synced_catalog_middle_change_cannot_authorize_structure_reuse(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder, db, _signal_ids = self.publish_n16_claims(
                root, ("AAAUSDT", "BBBUSDT", "CCCUSDT")
            )
            checkpoint_delete_mode(db)
            with closing(sqlite3.connect(db)) as connection:
                triggers = (
                    "trg_n16_seal_no_update",
                    "trg_n16_audit_no_update_after_active",
                    "trg_n16_ledger_no_update_after_active",
                    "trg_n16_guard_no_update",
                    "trg_n16_root_no_update",
                )
                for trigger in triggers:
                    connection.execute("DROP TRIGGER " + trigger)
                source_signal_id, structure_id = connection.execute(
                    "SELECT source_signal_id,structure_id "
                    "FROM n16_consumption_seals WHERE seal_ordinal=2"
                ).fetchone()
                changed_at = "2026-07-16T23:59:59+00:00"
                self.assertEqual(
                    connection.execute(
                        "UPDATE n16_consumption_seals "
                        "SET claim_created_at=? WHERE seal_ordinal=2",
                        (changed_at,),
                    ).rowcount,
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "UPDATE strategy_passed_signal_audits "
                        "SET created_at=? WHERE strategy_id='N16' "
                        "AND source_signal_id=?",
                        (changed_at, source_signal_id),
                    ).rowcount,
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "UPDATE strategy_passed_structure_ledger "
                        "SET created_at=? WHERE strategy_id='N16' "
                        "AND source_signal_id=?",
                        (changed_at, source_signal_id),
                    ).rowcount,
                    1,
                )
                current_cookie = connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()[0]
                final_cookie = current_cookie + len(triggers)
                connection.execute(
                    "UPDATE n16_lifecycle_guard "
                    "SET catalog_schema_version=? WHERE singleton_id=1",
                    (final_cookie,),
                )
                connection.execute(
                    "UPDATE strategy_lifecycle_installations "
                    "SET catalog_schema_version=? "
                    "WHERE singleton_id=1 AND strategy_id='N16'",
                    (final_cookie,),
                )
                for trigger in triggers:
                    connection.execute(_N16_TRIGGER_SQL[trigger])
                self.assertEqual(
                    connection.execute("PRAGMA schema_version").fetchone(),
                    (final_cookie,),
                )
                connection.commit()

            # The independent protected-generation commitment now binds the
            # global SQLite catalog cookie as well as the protected catalog.
            # A coordinated trigger rebuild is therefore rejected at startup,
            # before exact per-structure access can be attempted.
            with self.assertRaisesRegex(
                RuntimeError,
                "protected Review catalog changed",
            ):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-synced-middle"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
            with closing(sqlite3.connect(db)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_consumption_seals"
                    ).fetchone(),
                    (3,),
                )

    def test_review_chain_head_conflict_is_rejected_before_write(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, db, _signal_ids = self.publish_n16_claims(
                root, ("AAAUSDT", "BBBUSDT", "CCCUSDT")
            )
            checkpoint_delete_mode(db)
            with closing(sqlite3.connect(db)) as connection:
                for trigger in (
                    "trg_n16_guard_no_update",
                    "trg_n16_root_no_update",
                ):
                    connection.execute("DROP TRIGGER " + trigger)
                connection.execute(
                    "UPDATE n16_lifecycle_guard "
                    "SET confirmed_chain_sha256=?",
                    ("f" * 64,),
                )
                connection.execute(
                    "UPDATE strategy_lifecycle_installations "
                    "SET confirmed_chain_sha256=? WHERE strategy_id='N16'",
                    ("f" * 64,),
                )
                for trigger in (
                    "trg_n16_guard_no_update",
                    "trg_n16_root_no_update",
                ):
                    connection.execute(_N16_TRIGGER_SQL[trigger])
                connection.commit()
            before = zero_write_database_fingerprint(db)
            with self.assertRaisesRegex(RuntimeError, "claim chain"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-head-chain"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)

    def test_review_and_claim_ledger_rollback_must_be_a_matched_pair(self):
        def restore_payload(path, payload):
            with open(path, "r+b") as handle:
                handle.seek(0)
                handle.write(payload)
                handle.truncate()
                handle.flush()
                os.fsync(handle.fileno())

        for mode in ("paired", "review_only", "ledger_only"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                first, review_db, _ids = self.publish_n16_claims(
                    root, ("FIRSTUSDT",)
                )
                ledger_path = Path(first.n16_claim_ledger_file)
                checkpoint_delete_mode(review_db)
                checkpoint_delete_mode(ledger_path)
                self.assertFalse(
                    any(
                        Path(str(path) + suffix).exists()
                        for path in (review_db, ledger_path)
                        for suffix in ("-wal", "-shm", "-journal")
                    )
                )
                claim_one_review = review_db.read_bytes()
                claim_one_ledger = ledger_path.read_bytes()
                claim_one_hashes = (
                    hashlib.sha256(claim_one_review).hexdigest(),
                    hashlib.sha256(claim_one_ledger).hexdigest(),
                )
                with first._read_only_runtime_snapshot() as connection:
                    claim_one_summary = _n16_review_claim_summary(connection)
                claim_one_chain_head = (
                    claim_one_summary.confirmed_chain_sha256
                )

                second, _review_db, _ids = self.publish_n16_claims(
                    root, ("SECONDUSDT",)
                )
                checkpoint_delete_mode(review_db)
                checkpoint_delete_mode(ledger_path)
                with closing(sqlite3.connect(review_db)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT sealed_claim_count,confirmed_chain_sha256 "
                            "FROM n16_lifecycle_guard"
                        ).fetchone()[0],
                        2,
                    )
                review_inode = review_db.lstat().st_ino
                ledger_inode = ledger_path.lstat().st_ino
                if mode in {"paired", "review_only"}:
                    restore_payload(review_db, claim_one_review)
                if mode in {"paired", "ledger_only"}:
                    restore_payload(ledger_path, claim_one_ledger)
                self.assertEqual(review_db.lstat().st_ino, review_inode)
                self.assertEqual(ledger_path.lstat().st_ino, ledger_inode)
                self.assertFalse(
                    any(
                        Path(str(path) + suffix).exists()
                        for path in (review_db, ledger_path)
                        for suffix in ("-wal", "-shm", "-journal")
                    )
                )

                if mode == "paired":
                    self.assertEqual(
                        (
                            hashlib.sha256(review_db.read_bytes()).hexdigest(),
                            hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
                        ),
                        claim_one_hashes,
                    )
                    restarted = ReviewRecorder(
                        review_db,
                        logging.getLogger("n16-paired-rollback"),
                        n16_claim_ledger_file=ledger_path,
                    )
                    with restarted._read_only_runtime_snapshot() as connection:
                        summary = _n16_review_claim_summary(connection)
                    self.assertEqual(summary.confirmed_claim_count, 1)
                    self.assertEqual(
                        summary.confirmed_chain_sha256,
                        claim_one_chain_head,
                    )
                    with restarted.n16_claim_ledger._open(
                        read_only=True
                    ) as ledger_connection:
                        ledger_chain_head = (
                            restarted.n16_claim_ledger._meta(
                                ledger_connection
                            )[10]
                        )
                    self.assertEqual(ledger_chain_head, claim_one_chain_head)
                    restarted.n16_claim_ledger.attest(summary)
                    continue

                before = (
                    zero_write_database_fingerprint(review_db),
                    zero_write_database_fingerprint(ledger_path),
                )
                with self.assertRaisesRegex(RuntimeError, "inconsistent"):
                    ReviewRecorder(
                        review_db,
                        logging.getLogger("n16-unpaired-rollback"),
                        n16_claim_ledger_file=ledger_path,
                    )
                self.assertEqual(
                    (
                        zero_write_database_fingerprint(review_db),
                        zero_write_database_fingerprint(ledger_path),
                    ),
                    before,
                )

    def test_pre_n16_ordinary_startup_is_zero_write_and_requires_maintenance(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "legacy-review.sqlite3"
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY, value TEXT)"
                )
                connection.execute(
                    "INSERT INTO legacy_marker(id,value) VALUES(1,'preserve')"
                )
                connection.commit()
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.commit()
            ledger = Path(root) / "n16_claim_ledger.sqlite3"
            before = zero_write_database_fingerprint(db)
            statements = []
            original_connect = sqlite3.connect

            def traced_connect(*args, **kwargs):
                connection = original_connect(*args, **kwargs)
                connection.set_trace_callback(statements.append)
                return connection

            with patch(
                "trading_bot.recorder.sqlite3.connect",
                side_effect=traced_connect,
            ), patch(
                "trading_bot.recorder._n16_preinstall_trace_exists",
                side_effect=AssertionError(
                    "ordinary PRE_N16 startup must not scan business history"
                ),
            ), self.assertRaisesRegex(RuntimeError, "explicit stopped-service"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-preinstall-readonly"),
                    n16_claim_ledger_file=ledger,
                )
            forbidden_tables = (
                "strategy_definitions",
                "strategy_signals",
                "strategy_passed_signal_audits",
                "strategy_passed_structure_ledger",
                "strategy_paper_trades",
                "strategy_states",
                "strategy_live_links",
                "strategy_structure_terminal_states",
            )
            data_queries = tuple(
                statement.casefold()
                for statement in statements
                if statement.lstrip().casefold().startswith(("select", "with"))
            )
            for table in forbidden_tables:
                self.assertFalse(
                    any(
                        (" from %s" % table) in statement
                        or (' from "%s"' % table) in statement
                        or (" join %s" % table) in statement
                        or (' join "%s"' % table) in statement
                        for statement in data_queries
                    ),
                    (table, data_queries),
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)
            self.assertFalse(ledger.exists())

    def test_explicit_cli_installs_pre_n16_once_and_preserves_existing_data(self):
        with tempfile.TemporaryDirectory() as root:
            app_root = Path(root) / "binance-app"
            app_root.mkdir(mode=0o700)
            db = app_root / "review.sqlite3"
            ledger = app_root / "n16_claim_ledger.sqlite3"
            lock = app_root / "trading_bot.lock"
            lock.write_text("\n", encoding="utf-8")
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY, value TEXT)"
                )
                connection.execute(
                    "INSERT INTO legacy_marker(id,value) VALUES(1,'preserve')"
                )
                connection.commit()
            output = io.StringIO()
            with redirect_stdout(output), redirect_stderr(output):
                exit_code = signal_retention_main(
                    [
                        "--install-n16",
                        "--db",
                        str(db.resolve()),
                        "--n16-claim-ledger",
                        str(ledger.resolve()),
                        "--lock-file",
                        str(lock.resolve()),
                        "--binance-root",
                        str(app_root.resolve()),
                    ]
                )
            self.assertEqual(exit_code, 0, output.getvalue())
            _install_n17_lifecycle_boundary(db, ledger)
            _install_n19_lifecycle_boundary(db, ledger)
            _install_n18_lifecycle_boundary(db, ledger)
            _install_n20_lifecycle_boundary(db, ledger)
            _install_micro_lifecycle_boundary(db, ledger)
            _install_coverage_epoch_boundary(db, ledger)
            recorder = ReviewRecorder(
                db,
                logging.getLogger("n16-cli-install"),
                n16_claim_ledger_file=ledger,
            )
            self.assertEqual(recorder.n16_claim_ledger.metadata_phase(), "READY")
            with closing(sqlite3.connect(db)) as connection:
                self.assertEqual(
                    connection.execute("SELECT * FROM legacy_marker").fetchall(),
                    [(1, "preserve")],
                )

    def test_current_review_with_missing_ledger_cannot_bootstrap(self):
        with tempfile.TemporaryDirectory() as root:
            app_root = Path(root) / "binance-app"
            app_root.mkdir(mode=0o700)
            recorder = self.make_recorder(str(app_root))
            db = Path(recorder.db_file)
            ledger = Path(recorder.n16_claim_ledger_file)
            checkpoint_delete_mode(db)
            held = app_root / "held-ledger.sqlite3"
            ledger.replace(held)
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(str(ledger) + suffix)
                if sidecar.exists():
                    sidecar.unlink()
            before_main = hashlib.sha256(db.read_bytes()).hexdigest()
            before_identity = zero_write_database_fingerprint(db)[2]
            held_fingerprint = (
                hashlib.sha256(held.read_bytes()).hexdigest(),
                held.lstat().st_ino,
                held.lstat().st_nlink,
            )
            (app_root / "trading_bot.lock").write_text("\n", encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output), redirect_stderr(output):
                exit_code = signal_retention_main(
                    [
                        "--install-n16",
                        "--db",
                        str(db.resolve()),
                        "--n16-claim-ledger",
                        str(ledger.resolve()),
                        "--lock-file",
                        str((app_root / "trading_bot.lock").resolve()),
                        "--binance-root",
                        str(app_root.resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertFalse(ledger.exists())
            self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(), before_main)
            self.assertEqual(zero_write_database_fingerprint(db)[2], before_identity)
            self.assertEqual(
                (
                    hashlib.sha256(held.read_bytes()).hexdigest(),
                    held.lstat().st_ino,
                    held.lstat().st_nlink,
                ),
                held_fingerprint,
            )
            ordinary_before = zero_write_database_fingerprint(db)
            with self.assertRaisesRegex(RuntimeError, "claim ledger is missing"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-current-missing"),
                    n16_claim_ledger_file=ledger,
                )
            self.assertEqual(zero_write_database_fingerprint(db), ordinary_before)
            self.assertFalse(hasattr(N16PermanentClaimLedger, "bootstrap"))

    def test_interrupted_install_is_read_only_until_explicit_confirmation(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY, value TEXT)"
                )
                connection.execute(
                    "INSERT INTO legacy_marker VALUES(1,'preserve')"
                )
                connection.commit()
            real_confirm = N16PermanentClaimLedger.confirm_install
            with patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                side_effect=RuntimeError("installation confirmation unavailable"),
            ), self.assertRaisesRegex(
                SignalRetentionMaintenanceError, "did not complete"
            ):
                _install_n16_claim_boundary(db, ledger_path)
            ledger = N16PermanentClaimLedger(ledger_path)
            self.assertEqual(ledger.metadata_phase(), "INSTALLING")
            checkpoint_delete_mode(db)
            before = zero_write_database_fingerprint(db)
            with self.assertRaisesRegex(RuntimeError, "inconsistent"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-install-interrupted"),
                    n16_claim_ledger_file=ledger_path,
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)
            with patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                real_confirm,
            ):
                summary = _install_n16_claim_boundary(db, ledger_path)
            self.assertEqual(summary.confirmed_claim_count, 0)
            self.assertEqual(
                N16PermanentClaimLedger(ledger_path).metadata_phase(),
                "READY",
            )
            _install_n17_lifecycle_boundary(db, ledger_path)
            _install_n19_lifecycle_boundary(db, ledger_path)
            _install_n18_lifecycle_boundary(db, ledger_path)
            _install_n20_lifecycle_boundary(db, ledger_path)
            _install_micro_lifecycle_boundary(db, ledger_path)
            _install_coverage_epoch_boundary(db, ledger_path)
            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-install-resumed"),
                n16_claim_ledger_file=ledger_path,
            )
            self.assertEqual(restarted.n16_claim_ledger.metadata_phase(), "READY")

    def test_install_confirmation_commit_ack_loss_requires_explicit_retry(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY)"
                )
                connection.commit()
            real_confirm = N16PermanentClaimLedger.confirm_install
            committed = []

            def commit_then_lose_ack(ledger, summary, now):
                real_confirm(ledger, summary, now)
                committed.append(summary.confirmed_claim_count)
                raise RuntimeError("installation confirmation acknowledgement lost")

            with patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                new=commit_then_lose_ack,
            ), self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "did not complete",
            ):
                _install_n16_claim_boundary(db, ledger_path)
            self.assertEqual(committed, [0])
            self.assertEqual(
                N16PermanentClaimLedger(ledger_path).metadata_phase(),
                "READY",
            )

            summary = _install_n16_claim_boundary(db, ledger_path)
            self.assertEqual(summary.confirmed_claim_count, 0)
            self.assertEqual(
                N16PermanentClaimLedger(ledger_path).metadata_phase(),
                "READY",
            )
            _install_n17_lifecycle_boundary(db, ledger_path)
            _install_n19_lifecycle_boundary(db, ledger_path)
            _install_n18_lifecycle_boundary(db, ledger_path)
            _install_n20_lifecycle_boundary(db, ledger_path)
            _install_micro_lifecycle_boundary(db, ledger_path)
            _install_coverage_epoch_boundary(db, ledger_path)
            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-install-ack-retry"),
                n16_claim_ledger_file=ledger_path,
            )
            self.assertEqual(
                restarted.n16_claim_ledger.metadata_phase(),
                "READY",
            )

    def test_installing_resume_rejects_every_n16_business_trace_zero_write(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=1, checked_at_ms=checked
        )
        self.assertIsNotNone(analysis.state_record)

        for mode in ("state", "state_sequence", "event", "trade_review"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                db = Path(root) / "review.sqlite3"
                ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
                with closing(sqlite3.connect(db)) as connection:
                    connection.execute(
                        "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY)"
                    )
                    connection.commit()
                with patch.object(
                    N16PermanentClaimLedger,
                    "confirm_install",
                    side_effect=RuntimeError(
                        "installation confirmation acknowledgement unavailable"
                    ),
                ), self.assertRaises(SignalRetentionMaintenanceError):
                    _install_n16_claim_boundary(db, ledger_path)
                self.assertEqual(
                    N16PermanentClaimLedger(ledger_path).metadata_phase(),
                    "INSTALLING",
                )
                checkpoint_delete_mode(db)
                checkpoint_delete_mode(ledger_path)
                with closing(sqlite3.connect(db)) as connection:
                    if mode in {"state", "state_sequence"}:
                        record = analysis.state_record
                        connection.execute(
                            "INSERT INTO n16_trend_support_states("
                            "strategy_id,symbol,episode_id,structure_id,stage,"
                            "reason,quote_volume_rank,evidence_json,"
                            "evidence_sha256,created_at,updated_at) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                record.strategy_id,
                                record.symbol,
                                record.episode_id,
                                record.structure_id,
                                record.stage,
                                record.reason,
                                record.quote_volume_rank,
                                record.evidence_json,
                                record.evidence_sha256,
                                "2026-07-16T00:00:00+00:00",
                                "2026-07-16T00:00:00+00:00",
                            ),
                        )
                        if mode == "state_sequence":
                            connection.execute(
                                "DELETE FROM n16_trend_support_states"
                            )
                    elif mode == "event":
                        connection.execute(
                            "INSERT INTO events(occurred_at,event_type,symbol,"
                            "payload_json) VALUES(?,?,?,?)",
                            (
                                "2026-07-16T00:00:00+00:00",
                                "strategy_live_result_alarm",
                                "N16USDT",
                                json.dumps(
                                    {
                                        "strategy_id": "N16",
                                        "result": "LIVE_RESULT_PENDING",
                                    }
                                ),
                            ),
                        )
                    else:
                        connection.execute(
                            "INSERT INTO trade_reviews(opened_at,symbol,side,"
                            "dry_run,status,orders_json) VALUES(?,?,?,?,?,?)",
                            (
                                "2026-07-16T00:00:00+00:00",
                                "N16USDT",
                                "BUY",
                                0,
                                "OPENED",
                                json.dumps(
                                    {
                                        "state_orders": {
                                            "strategy": {
                                                "strategy_id": "N16",
                                                "signal_id": 1,
                                                "structure_id": "a" * 24,
                                            }
                                        }
                                    }
                                ),
                            ),
                        )
                    connection.commit()
                checkpoint_delete_mode(db)
                before_review = zero_write_database_fingerprint(db)
                before_ledger = zero_write_database_fingerprint(ledger_path)
                with patch.object(
                    N16PermanentClaimLedger,
                    "confirm_install",
                    side_effect=AssertionError(
                        "invalid installation baseline reached phase write"
                    ),
                ), self.assertRaises(SignalRetentionMaintenanceError):
                    _install_n16_claim_boundary(db, ledger_path)
                self.assertEqual(zero_write_database_fingerprint(db), before_review)
                self.assertEqual(
                    zero_write_database_fingerprint(ledger_path), before_ledger
                )
                self.assertEqual(
                    N16PermanentClaimLedger(ledger_path).metadata_phase(),
                    "INSTALLING",
                )

    def test_installing_resume_requires_ledger_integrity_before_phase_write(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY)"
                )
                connection.commit()
            with patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                side_effect=RuntimeError("confirmation unavailable"),
            ), self.assertRaises(SignalRetentionMaintenanceError):
                _install_n16_claim_boundary(db, ledger_path)
            checkpoint_delete_mode(db)
            checkpoint_delete_mode(ledger_path)
            before_review = zero_write_database_fingerprint(db)
            before_ledger = zero_write_database_fingerprint(ledger_path)
            with patch.object(
                N16PermanentClaimLedger,
                "_maintenance_integrity_check",
                side_effect=N16ClaimLedgerError("integrity unavailable"),
            ), patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                side_effect=AssertionError("phase write must not run"),
            ), self.assertRaises(SignalRetentionMaintenanceError):
                _install_n16_claim_boundary(db, ledger_path)
            self.assertEqual(zero_write_database_fingerprint(db), before_review)
            self.assertEqual(
                zero_write_database_fingerprint(ledger_path), before_ledger
            )

    def test_unsafe_installing_cli_is_controlled_and_zero_write(self):
        with tempfile.TemporaryDirectory() as root:
            app_root = Path(root) / "binance-app"
            data_root = app_root / "data"
            data_root.mkdir(parents=True, mode=0o700)
            db = data_root / "review.sqlite3"
            ledger_path = data_root / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY)"
                )
                connection.commit()
            with patch.object(
                N16PermanentClaimLedger,
                "confirm_install",
                side_effect=RuntimeError("confirmation unavailable"),
            ), self.assertRaises(SignalRetentionMaintenanceError):
                _install_n16_claim_boundary(db, ledger_path)
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "INSERT INTO events(occurred_at,event_type,symbol,payload_json) "
                    "VALUES(?,?,?,?)",
                    (
                        "2026-07-16T00:00:00+00:00",
                        "strategy_live_result_alarm",
                        "N16USDT",
                        json.dumps(
                            {"strategy_id": "N16", "result": "PENDING"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                connection.commit()
            checkpoint_delete_mode(db)
            checkpoint_delete_mode(ledger_path)
            before_review = zero_write_database_fingerprint(db)
            before_ledger = zero_write_database_fingerprint(ledger_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = signal_retention_main(
                    [
                        "--install-n16",
                        "--db",
                        str(db.resolve()),
                        "--n16-claim-ledger",
                        str(ledger_path.resolve()),
                        "--lock-file",
                        str((app_root / "trading_bot.lock").resolve()),
                        "--binance-root",
                        str(app_root.resolve()),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertNotIn("Traceback", stdout.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())
            self.assertEqual(zero_write_database_fingerprint(db), before_review)
            self.assertEqual(
                zero_write_database_fingerprint(ledger_path), before_ledger
            )
            self.assertEqual(
                N16PermanentClaimLedger(ledger_path).metadata_phase(),
                "INSTALLING",
            )

    def test_n16_claim_ledger_create_rejects_preexisting_sidecars_without_touching_them(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as root:
                ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
                sentinel = Path(str(ledger_path) + suffix)
                sentinel.write_bytes(b"preexisting-sidecar-" + suffix.encode("ascii"))
                before = (
                    sentinel.read_bytes(),
                    sentinel.lstat().st_ino,
                    sentinel.lstat().st_nlink,
                    tuple(path.name for path in Path(root).iterdir()),
                )
                with self.assertRaisesRegex(
                    N16ClaimLedgerError,
                    "path or sidecar already exists",
                ):
                    N16PermanentClaimLedger.prepare_install(
                        ledger_path,
                        "2026-07-16T00:00:00+00:00",
                    )
                self.assertFalse(ledger_path.exists())
                self.assertEqual(
                    (
                        sentinel.read_bytes(),
                        sentinel.lstat().st_ino,
                        sentinel.lstat().st_nlink,
                        tuple(path.name for path in Path(root).iterdir()),
                    ),
                    before,
                )

    def test_n16_claim_ledger_empty_file_cleanup_never_unlinks_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            parent = Path(root)
            ledger_path = parent / "n16_claim_ledger.sqlite3"
            owned = parent / "created-owned.sqlite3"
            ledger = N16PermanentClaimLedger(ledger_path)
            flags = os.O_RDONLY
            if hasattr(os, "O_DIRECTORY"):
                flags |= os.O_DIRECTORY
            parent_descriptor = os.open(str(parent), flags)
            real_fsync = os.fsync
            replacement = {}

            def fsync_then_replace(descriptor):
                if descriptor != parent_descriptor:
                    return real_fsync(descriptor)
                ledger_path.replace(owned)
                ledger_path.write_bytes(b"replacement-must-survive")
                replacement["identity"] = (
                    ledger_path.lstat().st_dev,
                    ledger_path.lstat().st_ino,
                )
                raise OSError("directory fsync acknowledgement failed")

            try:
                with patch(
                    "trading_bot.n16_claim_ledger.os.fsync",
                    side_effect=fsync_then_replace,
                ), self.assertRaisesRegex(OSError, "acknowledgement"):
                    ledger._secure_create_empty_file(
                        parent_descriptor,
                        ledger_path.name,
                    )
            finally:
                os.close(parent_descriptor)
            self.assertEqual(ledger_path.read_bytes(), b"replacement-must-survive")
            self.assertEqual(
                (ledger_path.lstat().st_dev, ledger_path.lstat().st_ino),
                replacement["identity"],
            )
            self.assertTrue(owned.is_file())

    def test_n16_claim_ledger_private_stage_leaves_concurrent_final_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            ledger_path = Path(root) / "n16_claim_ledger.sqlite3"
            real_link = os.link
            replacement = {}

            def link_then_replace(source, destination, **kwargs):
                real_link(source, destination, **kwargs)
                parent_descriptor = kwargs["dst_dir_fd"]
                os.unlink(destination, dir_fd=parent_descriptor)
                descriptor = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_descriptor,
                )
                try:
                    os.write(descriptor, b"concurrent-final-replacement")
                finally:
                    os.close(descriptor)
                details = os.stat(
                    destination,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
                replacement["identity"] = (
                    int(details.st_dev),
                    int(details.st_ino),
                )
                raise OSError("link acknowledgement failed")

            with patch(
                "trading_bot.n16_claim_ledger.os.link",
                side_effect=link_then_replace,
            ), self.assertRaisesRegex(
                N16ClaimLedgerError,
                "private cleanup was incomplete",
            ):
                N16PermanentClaimLedger.prepare_install(
                    ledger_path,
                    "2026-07-16T00:00:00+00:00",
                )
            self.assertEqual(
                ledger_path.read_bytes(),
                b"concurrent-final-replacement",
            )
            self.assertEqual(
                (ledger_path.lstat().st_dev, ledger_path.lstat().st_ino),
                replacement["identity"],
            )
            self.assertFalse(
                any(
                    path.name.startswith(".n16-ledger-stage-")
                    for path in Path(root).iterdir()
                )
            )

    def test_n16_claim_ledger_parent_swap_is_anchored_and_requires_visible_identity(self):
        for restore in (False, True):
            with self.subTest(restore=restore), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                parent = root_path / "state"
                parent.mkdir(mode=0o700)
                ledger_path = parent / "n16_claim_ledger.sqlite3"
                moved_parent = root_path / "state-original"
                external_parent = root_path / "state-replacement"
                real_link = os.link
                sentinel_payload = b"replacement-directory-sentinel"

                def link_with_parent_swap(source, destination, **kwargs):
                    parent.rename(moved_parent)
                    parent.mkdir(mode=0o700)
                    (parent / "sentinel").write_bytes(sentinel_payload)
                    real_link(source, destination, **kwargs)
                    if restore:
                        parent.rename(external_parent)
                        moved_parent.rename(parent)

                context = patch(
                    "trading_bot.n16_claim_ledger.os.link",
                    side_effect=link_with_parent_swap,
                )
                if restore:
                    with context:
                        ledger = N16PermanentClaimLedger.prepare_install(
                            ledger_path,
                            "2026-07-16T00:00:00+00:00",
                        )
                    self.assertEqual(ledger.metadata_phase(), "INSTALLING")
                    self.assertEqual(
                        (external_parent / "sentinel").read_bytes(),
                        sentinel_payload,
                    )
                else:
                    with context, self.assertRaises(Exception):
                        N16PermanentClaimLedger.prepare_install(
                            ledger_path,
                            "2026-07-16T00:00:00+00:00",
                        )
                    self.assertEqual(
                        (parent / "sentinel").read_bytes(),
                        sentinel_payload,
                    )
                    self.assertFalse((parent / ledger_path.name).exists())
                    self.assertFalse((moved_parent / ledger_path.name).exists())
                    self.assertFalse(
                        any(
                            path.name.startswith(".n16-ledger-stage-")
                            for path in moved_parent.iterdir()
                        )
                    )

    def test_n16_active_graph_rows_and_guard_are_database_immutable(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, db, structure_id, episode_id, _scan_id = (
                self.publish_and_rotate_n16(root)
            )
            attacks = (
                "UPDATE strategy_passed_signal_audits SET detail_json = '{}' "
                "WHERE strategy_id = 'N16'",
                "DELETE FROM strategy_passed_structure_ledger "
                "WHERE strategy_id = 'N16'",
                "UPDATE n16_trend_support_states SET symbol = 'FORGED' "
                "WHERE strategy_id = 'N16'",
                "DELETE FROM n16_consumption_seals",
                "DELETE FROM n16_first_claim_witness",
                "UPDATE n16_first_claim_witness "
                "SET claim_created_at = 'FORGED'",
                "UPDATE n16_lifecycle_guard SET schema_version = 1",
                "UPDATE n16_lifecycle_guard SET sealed_claim_count = 0",
                "INSERT INTO strategy_passed_signal_audits ("
                "source_signal_id, source_scan_id, strategy_id, symbol, "
                "funding_rate, matched_patterns, trend_slope, current_bullish, "
                "passed, decision, reason, structure_id, detail_json, "
                "signal_created_at, evidence_sha256, created_at, claim_state) "
                "SELECT source_signal_id + 1000000, source_scan_id, strategy_id, "
                "symbol, funding_rate, matched_patterns, trend_slope, "
                "current_bullish, passed, decision, reason, structure_id, "
                "detail_json, signal_created_at, evidence_sha256, created_at, "
                "'ACTIVE' FROM strategy_passed_signal_audits "
                "WHERE strategy_id = 'N16'",
            )
            with closing(sqlite3.connect(db)) as connection:
                for statement in attacks:
                    with self.subTest(statement=statement), self.assertRaises(
                        sqlite3.Error
                    ):
                        connection.execute(statement)
                    connection.rollback()
            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-immutable"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                restarted.inspect_passed_structure(
                    "N16", "N16USDT", structure_id
                ),
                "CONSUMED",
            )
            self.assertEqual(
                recorder.get_n16_state("N16", episode_id).stage,
                "CONSUMED",
            )

    def test_n16_replace_conflicts_cannot_evict_permanent_graph_rows(self):
        attacks = {
            "guard": (
                "INSERT OR REPLACE INTO n16_lifecycle_guard "
                "SELECT singleton_id, schema_version, rule_version, "
                "guard_sha256, catalog_schema_version, 0 "
                "FROM n16_lifecycle_guard"
            ),
            "root": (
                "INSERT OR REPLACE INTO strategy_lifecycle_installations "
                "SELECT singleton_id, strategy_id, schema_version, "
                "rule_version, guard_sha256, catalog_schema_version, 0, "
                "installed_at FROM strategy_lifecycle_installations"
            ),
            "state": (
                "INSERT OR REPLACE INTO n16_trend_support_states "
                "SELECT id, strategy_id, symbol, episode_id, structure_id, "
                "stage, reason, quote_volume_rank, evidence_json, "
                "evidence_sha256, created_at, 'FORGED-UPDATED-AT' "
                "FROM n16_trend_support_states"
            ),
            "seal": (
                "INSERT OR REPLACE INTO n16_consumption_seals "
                "SELECT seal_ordinal, source_signal_id, schema_version, "
                "rule_version, strategy_id, source_scan_id, audit_id, "
                "ledger_id, state_id, symbol, episode_id, structure_id, "
                "signal_evidence_sha256, state_evidence_sha256, "
                "signal_created_at, 'FORGED-CLAIM-AT' "
                "FROM n16_consumption_seals"
            ),
            "first_claim_witness": (
                "INSERT OR REPLACE INTO n16_first_claim_witness "
                "SELECT singleton_id, seal_ordinal, source_signal_id, "
                "schema_version, rule_version, strategy_id, source_scan_id, "
                "audit_id, ledger_id, state_id, symbol, episode_id, "
                "structure_id, signal_evidence_sha256, "
                "state_evidence_sha256, signal_created_at, "
                "'FORGED-CLAIM-AT' FROM n16_first_claim_witness"
            ),
            "audit_cross_strategy": (
                "INSERT OR REPLACE INTO strategy_passed_signal_audits "
                "SELECT id, source_signal_id, source_scan_id, 'N01', symbol, "
                "funding_rate, matched_patterns, trend_slope, "
                "current_bullish, passed, decision, reason, structure_id, "
                "detail_json, signal_created_at, evidence_sha256, "
                "created_at, claim_state "
                "FROM strategy_passed_signal_audits "
                "WHERE strategy_id = 'N16'"
            ),
            "ledger_cross_strategy": (
                "INSERT OR REPLACE INTO strategy_passed_structure_ledger "
                "SELECT id, 'N06', symbol, structure_id, source_signal_id, "
                "source_scan_id, source_signal_created_at, evidence_sha256, "
                "created_at, claim_state "
                "FROM strategy_passed_structure_ledger "
                "WHERE strategy_id = 'N16'"
            ),
        }
        for attack, statement in attacks.items():
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as root:
                recorder, db, structure_id, _episode_id, _scan_id = (
                    self.publish_and_rotate_n16(root)
                )
                checkpoint_delete_mode(db)
                before = zero_write_database_fingerprint(db)
                with closing(sqlite3.connect(db)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "PRAGMA recursive_triggers"
                        ).fetchone(),
                        (0,),
                    )
                    with self.assertRaises(sqlite3.Error):
                        connection.execute(statement)
                    connection.rollback()
                self.assertEqual(zero_write_database_fingerprint(db), before)
                restarted = ReviewRecorder(
                    db,
                    logging.getLogger("n16-replace-blocked"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
                self.assertEqual(
                    restarted.inspect_passed_structure(
                        "N16", "N16USDT", structure_id
                    ),
                    "CONSUMED",
                )

    def test_historical_n16_graph_corruption_is_zero_write_startup_rejection(self):
        attacks = (
            "state_row",
            "state_table",
            "audit",
            "ledger",
            "audit_and_ledger",
            "audit_evidence",
            "seal_evidence",
            "guard_row",
            "all_graph_rows",
            "all_graph_and_roots",
            "all_review_proof",
        )
        for attack in attacks:
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as root:
                recorder, db, structure_id, _episode_id, _scan_id = (
                    self.publish_and_rotate_n16(root)
                )
                checkpoint_delete_mode(db)
                with closing(sqlite3.connect(db)) as connection:
                    removed_triggers = []
                    if attack == "state_row":
                        removed_triggers.append(
                            "trg_n16_state_no_delete_after_seal"
                        )
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute(
                            "DELETE FROM n16_trend_support_states"
                        )
                    elif attack == "state_table":
                        connection.execute(
                            "DROP TABLE n16_trend_support_states"
                        )
                    elif attack in {"audit", "audit_and_ledger"}:
                        removed_triggers.append(
                            "trg_n16_audit_no_delete_after_active"
                        )
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute(
                            "DELETE FROM strategy_passed_signal_audits "
                            "WHERE strategy_id = 'N16'"
                        )
                        if attack == "audit_and_ledger":
                            removed_triggers.append(
                                "trg_n16_ledger_no_delete_after_active"
                            )
                            connection.execute("DROP TRIGGER " + removed_triggers[-1])
                            connection.execute(
                                "DELETE FROM strategy_passed_structure_ledger "
                                "WHERE strategy_id = 'N16'"
                            )
                    elif attack == "ledger":
                        removed_triggers.append(
                            "trg_n16_ledger_no_delete_after_active"
                        )
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute(
                            "DELETE FROM strategy_passed_structure_ledger "
                            "WHERE strategy_id = 'N16'"
                        )
                    elif attack == "audit_evidence":
                        removed_triggers.append(
                            "trg_n16_audit_no_update_after_active"
                        )
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute(
                            "UPDATE strategy_passed_signal_audits "
                            "SET detail_json = '{}' WHERE strategy_id = 'N16'"
                        )
                    elif attack == "seal_evidence":
                        removed_triggers.append("trg_n16_seal_no_update")
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute(
                            "UPDATE n16_consumption_seals "
                            "SET state_evidence_sha256 = ?",
                            ("0" * 64,),
                        )
                    elif attack in {
                        "all_graph_rows",
                        "all_graph_and_roots",
                        "all_review_proof",
                    }:
                        removed_triggers.extend(
                            (
                                "trg_n16_seal_no_delete",
                                "trg_n16_state_no_delete_after_seal",
                                "trg_n16_audit_no_delete_after_active",
                                "trg_n16_ledger_no_delete_after_active",
                            )
                        )
                        if attack in {"all_graph_and_roots", "all_review_proof"}:
                            removed_triggers.extend(
                                (
                                    "trg_n16_guard_no_update",
                                    "trg_n16_root_no_update",
                                )
                            )
                        if attack == "all_review_proof":
                            removed_triggers.append("trg_n16_witness_no_delete")
                        original_catalog = connection.execute(
                            "PRAGMA schema_version"
                        ).fetchone()[0]
                        for trigger in removed_triggers:
                            connection.execute("DROP TRIGGER " + trigger)
                        connection.execute("DELETE FROM n16_consumption_seals")
                        connection.execute(
                            "DELETE FROM n16_trend_support_states"
                        )
                        connection.execute(
                            "DELETE FROM strategy_passed_signal_audits "
                            "WHERE strategy_id = 'N16'"
                        )
                        connection.execute(
                            "DELETE FROM strategy_passed_structure_ledger "
                            "WHERE strategy_id = 'N16'"
                        )
                        if attack == "all_review_proof":
                            connection.execute("DELETE FROM n16_first_claim_witness")
                        if attack in {"all_graph_and_roots", "all_review_proof"}:
                            sequence_update = connection.execute(
                                "UPDATE sqlite_sequence SET seq = 0 "
                                "WHERE name = 'n16_consumption_seals'"
                            )
                            self.assertEqual(sequence_update.rowcount, 1)
                            final_catalog = (
                                original_catalog + 2 * len(removed_triggers)
                            )
                            connection.execute(
                                "UPDATE n16_lifecycle_guard "
                                "SET catalog_schema_version = ?, "
                                "sealed_claim_count = 0, "
                                "confirmed_chain_sha256 = ?",
                                (final_catalog, "0" * 64),
                            )
                            connection.execute(
                                "UPDATE strategy_lifecycle_installations "
                                "SET catalog_schema_version = ?, "
                                "sealed_claim_count = 0, "
                                "confirmed_chain_sha256 = ?",
                                (final_catalog, "0" * 64),
                            )
                    else:
                        removed_triggers.append("trg_n16_guard_no_delete")
                        connection.execute("DROP TRIGGER " + removed_triggers[-1])
                        connection.execute("DELETE FROM n16_lifecycle_guard")
                    for trigger in removed_triggers:
                        connection.execute(_N16_TRIGGER_SQL[trigger])
                    connection.commit()
                    if attack in {"all_graph_and_roots", "all_review_proof"}:
                        self.assertEqual(
                            connection.execute(
                                "SELECT (SELECT COUNT(*) FROM "
                                "n16_consumption_seals), "
                                "(SELECT sealed_claim_count FROM "
                                "n16_lifecycle_guard), "
                                "(SELECT sealed_claim_count FROM "
                                "strategy_lifecycle_installations), "
                                "(SELECT seq FROM sqlite_sequence WHERE "
                                "name='n16_consumption_seals'), "
                                "(SELECT COUNT(*) FROM "
                                "n16_first_claim_witness)"
                            ).fetchone(),
                            (
                                0,
                                0,
                                0,
                                0,
                                0 if attack == "all_review_proof" else 1,
                            ),
                        )
                        if attack == "all_graph_and_roots":
                            self.assertEqual(
                                connection.execute(
                                    "SELECT structure_id FROM "
                                    "n16_first_claim_witness"
                                ).fetchone(),
                                (structure_id,),
                            )
                before = zero_write_database_fingerprint(db)
                self.assertEqual(before[2][0], ("delete",))
                with self.assertRaises(RuntimeError):
                    ReviewRecorder(
                        db,
                        logging.getLogger("n16-corrupt"),
                        n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                    )
                self.assertEqual(zero_write_database_fingerprint(db), before)
                if attack in {"all_graph_and_roots", "all_review_proof"}:
                    self.assertIsNone(
                        recorder.begin_scan(1, [candidate()], True)
                    )
                    self.assertEqual(
                        zero_write_database_fingerprint(db), before
                    )
                if attack == "ledger":
                    with self.assertRaises(RuntimeError):
                        recorder.get_active_n16_states("N16")
                    self.assertIsNone(
                        recorder.begin_scan(1, [candidate()], True)
                    )
                    with closing(sqlite3.connect(
                        db.resolve().as_uri() + "?mode=ro&immutable=1",
                        uri=True,
                    )) as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM "
                                "strategy_passed_signal_audits "
                                "WHERE strategy_id = 'N16'"
                            ).fetchone(),
                            (1,),
                        )
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM "
                                "strategy_passed_structure_ledger "
                                "WHERE strategy_id = 'N16'"
                            ).fetchone(),
                            (0,),
                        )

    def test_n16_identity_cannot_be_adopted_from_another_strategy(self):
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            scan_id = recorder.begin_scan(1, [candidate("N06USDT")], True)
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    scan_id,
                    "N06",
                    "N06USDT",
                    "-0.01",
                    (),
                    "1",
                    True,
                    True,
                    "PASSED",
                    "PASSED",
                    structure_id="n06-structure",
                    detail={"source": "n06"},
                )
            )
            self.assertTrue(recorder.publish_strategy_signal_batch(scan_id, 1))
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                for table in (
                    "strategy_passed_signal_audits",
                    "strategy_passed_structure_ledger",
                ):
                    with self.subTest(table=table), self.assertRaisesRegex(
                        sqlite3.IntegrityError, "identity cannot be adopted"
                    ):
                        connection.execute(
                            "UPDATE %s SET strategy_id = 'N16' "
                            "WHERE strategy_id = 'N06'" % table
                        )
                    connection.rollback()
            restarted = ReviewRecorder(
                recorder.db_file,
                logging.getLogger("n16-no-adoption"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            with closing(sqlite3.connect(restarted.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE strategy_id = 'N16'"
                    ).fetchone(),
                    (0,),
                )

    def test_unknown_n16_catalog_is_pre_wal_zero_write_rejection(self):
        for mode in ("table", "index", "trigger"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "review.sqlite3"
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute(
                        "CREATE TABLE ordinary_sentinel "
                        "(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
                    )
                    if mode == "table":
                        connection.execute(
                            "CREATE TABLE n16_orphan (id INTEGER PRIMARY KEY)"
                        )
                    elif mode == "index":
                        connection.execute(
                            "CREATE INDEX idx_n16_orphan "
                            "ON ordinary_sentinel(payload)"
                        )
                    else:
                        connection.execute(
                            "CREATE TRIGGER trg_n16_orphan AFTER INSERT "
                            "ON ordinary_sentinel BEGIN SELECT 1; END"
                        )
                before = zero_write_database_fingerprint(database)
                with self.assertRaisesRegex(RuntimeError, "N16"):
                    ReviewRecorder(
                        database,
                        logging.getLogger("n16-orphan"),
                        n16_claim_ledger_file=Path(root)
                        / "n16_claim_ledger.sqlite3",
                    )
                self.assertEqual(zero_write_database_fingerprint(database), before)

    def test_retention_and_vacuum_reject_detached_permanent_n16_claim(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            recorder = self.make_recorder(root)
            claim_ledger = Path(recorder.n16_claim_ledger_file)
            recorder.upsert_strategy_definitions(load_all_strategies())
            first_scan = recorder.begin_scan(1, [candidate()], True)
            first = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            ).evaluate(
                first_scan,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertTrue(first.signal_batch_published)

            second_scan = recorder.begin_scan(1, [candidate("OTHERUSDT")], True)
            self.assertIsNotNone(
                recorder.record_strategy_signal(
                    second_scan,
                    "N01",
                    "OTHERUSDT",
                    "-0.02",
                    (),
                    "0",
                    False,
                    False,
                    "REJECTED",
                    "NO_MATCH",
                    detail={"rotation": True},
                )
            )
            self.assertTrue(
                recorder.publish_strategy_signal_batch(second_scan, 1)
            )
            with closing(sqlite3.connect(db)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE strategy_id = 'N16'"
                    ).fetchone(),
                    (0,),
                )
                batch = connection.execute(
                    "SELECT recorded_count, manifest_sha256 "
                    "FROM strategy_signal_batches WHERE scan_id = ?",
                    (second_scan,),
                ).fetchone()
                source = list(
                    connection.execute(
                        """
                        SELECT source_signal_id, source_scan_id, strategy_id,
                               symbol, funding_rate, matched_patterns,
                               trend_slope, current_bullish, passed, decision,
                               reason, structure_id, detail_json,
                               signal_created_at
                        FROM strategy_passed_signal_audits
                        WHERE strategy_id = 'N16'
                        """
                    ).fetchone()
                )
                source[12] = "{}"
                forged_sha = _signal_evidence_sha256(tuple(source))
                connection.execute(
                    "DROP TRIGGER trg_n16_audit_no_update_after_active"
                )
                connection.execute(
                    "DROP TRIGGER trg_n16_ledger_no_update_after_active"
                )
                connection.execute(
                    "UPDATE strategy_passed_signal_audits "
                    "SET detail_json = '{}', evidence_sha256 = ? "
                    "WHERE strategy_id = 'N16'",
                    (forged_sha,),
                )
                connection.execute(
                    "UPDATE strategy_passed_structure_ledger "
                    "SET evidence_sha256 = ? WHERE strategy_id = 'N16'",
                    (forged_sha,),
                )
                connection.commit()

            for operation in (
                lambda: inspect_signal_retention(
                    str(db.resolve()),
                    second_scan,
                    batch[0],
                    batch[1],
                    n16_claim_ledger=str(claim_ledger.resolve()),
                ),
                lambda: apply_signal_retention_maintenance(
                    str(db.resolve()),
                    second_scan,
                    batch[0],
                    batch[1],
                    n16_claim_ledger=str(claim_ledger.resolve()),
                ),
            ):
                with self.assertRaisesRegex(
                    SignalRetentionMaintenanceError,
                    "N16",
                ):
                    operation()

            connection = sqlite3.connect(db)
            try:
                self.assertEqual(
                    connection.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone(),
                    (0, 0, 0),
                )
                self.assertEqual(
                    connection.execute("PRAGMA journal_mode=DELETE").fetchone(),
                    ("delete",),
                )
            finally:
                connection.close()
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(str(db) + suffix)
                if sidecar.exists():
                    details = sidecar.lstat()
                    self.assertTrue(sidecar.is_file())
                    self.assertEqual(details.st_nlink, 1)
                    sidecar.unlink()
            destination_parent = Path(root) / "vacuum-output"
            destination_parent.mkdir()
            destination = destination_parent / "compacted.sqlite3"
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "N16",
            ):
                vacuum_signal_database_into(
                    str(db.resolve()),
                    str(destination.resolve()),
                    n16_claim_ledger=str(claim_ledger.resolve()),
                )
            self.assertFalse(destination.exists())

    def test_valid_n16_permanent_graph_survives_retention_and_vacuum(self):
        with tempfile.TemporaryDirectory() as root:
            recorder, db, structure_id, _episode_id, scan_id = (
                self.publish_and_rotate_n16(root)
            )
            recorder.complete_scan(scan_id, False)
            protected_tables = (
                "n13_rotation_states",
                "n14_market_snapshots",
                "n14_active_episodes",
                "n14_sell_impact_states",
                "n15_market_snapshots",
                "n15_entry_states",
                "n16_trend_support_states",
                "n16_lifecycle_guard",
                "n16_consumption_seals",
                "n16_first_claim_witness",
                "strategy_lifecycle_installations",
            )
            with closing(sqlite3.connect(db)) as connection:
                batch = connection.execute(
                    "SELECT recorded_count, manifest_sha256 "
                    "FROM strategy_signal_batches WHERE scan_id = ?",
                    (scan_id,),
                ).fetchone()
                before = {
                    table: _table_full_hash(connection, table)
                    for table in protected_tables
                }
            inspected = inspect_signal_retention(
                str(db.resolve()),
                scan_id,
                batch[0],
                batch[1],
                n16_claim_ledger=str(
                    Path(recorder.n16_claim_ledger_file).resolve()
                ),
            )
            self.assertEqual(inspected.migration_state, "COMPLETE")
            applied = apply_signal_retention_maintenance(
                str(db.resolve()),
                scan_id,
                batch[0],
                batch[1],
                n16_claim_ledger=str(
                    Path(recorder.n16_claim_ledger_file).resolve()
                ),
            )
            self.assertEqual(applied.migration_state, "COMPLETE")
            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-valid"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                restarted.inspect_passed_structure(
                    "N16", "N16USDT", structure_id
                ),
                "CONSUMED",
            )
            checkpoint_delete_mode(db)
            output_parent = Path(root) / "vacuum-output"
            output_parent.mkdir()
            output = output_parent / "compacted.sqlite3"
            vacuumed = vacuum_signal_database_into(
                str(db.resolve()),
                str(output.resolve()),
                n16_claim_ledger=str(
                    Path(recorder.n16_claim_ledger_file).resolve()
                ),
            )
            self.assertEqual(vacuumed.migration_state, "COMPLETE")
            with closing(sqlite3.connect(db)) as source, closing(
                sqlite3.connect(output)
            ) as compacted:
                self.assertEqual(
                    {
                        table: _table_full_hash(source, table)
                        for table in protected_tables
                    },
                    before,
                )
                self.assertEqual(
                    {
                        table: _table_full_hash(compacted, table)
                        for table in protected_tables
                    },
                    before,
                )
            compacted_recorder = ReviewRecorder(
                output,
                logging.getLogger("n16-vacuum-first-start"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                compacted_recorder.inspect_passed_structure(
                    "N16", "N16USDT", structure_id
                ),
                "CONSUMED",
            )
            with closing(sqlite3.connect(output)) as connection:
                catalog = connection.execute(
                    "PRAGMA schema_version"
                ).fetchone()[0]
                self.assertEqual(
                    connection.execute(
                        "SELECT catalog_schema_version, sealed_claim_count "
                        "FROM n16_lifecycle_guard"
                    ).fetchone(),
                    (catalog, 1),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT catalog_schema_version, sealed_claim_count "
                        "FROM strategy_lifecycle_installations"
                    ).fetchone(),
                    (catalog, 1),
                )
            with patch(
                "trading_bot.recorder._validate_n16_permanent_graph",
                side_effect=AssertionError("ordinary restart rescanned N16 history"),
            ):
                ReviewRecorder(
                    output,
                    logging.getLogger("n16-vacuum-o1"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )

            checkpoint_delete_mode(output)
            with closing(sqlite3.connect(output)) as connection:
                before_second = {
                    table: _table_full_hash(connection, table)
                    for table in protected_tables
                }
            second_parent = Path(root) / "vacuum-output-second"
            second_parent.mkdir()
            second_output = second_parent / "compacted.sqlite3"
            second = vacuum_signal_database_into(
                str(output.resolve()),
                str(second_output.resolve()),
                n16_claim_ledger=str(
                    Path(recorder.n16_claim_ledger_file).resolve()
                ),
            )
            self.assertEqual(second.migration_state, "COMPLETE")
            with closing(sqlite3.connect(output)) as source, closing(
                sqlite3.connect(second_output)
            ) as compacted:
                self.assertEqual(
                    {
                        table: _table_full_hash(source, table)
                        for table in protected_tables
                    },
                    before_second,
                )
                self.assertEqual(
                    {
                        table: _table_full_hash(compacted, table)
                        for table in protected_tables
                    },
                    before_second,
                )
            ReviewRecorder(
                second_output,
                logging.getLogger("n16-vacuum-second-start"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            with patch(
                "trading_bot.recorder._validate_n16_permanent_graph",
                side_effect=AssertionError("second VACUUM stayed unbounded"),
            ):
                ReviewRecorder(
                    second_output,
                    logging.getLogger("n16-vacuum-second-o1"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )

    def test_definition_bootstrap_failure_stops_constructor_before_scheduler(self):
        class AcquiredLock:
            acquired = True

            def release(self):
                raise AssertionError("external lock must not be released")

        class FailingRecorder:
            def upsert_strategy_definitions(self, _strategies):
                raise RuntimeError("definition write failed")

        with tempfile.TemporaryDirectory() as root:
            config = test_config(root)
            with (
                patch(
                    "trading_bot.main.setup_logging",
                    return_value=logging.getLogger("n16-bootstrap"),
                ),
                patch("trading_bot.main.BinanceFuturesClient", return_value=object()),
                patch("trading_bot.main.StateStore", return_value=object()),
                patch("trading_bot.main.ReviewRecorder", return_value=FailingRecorder()),
                patch("trading_bot.main.FundingMonitor", return_value=object()),
                patch("trading_bot.main.Trader", return_value=object()),
                patch("trading_bot.main.PaperTrader") as paper_trader,
                patch("trading_bot.main.StrategyScheduler") as scheduler,
            ):
                with self.assertRaisesRegex(RuntimeError, "definition write failed"):
                    TradingBot(config=config, instance_lock=AcquiredLock())
            paper_trader.assert_not_called()
            scheduler.assert_not_called()

    def test_publish_failure_keeps_confirmation_retryable(self):
        rows, checked = n16_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            scan_id = recorder.begin_scan(1, [candidate()], True)
            scheduler = StrategyScheduler((N16_STRATEGY,), 5, recorder, logging.getLogger("n16"))
            recorder.publish_strategy_signal_batch = lambda *_args: False
            first = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertFalse(first.signal_batch_published)
            states = recorder.get_active_n16_states("N16")
            self.assertEqual([state.stage for state in states], ["CONFIRMED"])
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_consumption_seals"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT sealed_claim_count FROM n16_lifecycle_guard"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM n16_first_claim_witness"
                    ).fetchone(),
                    (0,),
                )
            restarted = ReviewRecorder(
                recorder.db_file,
                logging.getLogger("n16-staged-restart"),
                n16_claim_ledger_file=recorder.n16_claim_ledger_file,
            )
            self.assertEqual(
                [state.stage for state in restarted.get_active_n16_states("N16")],
                ["CONFIRMED"],
            )
            restarted.complete_scan(scan_id, False)
            retry_scan = restarted.begin_scan(1, [candidate()], True)
            retry_scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, restarted, logging.getLogger("n16")
            )
            retried = retry_scheduler.evaluate(
                retry_scan,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertTrue(retried.signal_batch_published)
            self.assertEqual(
                recorder.get_active_n16_states("N16"),
                [],
            )

    def test_metric_history_rejection_is_audited_and_batch_still_publishes(self):
        rows, checked = n16_early_metric_klines()
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            scan_id = recorder.begin_scan(1, [candidate()], True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [candidate()]},
                {"N16USDT": rows},
                checked_at_ms=checked,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 1)
            self.assertFalse(result.signals[0].passed)
            self.assertEqual(
                result.signals[0].reason,
                "N16_METRIC_HISTORY_INCOMPLETE",
            )
            self.assertEqual(recorder.get_active_n16_states("N16"), [])

    def test_restore_failure_with_zero_n16_candidates_blocks_other_strategy(self):
        rows, checked = n16_klines()
        n01, n16 = load_all_strategies()[0], N16_STRATEGY
        other = replace(
            candidate("OTHERUSDT", 1),
            funding_rate=Decimal("-0.02"),
            candidate_universe="negative_funding",
        )
        analysis = AnalysisResult(
            symbol="OTHERUSDT",
            passed=True,
            trend_slope=Decimal("1"),
            pattern="C_UP_PULLBACK_BOUNCE",
            current_bullish=True,
            detail="passed",
            matched_patterns=("C",),
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions((n01, n16))
            scan_id = recorder.begin_scan(1, [other], True)
            scheduler = StrategyScheduler(
                (n01, n16), 5, recorder, logging.getLogger("n16")
            )
            with (
                patch.object(
                    recorder,
                    "get_active_n16_states",
                    side_effect=RuntimeError("restore failed"),
                ),
                patch(
                    "trading_bot.strategy_scheduler.analyze_symbol",
                    return_value=analysis,
                ),
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"negative_funding": [other], "quote_volume_top": []},
                    {"OTHERUSDT": rows},
                    checked_at_ms=checked,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(
                [(item.strategy.strategy_id, item.passed, item.reason)
                 for item in result.signals],
                [("N01", False, "SIGNAL_BATCH_AUDIT_INCOMPLETE")],
            )
            self.assertIsNone(recorder.current_strategy_signal_scan_id())

    def test_missing_dropped_episode_kline_blocks_other_strategy_publish(self):
        rows, checked = n16_klines()
        n01, n16 = load_all_strategies()[0], N16_STRATEGY
        frozen = analyze_n16_mature_trend_support(
            "DROPUSDT", rows, quote_volume_rank=9, checked_at_ms=checked
        )
        other = replace(
            candidate("OTHERUSDT", 1),
            funding_rate=Decimal("-0.02"),
            candidate_universe="negative_funding",
        )
        analysis = AnalysisResult(
            "OTHERUSDT",
            True,
            Decimal("1"),
            "C_UP_PULLBACK_BOUNCE",
            True,
            "passed",
            ("C",),
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions((n01, n16))
            self.assertEqual(
                recorder.record_n16_state(frozen.state_record), "INSERTED"
            )
            scan_id = recorder.begin_scan(1, [other], True)
            scheduler = StrategyScheduler(
                (n01, n16), 5, recorder, logging.getLogger("n16")
            )
            with patch(
                "trading_bot.strategy_scheduler.analyze_symbol",
                return_value=analysis,
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {"negative_funding": [other], "quote_volume_top": []},
                    {"OTHERUSDT": rows},
                    checked_at_ms=checked,
                )
            self.assertFalse(result.signal_batch_published)
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertIsNone(recorder.current_strategy_signal_scan_id())

    def test_active_episode_survives_top100_drop_and_is_still_evaluated(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "DROPUSDT", rows, quote_volume_rank=9, checked_at_ms=checked
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(recorder.record_n16_state(analysis.state_record), "INSERTED")
            self.assertEqual(
                recorder.get_required_n16_episode_symbols("N16"),
                ("DROPUSDT",),
            )
            scan_id = recorder.begin_scan(0, [], True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": []},
                {"DROPUSDT": deepcopy(rows)},
                checked_at_ms=checked,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(result.signals, [])
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_id
            )
            self.assertEqual(
                recorder.get_required_n16_episode_symbols("N16"),
                ("DROPUSDT",),
            )

    def test_dropped_active_episode_obeys_bulk_global_cooldown(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "DROPUSDT", rows, quote_volume_rank=9, checked_at_ms=checked
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(
                recorder.record_n16_state(analysis.state_record),
                "INSERTED",
            )
            self.assertTrue(
                recorder.set_symbol_cooldown(
                    "DROPUSDT",
                    datetime.now(timezone.utc) + timedelta(hours=4),
                    "TEST_GLOBAL_COOLDOWN",
                    None,
                )
            )
            scan_id = recorder.begin_scan(0, [], True)
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            with patch.object(
                recorder,
                "active_symbol_cooldowns",
                wraps=recorder.active_symbol_cooldowns,
            ) as bulk_read, patch.object(
                recorder,
                "active_symbol_cooldown",
                wraps=recorder.active_symbol_cooldown,
            ) as single_read:
                result = scheduler.evaluate(
                    scan_id,
                    {"quote_volume_top": []},
                    {"DROPUSDT": deepcopy(rows)},
                    checked_at_ms=checked,
                )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(result.signals, [])
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            self.assertEqual(
                recorder.current_strategy_signal_scan_id(), scan_id
            )
            self.assertEqual(bulk_read.call_count, 1)
            self.assertEqual(single_read.call_count, 0)
            with recorder._read_only_runtime_snapshot() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_signals "
                        "WHERE scan_id=?",
                        (scan_id,),
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links"
                    ).fetchone(),
                    (0,),
                )

    def test_schema_is_exact_and_evidence_is_bounded(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=7, checked_at_ms=checked
        )
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            recorder = self.make_recorder(root)
            self.assertEqual(recorder.record_n16_state(analysis.state_record), "INSERTED")
            self.assertLess(
                len(analysis.state_record.evidence_json.encode("utf-8")),
                128 * 1024,
            )
            with closing(sqlite3.connect(db)) as connection:
                columns = [
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(n16_trend_support_states)"
                    )
                ]
            self.assertEqual(
                columns,
                [
                    "id", "strategy_id", "symbol", "episode_id",
                    "structure_id", "stage", "reason", "quote_volume_rank",
                    "evidence_json", "evidence_sha256", "created_at", "updated_at",
                ],
            )

    def test_all_intrinsic_passes_are_permanent_but_only_rank_winner_claims(self):
        rows, checked = n16_klines()
        first_candidate = candidate("AAAUSDT", 3)
        second_candidate = candidate("BBBUSDT", 8)
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            scan_id = recorder.begin_scan(
                2, [first_candidate, second_candidate], True
            )
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [second_candidate, first_candidate]},
                {"AAAUSDT": deepcopy(rows), "BBBUSDT": deepcopy(rows)},
                checked_at_ms=checked,
            )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.signals), 2)
            self.assertEqual(
                [(item.candidate.symbol, item.reason) for item in result.signals],
                [("AAAUSDT", "PASSED"), ("BBBUSDT", "N16_NOT_REPRESENTATIVE")],
            )
            self.assertEqual(
                [item.candidate.symbol for item in result.passed_signals],
                ["AAAUSDT"],
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                rows_in_db = connection.execute(
                    "SELECT symbol, stage, reason, evidence_json "
                    "FROM n16_trend_support_states ORDER BY symbol"
                ).fetchall()
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                        "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                        "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'"
                    ).fetchone()[0],
                    1,
                )
            self.assertEqual(
                [(row[0], row[1], row[2]) for row in rows_in_db],
                [
                    ("AAAUSDT", "CONFIRMED", "PASSED"),
                    ("BBBUSDT", "CONFIRMED", "PASSED"),
                ],
            )
            for row in rows_in_db:
                decoded = decode_n16_state_envelope(
                    json.loads(row[3]), "N16", row[0]
                )
                self.assertIsNotNone(decoded.qualified_observation)
            active = recorder.get_active_n16_states("N16")
            self.assertEqual([state.symbol for state in active], ["BBBUSDT"])

    def test_representative_state_or_signal_failure_never_promotes_runner(self):
        rows, checked = n16_klines()
        winner = candidate("AAAUSDT", 3)
        runner = candidate("BBBUSDT", 8)
        for failure in ("state", "signal"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                recorder = self.make_recorder(root)
                recorder.upsert_strategy_definitions(load_all_strategies())
                scan_id = recorder.begin_scan(2, [winner, runner], True)
                scheduler = StrategyScheduler(
                    (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
                )
                if failure == "state":
                    original = recorder.record_n16_state

                    def record_state(record):
                        if record.symbol == "AAAUSDT":
                            return "N16_STATE_PERSIST_FAILED"
                        return original(record)

                    context = patch.object(
                        recorder, "record_n16_state", side_effect=record_state
                    )
                else:
                    def record_signals(_scan_id, records):
                        return StrategySignalBatchWriteResult(
                            failed_index=next(
                                index
                                for index, item in enumerate(records)
                                if item["symbol"] == "AAAUSDT"
                            )
                        )

                    context = patch.object(
                        recorder,
                        "record_strategy_signals",
                        side_effect=record_signals,
                    )
                with context:
                    result = scheduler.evaluate(
                        scan_id,
                        {"quote_volume_top": [runner, winner]},
                        {
                            "AAAUSDT": deepcopy(rows),
                            "BBBUSDT": deepcopy(rows),
                        },
                        checked_at_ms=checked,
                    )
                self.assertFalse(result.signal_batch_published)
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])
                by_symbol = {item.candidate.symbol: item for item in result.signals}
                self.assertFalse(by_symbol["AAAUSDT"].passed)
                self.assertEqual(
                    by_symbol["BBBUSDT"].reason,
                    "N16_NOT_REPRESENTATIVE",
                )
                with closing(sqlite3.connect(recorder.db_file)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_signal_audits "
                            "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'"
                        ).fetchone(),
                        (0,),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM strategy_passed_structure_ledger "
                            "WHERE strategy_id = 'N16' AND claim_state = 'ACTIVE'"
                        ).fetchone(),
                        (0,),
                    )

    def test_same_symbol_old_strategy_overlap_is_audited_without_veto(self):
        rows, checked = n16_klines()
        n01 = load_all_strategies()[0]
        shared = replace(
            candidate("OVERLAPUSDT", 7),
            funding_rate=Decimal("-0.02"),
            candidate_universe="quote_volume_top",
        )
        old_analysis = AnalysisResult(
            symbol="OVERLAPUSDT",
            passed=True,
            trend_slope=Decimal("1"),
            pattern="C_UP_PULLBACK_BOUNCE",
            current_bullish=True,
            detail="passed",
            matched_patterns=("C",),
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions((n01, N16_STRATEGY))
            scan_id = recorder.begin_scan(1, [shared], True)
            scheduler = StrategyScheduler(
                (n01, N16_STRATEGY), 5, recorder, logging.getLogger("n16")
            )
            with patch(
                "trading_bot.strategy_scheduler.analyze_symbol",
                return_value=old_analysis,
            ):
                result = scheduler.evaluate(
                    scan_id,
                    {
                        "negative_funding": [shared],
                        "quote_volume_top": [shared],
                    },
                    {"OVERLAPUSDT": rows},
                    checked_at_ms=checked,
                )
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(
                [(item.strategy.strategy_id, item.passed, item.reason)
                 for item in result.signals],
                [("N01", True, "PASSED"), ("N16", True, "PASSED")],
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                permanent = connection.execute(
                    "SELECT strategy_id, symbol, source_scan_id, claim_state "
                    "FROM strategy_passed_signal_audits "
                    "WHERE source_scan_id = ? AND symbol = ? "
                    "ORDER BY strategy_id",
                    (scan_id, "OVERLAPUSDT"),
                ).fetchall()
            self.assertEqual(
                permanent,
                [
                    ("N01", "OVERLAPUSDT", scan_id, "ACTIVE"),
                    ("N16", "OVERLAPUSDT", scan_id, "ACTIVE"),
                ],
            )

    def test_n16_schema_tampering_fails_before_runtime_writes(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            recorder = self.make_recorder(root)
            checkpoint_delete_mode(db)
            with closing(sqlite3.connect(db)) as connection:
                connection.execute(
                    "DROP INDEX idx_n16_trend_support_structure"
                )
                connection.execute(
                    "CREATE INDEX idx_n16_trend_support_structure "
                    "ON n16_trend_support_states(symbol, structure_id DESC)"
                )
                connection.commit()
            before = zero_write_database_fingerprint(db)
            self.assertEqual(before[2][0], ("delete",))
            with self.assertRaisesRegex(RuntimeError, "N16"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)

    def test_shared_execution_fresh_and_legacy_whitelist_variants(self):
        variants = {
            "fresh": None,
            "pre_columns": (
                _N16_PRE_LASTCHECK_PAPER_TABLE_SQL,
                _N16_LEGACY_STRATEGY_STATES_TABLE_SQL,
            ),
            "migrated_columns": (
                _N16_LEGACY_PAPER_TABLE_SQL,
                _N16_MIGRATED_STRATEGY_STATES_TABLE_SQL,
            ),
        }
        for variant, legacy_sql in variants.items():
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as root:
                db = Path(root) / "review.sqlite3"
                ledger = Path(root) / "n16_claim_ledger.sqlite3"
                if legacy_sql is None:
                    with closing(sqlite3.connect(db)) as connection:
                        connection.commit()
                else:
                    with closing(sqlite3.connect(db)) as connection:
                        connection.execute(legacy_sql[0])
                        connection.execute(
                            "CREATE INDEX idx_strategy_paper_open "
                            "ON strategy_paper_trades(strategy_id, result)"
                        )
                        connection.execute(
                            "CREATE INDEX idx_strategy_paper_symbol_result "
                            "ON strategy_paper_trades("
                            "strategy_id, symbol, result, closed_at)"
                        )
                        connection.execute(legacy_sql[1])
                        connection.commit()
                summary = _install_n16_claim_boundary(db, ledger)
                self.assertEqual(summary.confirmed_claim_count, 0)
                _install_n17_lifecycle_boundary(db, ledger)
                _install_n19_lifecycle_boundary(db, ledger)
                _install_n18_lifecycle_boundary(db, ledger)
                _install_n20_lifecycle_boundary(db, ledger)
                _install_micro_lifecycle_boundary(db, ledger)
                _install_coverage_epoch_boundary(db, ledger)
                recorder = ReviewRecorder(
                    db,
                    logging.getLogger("n16-shared-whitelist-%s" % variant),
                    n16_claim_ledger_file=ledger,
                )
                self.assertEqual(
                    recorder.n16_claim_ledger.metadata_phase(),
                    "READY",
                )
                with closing(sqlite3.connect(db)) as connection:
                    tables = {
                        row[0]
                        for row in connection.execute(
                            "SELECT name FROM sqlite_schema WHERE type='table'"
                        ).fetchall()
                    }
                    self.assertTrue(
                        {
                            "trade_reviews",
                            "strategy_live_links",
                            "strategy_paper_trades",
                            "strategy_states",
                            "symbol_cooldowns",
                        }
                        <= tables
                    )
                    for table, index_name in (
                        (
                            "strategy_states",
                            "sqlite_autoindex_strategy_states_1",
                        ),
                        (
                            "symbol_cooldowns",
                            "sqlite_autoindex_symbol_cooldowns_1",
                        ),
                    ):
                        identities = {
                            row[1]: tuple(row[2:5])
                            for row in connection.execute(
                                'PRAGMA index_list("%s")' % table
                            ).fetchall()
                        }
                        self.assertEqual(
                            identities[index_name],
                            (1, "pk", 0),
                        )
                    paper_columns = [
                        row[1]
                        for row in connection.execute(
                            "PRAGMA table_xinfo(strategy_paper_trades)"
                        ).fetchall()
                    ]
                    state_columns = [
                        row[1]
                        for row in connection.execute(
                            "PRAGMA table_xinfo(strategy_states)"
                        ).fetchall()
                    ]
                    self.assertIn("last_checked_at", paper_columns)
                    self.assertIn("live_result_pending", state_columns)
                    if variant == "fresh":
                        self.assertLess(
                            paper_columns.index("last_checked_at"),
                            paper_columns.index("closed_at"),
                        )
                        self.assertLess(
                            state_columns.index("live_result_pending"),
                            state_columns.index("last_trade_result"),
                        )
                    else:
                        self.assertEqual(paper_columns[-1], "last_checked_at")
                        self.assertEqual(
                            state_columns[-1],
                            "live_result_pending",
                        )

    def test_historical_trade_reviews_migration_is_exactly_whitelisted(self):
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            db = Path(recorder.db_file)
            ledger = Path(recorder.n16_claim_ledger_file)
            downgrade_test_pair_to_pre_n16(db, ledger)
            with closing(sqlite3.connect(db)) as connection:
                rebuild_trade_reviews_as_historical_migration(connection)
                connection.execute(
                    "INSERT INTO trade_reviews("
                    "id,scan_id,opened_at,symbol,side,quantity,entry_price,"
                    "stop_loss_price,take_profit_price,dry_run,status,"
                    "orders_json,target_risk_amount,actual_risk_amount,"
                    "closed_at,exit_reason,balance_after_close"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        41,
                        None,
                        "2025-01-02T03:04:05+00:00",
                        "LEGACYUSDT",
                        "LONG",
                        "1.25",
                        "10",
                        "9",
                        "15",
                        1,
                        "CLOSED_TAKE_PROFIT",
                        '{"legacy":true}',
                        "1",
                        "0.9",
                        "2025-01-02T04:04:05+00:00",
                        "TAKE_PROFIT",
                        "1005",
                    ),
                )
                connection.commit()
                before_columns = tuple(
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_xinfo(trade_reviews)"
                    ).fetchall()
                )
                before_rows = tuple(
                    tuple(row)
                    for row in connection.execute(
                        "SELECT * FROM trade_reviews ORDER BY id"
                    ).fetchall()
                )
            checkpoint_delete_mode(db)
            self.assertFalse(ledger.exists())

            installed = _install_n16_claim_boundary(db, ledger)
            self.assertEqual(installed.confirmed_claim_count, 0)
            # This fixture intentionally preserves the already-installed
            # downstream coverage-family Review schema while rebuilding only
            # N16.  Complete the matching EMPTY ledger mirror generation
            # before constructing a current runtime pair.
            N16PermanentClaimLedger(
                ledger
            ).install_legacy_witness_mirror_schema(
                "2026-07-27T00:00:00+00:00"
            )
            install_test_protected_generation_highwater(db, ledger)
            restarted = ReviewRecorder(
                db,
                logging.getLogger("n16-historical-trade-restart"),
                n16_claim_ledger_file=ledger,
            )
            self.assertEqual(
                restarted.n16_claim_ledger.metadata_phase(),
                "READY",
            )
            with closing(sqlite3.connect(db)) as connection:
                actual_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema "
                    "WHERE type='table' AND name='trade_reviews'"
                ).fetchone()[0]
                actual_xinfo = tuple(
                    tuple(row[1:7])
                    for row in connection.execute(
                        "PRAGMA table_xinfo(trade_reviews)"
                    ).fetchall()
                )
                after_columns = tuple(
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_xinfo(trade_reviews)"
                    ).fetchall()
                )
                after_rows = tuple(
                    tuple(row)
                    for row in connection.execute(
                        "SELECT * FROM trade_reviews ORDER BY id"
                    ).fetchall()
                )
            self.assertEqual(
                _n16_normalized_sql(actual_sql),
                _n16_normalized_sql(
                    _N16_MIGRATED_TRADE_REVIEWS_TABLE_SQL
                ),
            )
            self.assertEqual(
                actual_xinfo,
                _N16_MIGRATED_TRADE_REVIEWS_XINFO,
            )
            self.assertEqual(after_columns, before_columns)
            self.assertEqual(after_rows, before_rows)
            self.assertEqual(after_rows[0][0], 41)

    def test_historical_trade_reviews_drift_is_zero_write_rejected(self):
        attacks = (
            "check",
            "generated",
            "foreign_key",
            "same_name_expression",
            "trigger",
        )
        for phase in ("PRE_N16", "CURRENT"):
            for attack in attacks:
                with self.subTest(
                    phase=phase,
                    attack=attack,
                ), tempfile.TemporaryDirectory() as root:
                    recorder = self.make_recorder(root)
                    db = Path(recorder.db_file)
                    ledger = Path(recorder.n16_claim_ledger_file)
                    downgrade_test_pair_to_pre_n16(db, ledger)
                    with closing(sqlite3.connect(db)) as connection:
                        rebuild_trade_reviews_as_historical_migration(
                            connection
                        )
                        connection.commit()
                    if phase == "CURRENT":
                        _install_n16_claim_boundary(db, ledger)
                    checkpoint_delete_mode(db)
                    if phase == "CURRENT":
                        checkpoint_delete_mode(ledger)
                    with closing(sqlite3.connect(db)) as connection:
                        if attack == "same_name_expression":
                            connection.execute(
                                "DROP INDEX idx_trade_reviews_symbol_time"
                            )
                            connection.execute(
                                "CREATE INDEX idx_trade_reviews_symbol_time "
                                "ON trade_reviews(lower(symbol), opened_at)"
                            )
                        else:
                            tamper_shared_execution_table(
                                connection,
                                "trade_reviews",
                                attack,
                            )
                        connection.commit()
                    checkpoint_delete_mode(db)
                    before_review = zero_write_database_fingerprint(db)
                    if phase == "CURRENT":
                        before_ledger = zero_write_database_fingerprint(
                            ledger
                        )
                        with self.assertRaisesRegex(RuntimeError, "N16"):
                            ReviewRecorder(
                                db,
                                logging.getLogger(
                                    "n16-historical-trade-%s" % attack
                                ),
                                n16_claim_ledger_file=ledger,
                            )
                        self.assertEqual(
                            zero_write_database_fingerprint(ledger),
                            before_ledger,
                        )
                    else:
                        self.assertFalse(ledger.exists())
                        with self.assertRaises(
                            SignalRetentionMaintenanceError
                        ):
                            _install_n16_claim_boundary(db, ledger)
                        self.assertFalse(ledger.exists())
                    self.assertEqual(
                        zero_write_database_fingerprint(db),
                        before_review,
                    )

    def test_same_catalog_writes_reuse_only_full_schema_attestation(self):
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            db = Path(recorder.db_file)
            original_verify = recorder_module._verify_n16_schema
            full_schema_calls = 0

            def counted_verify(*args, **kwargs):
                nonlocal full_schema_calls
                full_schema_calls += 1
                return original_verify(*args, **kwargs)

            with patch.object(
                recorder_module,
                "_verify_n16_schema",
                side_effect=counted_verify,
            ), patch.object(
                recorder.n16_claim_ledger,
                "attest",
                wraps=recorder.n16_claim_ledger.attest,
            ) as ledger_attest:
                for index in range(20):
                    self.assertTrue(
                        recorder.record_event(
                            "n16-same-catalog-%d" % index,
                            {"index": index},
                        )
                    )
                self.assertEqual(full_schema_calls, 0)
                self.assertEqual(ledger_attest.call_count, 40)

                with closing(sqlite3.connect(db)) as connection:
                    version = connection.execute(
                        "PRAGMA schema_version"
                    ).fetchone()[0]
                    connection.execute(
                        "CREATE TABLE catalog_generation_probe(value INTEGER)"
                    )
                    connection.commit()
                self.assertFalse(
                    recorder.record_event(
                        "n16-new-catalog-generation",
                        {"generation_before": version},
                    )
                )
                changed_catalog_calls = full_schema_calls
                attest_after_change = ledger_attest.call_count

                for index in range(5):
                    self.assertFalse(
                        recorder.record_event(
                            "n16-reused-new-catalog-%d" % index,
                            {"index": index},
                        )
                    )
                self.assertEqual(full_schema_calls, changed_catalog_calls + 5)
                self.assertEqual(
                    ledger_attest.call_count,
                    attest_after_change + 5,
                )

                with closing(sqlite3.connect(db)) as connection:
                    before_events = connection.execute(
                        "SELECT COUNT(*) FROM events"
                    ).fetchone()[0]
                    connection.execute(
                        "DROP INDEX idx_n16_trend_support_active"
                    )
                    connection.execute(
                        "CREATE INDEX idx_n16_trend_support_active "
                        "ON n16_trend_support_states("
                        "strategy_id, stage, symbol DESC)"
                    )
                    connection.commit()
                before_hostile_calls = full_schema_calls
                self.assertFalse(
                    recorder.record_event(
                        "n16-hostile-catalog-must-not-write",
                        {"blocked": True},
                    )
                )
                self.assertEqual(full_schema_calls, before_hostile_calls + 1)
                with closing(sqlite3.connect(db)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM events"
                        ).fetchone()[0],
                        before_events,
                    )

    def test_every_n16_index_metadata_drift_is_zero_write_rejected(self):
        cases = {
            "idx_n16_trend_support_active": (
                "CREATE INDEX idx_n16_trend_support_active "
                "ON n16_trend_support_states(strategy_id, stage, symbol DESC)"
            ),
            "idx_n16_trend_support_structure": (
                "CREATE INDEX idx_n16_trend_support_structure "
                "ON n16_trend_support_states(strategy_id, structure_id) "
                "WHERE structure_id IS NOT NULL"
            ),
            "idx_n16_live_link_state_key": (
                "CREATE INDEX idx_n16_live_link_state_key "
                "ON strategy_live_links(symbol COLLATE NOCASE, opened_at)"
            ),
            "idx_n16_live_link_review": (
                "CREATE INDEX idx_n16_live_link_review "
                "ON strategy_live_links(trade_review_id DESC)"
            ),
            "idx_n16_trade_review_state_key": (
                "CREATE INDEX idx_n16_trade_review_state_key "
                "ON trade_reviews(opened_at, symbol)"
            ),
            "idx_n16_review_pending_rows": (
                "CREATE INDEX idx_n16_review_pending_rows "
                "ON trade_reviews(id) WHERE status = 'OPENED'"
            ),
            "idx_n16_paper_open_rows": (
                "CREATE INDEX idx_n16_paper_open_rows "
                "ON strategy_paper_trades(id)"
            ),
            "idx_n16_live_open_rows": (
                "CREATE INDEX idx_n16_live_open_rows "
                "ON strategy_live_links(id) WHERE closed_at IS NOT NULL"
            ),
            "idx_n16_terminal_event_trade": (
                "CREATE UNIQUE INDEX idx_n16_terminal_event_trade "
                "ON events(trade_review_id DESC) WHERE event_type IN ("
                "'n16_dry_run_position_closed', "
                "'n16_live_position_closed')"
            ),
        }
        self.assertEqual(set(cases), set(_N16_INDEX_SQL))

        for index_name, replacement_sql in cases.items():
            with self.subTest(index=index_name), tempfile.TemporaryDirectory() as root:
                recorder = self.make_recorder(root)
                db = Path(recorder.db_file)
                ledger = Path(recorder.n16_claim_ledger_file)
                checkpoint_delete_mode(db)
                checkpoint_delete_mode(ledger)
                with closing(sqlite3.connect(db)) as connection:
                    connection.execute('DROP INDEX "%s"' % index_name)
                    connection.execute(replacement_sql)
                    connection.commit()
                checkpoint_delete_mode(db)
                before_review = zero_write_database_fingerprint(db)
                before_ledger = zero_write_database_fingerprint(ledger)
                with self.assertRaisesRegex(RuntimeError, "N16"):
                    ReviewRecorder(
                        db,
                        logging.getLogger("n16-index-metadata-%s" % index_name),
                        n16_claim_ledger_file=ledger,
                    )
                self.assertEqual(
                    zero_write_database_fingerprint(db), before_review
                )
                self.assertEqual(
                    zero_write_database_fingerprint(ledger), before_ledger
                )

    def test_pre_and_current_shared_execution_schema_drift_is_zero_write(self):
        tables = (
            "trade_reviews",
            "strategy_live_links",
            "strategy_paper_trades",
            "strategy_states",
            "symbol_cooldowns",
        )
        attacks = (
            "check",
            "generated",
            "foreign_key",
            "extra_unique",
            "expression_index",
            "trigger",
        )
        for phase in ("PRE_N16", "CURRENT"):
            for table in tables:
                for attack in attacks:
                    with self.subTest(
                        phase=phase,
                        table=table,
                        attack=attack,
                    ), tempfile.TemporaryDirectory() as root:
                        recorder = self.make_recorder(root)
                        db = Path(recorder.db_file)
                        ledger = Path(recorder.n16_claim_ledger_file)
                        if phase == "PRE_N16":
                            downgrade_test_pair_to_pre_n16(db, ledger)
                        else:
                            checkpoint_delete_mode(db)
                            checkpoint_delete_mode(ledger)
                        with closing(sqlite3.connect(db)) as connection:
                            tamper_shared_execution_table(
                                connection,
                                table,
                                attack,
                            )
                            connection.commit()
                        checkpoint_delete_mode(db)
                        before_review = zero_write_database_fingerprint(db)
                        if phase == "CURRENT":
                            before_ledger = zero_write_database_fingerprint(
                                ledger
                            )
                            with self.assertRaisesRegex(RuntimeError, "N16"):
                                ReviewRecorder(
                                    db,
                                    logging.getLogger(
                                        "n16-current-shared-%s-%s"
                                        % (table, attack)
                                    ),
                                    n16_claim_ledger_file=ledger,
                                )
                            self.assertEqual(
                                zero_write_database_fingerprint(ledger),
                                before_ledger,
                            )
                        else:
                            self.assertFalse(ledger.exists())
                            with self.assertRaises(
                                SignalRetentionMaintenanceError
                            ):
                                _install_n16_claim_boundary(db, ledger)
                            self.assertFalse(ledger.exists())
                        self.assertEqual(
                            zero_write_database_fingerprint(db),
                            before_review,
                        )

    def test_pre_and_current_shared_index_metadata_drift_is_zero_write(self):
        cases = {
            "idx_trade_reviews_symbol_time": (
                "CREATE INDEX idx_trade_reviews_symbol_time "
                "ON trade_reviews(lower(symbol), opened_at)"
            ),
            "idx_strategy_live_result": (
                "CREATE INDEX idx_strategy_live_result "
                "ON strategy_live_links(strategy_id, symbol, result, "
                "closed_at DESC)"
            ),
            "idx_strategy_paper_open": (
                "CREATE UNIQUE INDEX idx_strategy_paper_open "
                "ON strategy_paper_trades(strategy_id, result)"
            ),
            "idx_strategy_paper_symbol_result": (
                "CREATE INDEX idx_strategy_paper_symbol_result "
                "ON strategy_paper_trades(strategy_id, symbol COLLATE NOCASE, "
                "result, closed_at)"
            ),
            "sqlite_autoindex_strategy_states_1": None,
            "sqlite_autoindex_symbol_cooldowns_1": None,
            "idx_symbol_cooldowns_until": (
                "CREATE INDEX idx_symbol_cooldowns_until "
                "ON symbol_cooldowns(cooldown_until DESC)"
            ),
        }
        expected = {
            index_name
            for table_indexes in _N16_SHARED_INDEX_SQL.values()
            for index_name in table_indexes
            if index_name not in _N16_INDEX_SQL
        }
        self.assertEqual(set(cases), expected)
        auto_tables = {
            "sqlite_autoindex_strategy_states_1": (
                "strategy_states",
                "strategy_id TEXT PRIMARY KEY",
                "strategy_id TEXT UNIQUE",
            ),
            "sqlite_autoindex_symbol_cooldowns_1": (
                "symbol_cooldowns",
                "symbol TEXT PRIMARY KEY",
                "symbol TEXT UNIQUE",
            ),
        }

        for phase in ("PRE_N16", "CURRENT"):
            for index_name, replacement_sql in cases.items():
                with self.subTest(
                    phase=phase,
                    index=index_name,
                ), tempfile.TemporaryDirectory() as root:
                    recorder = self.make_recorder(root)
                    db = Path(recorder.db_file)
                    ledger = Path(recorder.n16_claim_ledger_file)
                    if phase == "PRE_N16":
                        downgrade_test_pair_to_pre_n16(db, ledger)
                    else:
                        checkpoint_delete_mode(db)
                        checkpoint_delete_mode(ledger)
                    with closing(sqlite3.connect(db)) as connection:
                        if replacement_sql is not None:
                            connection.execute(
                                'DROP INDEX "%s"' % index_name
                            )
                            connection.execute(replacement_sql)
                        else:
                            table, primary, unique = auto_tables[index_name]
                            table_sql = connection.execute(
                                "SELECT sql FROM sqlite_schema "
                                "WHERE type='table' AND name=?",
                                (table,),
                            ).fetchone()[0]
                            self.assertIn(primary, table_sql)
                            explicit_indexes = tuple(
                                row[0]
                                for row in connection.execute(
                                    "SELECT sql FROM sqlite_schema "
                                    "WHERE type='index' AND tbl_name=? "
                                    "AND sql IS NOT NULL ORDER BY name",
                                    (table,),
                                ).fetchall()
                            )
                            connection.execute('DROP TABLE "%s"' % table)
                            connection.execute(
                                table_sql.replace(primary, unique, 1)
                            )
                            for index_sql in explicit_indexes:
                                connection.execute(index_sql)
                            origin = {
                                row[1]: tuple(row[2:5])
                                for row in connection.execute(
                                    'PRAGMA index_list("%s")' % table
                                ).fetchall()
                            }
                            self.assertEqual(
                                origin[index_name],
                                (1, "u", 0),
                            )
                        connection.commit()
                    checkpoint_delete_mode(db)
                    before_review = zero_write_database_fingerprint(db)
                    if phase == "CURRENT":
                        before_ledger = zero_write_database_fingerprint(ledger)
                        with self.assertRaisesRegex(RuntimeError, "N16"):
                            ReviewRecorder(
                                db,
                                logging.getLogger(
                                    "n16-current-shared-index-%s" % index_name
                                ),
                                n16_claim_ledger_file=ledger,
                            )
                        self.assertEqual(
                            zero_write_database_fingerprint(ledger),
                            before_ledger,
                        )
                    else:
                        with self.assertRaises(
                            SignalRetentionMaintenanceError
                        ):
                            _install_n16_claim_boundary(db, ledger)
                        self.assertFalse(ledger.exists())
                    self.assertEqual(
                        zero_write_database_fingerprint(db),
                        before_review,
                    )

    def test_n16_schema_literals_triggers_and_incoming_fks_fail_unchanged(self):
        for attack in ("check_literal", "trigger", "incoming_fk"):
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as root:
                db = Path(root) / "review.sqlite3"
                recorder = self.make_recorder(root)
                self.assertTrue(recorder.record_event("n16-schema-guard", {"v": 1}))
                checkpoint_delete_mode(db)
                with closing(sqlite3.connect(db)) as connection:
                    if attack == "check_literal":
                        table_sql = connection.execute(
                            "SELECT sql FROM sqlite_schema "
                            "WHERE type = 'table' "
                            "AND name = 'n16_trend_support_states'"
                        ).fetchone()[0]
                        index_sql = [
                            row[0]
                            for row in connection.execute(
                                "SELECT sql FROM sqlite_schema "
                                "WHERE type = 'index' "
                                "AND tbl_name = 'n16_trend_support_states' "
                                "AND sql IS NOT NULL ORDER BY name"
                            ).fetchall()
                        ]
                        trigger_sql = [
                            row[0]
                            for row in connection.execute(
                                "SELECT sql FROM sqlite_schema "
                                "WHERE type = 'trigger' "
                                "AND tbl_name = 'n16_trend_support_states' "
                                "ORDER BY name"
                            ).fetchall()
                        ]
                        forged_sql = table_sql.replace("'N16'", "'n16'", 1)
                        self.assertNotEqual(forged_sql, table_sql)
                        connection.execute("DROP TABLE n16_trend_support_states")
                        connection.execute(forged_sql)
                        for statement in index_sql:
                            connection.execute(statement)
                        for statement in trigger_sql:
                            connection.execute(statement)
                    elif attack == "trigger":
                        connection.execute(
                            "DROP TRIGGER trg_n16_state_no_replace"
                        )
                        connection.execute(
                            "CREATE TRIGGER trg_n16_state_no_replace "
                            "BEFORE INSERT ON n16_trend_support_states "
                            "BEGIN SELECT RAISE(IGNORE); END"
                        )
                    else:
                        connection.execute(
                            "CREATE TABLE hostile_n16_child("
                            "parent_id INTEGER REFERENCES "
                            "n16_trend_support_states(id))"
                        )
                    connection.commit()
                    before_schema = connection.execute(
                        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                        "ORDER BY type, name"
                    ).fetchall()
                    before_events = connection.execute(
                        "SELECT * FROM events ORDER BY id"
                    ).fetchall()
                before_files = zero_write_database_fingerprint(db)
                with self.assertRaisesRegex(RuntimeError, "N16"):
                    ReviewRecorder(
                        db,
                        logging.getLogger("n16"),
                        n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                    )
                with closing(sqlite3.connect(db)) as connection:
                    after_schema = connection.execute(
                        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
                        "ORDER BY type, name"
                    ).fetchall()
                    after_events = connection.execute(
                        "SELECT * FROM events ORDER BY id"
                    ).fetchall()
                self.assertEqual(after_schema, before_schema)
                self.assertEqual(after_events, before_events)
                self.assertEqual(
                    zero_write_database_fingerprint(db), before_files
                )

    def test_legacy_database_gets_only_explicit_bounded_n16_schema_upgrade(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            recorder = self.make_recorder(root)
            ledger = Path(recorder.n16_claim_ledger_file)
            self.assertTrue(
                recorder.record_event("protected-before-n16-upgrade", {"v": 1})
            )
            with closing(sqlite3.connect(db)) as connection:
                n16_triggers = connection.execute(
                    "SELECT name FROM sqlite_schema "
                    "WHERE type = 'trigger' AND name GLOB 'trg_n16_*'"
                ).fetchall()
                for (trigger_name,) in n16_triggers:
                    connection.execute('DROP TRIGGER "%s"' % trigger_name)
                for index_name in _N16_INDEX_SQL:
                    connection.execute(
                        'DROP INDEX IF EXISTS "%s"' % index_name
                    )
                for table in (
                    "n16_consumption_seals",
                    "n16_first_claim_witness",
                    "n16_lifecycle_guard",
                    "n16_trend_support_states",
                    "strategy_lifecycle_installations",
                ):
                    connection.execute('DROP TABLE "%s"' % table)
                connection.execute("ALTER TABLE events RENAME TO events_n16_old")
                connection.execute(_N16_PREINSTALL_EVENTS_TABLE_SQL)
                connection.execute(
                    "INSERT INTO events(id,occurred_at,event_type,symbol,payload_json) "
                    "SELECT id,occurred_at,event_type,symbol,payload_json "
                    "FROM events_n16_old ORDER BY id"
                )
                connection.execute("DROP TABLE events_n16_old")
                connection.execute(
                    "CREATE INDEX idx_events_type_time "
                    "ON events(event_type, occurred_at)"
                )
                connection.commit()
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_schema "
                        "WHERE name GLOB '*n16*' OR name GLOB 'trg_n16_*' "
                        "OR name = 'strategy_lifecycle_installations'"
                    ).fetchall(),
                    [],
                )
                legacy_event_rows = connection.execute(
                    "SELECT id,occurred_at,event_type,symbol,payload_json "
                    "FROM events ORDER BY id"
                ).fetchall()
                protected_tables = [
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type = 'table' "
                        "AND name NOT LIKE 'n16_%' AND name != 'events' "
                        "ORDER BY name"
                    ).fetchall()
                ]
                protected_before = {
                    table: _table_full_hash(connection, table)
                    for table in protected_tables
                }
            checkpoint_delete_mode(db)
            checkpoint_delete_mode(ledger)
            ledger.unlink()
            _install_n16_claim_boundary(db.resolve(), ledger.resolve())
            N16PermanentClaimLedger(
                ledger
            ).install_legacy_witness_mirror_schema(
                "2026-07-27T00:00:00+00:00"
            )
            install_test_protected_generation_highwater(db, ledger)
            ReviewRecorder(
                db,
                logging.getLogger("n16"),
                n16_claim_ledger_file=ledger,
            )
            with closing(sqlite3.connect(db)) as connection:
                protected_after = {
                    table: _table_full_hash(connection, table)
                    for table in protected_tables
                }
                n16_count = connection.execute(
                    "SELECT COUNT(*) FROM n16_trend_support_states"
                ).fetchone()
                upgraded_event_rows = connection.execute(
                    "SELECT id,occurred_at,event_type,symbol,payload_json,"
                    "trade_review_id FROM events ORDER BY id"
                ).fetchall()
                event_sql = connection.execute(
                    "SELECT sql FROM sqlite_schema "
                    "WHERE type='table' AND name='events'"
                ).fetchone()
                event_xinfo = tuple(
                    tuple(row[1:7])
                    for row in connection.execute(
                        "PRAGMA table_xinfo(events)"
                    ).fetchall()
                )
                event_indexes = {
                    row[1]: tuple(row[2:5])
                    for row in connection.execute(
                        "PRAGMA index_list(events)"
                    ).fetchall()
                }
                event_trigger_sql = {
                    name: sql
                    for name, sql in connection.execute(
                        "SELECT name,sql FROM sqlite_schema "
                        "WHERE type='trigger' AND tbl_name='events' "
                        "ORDER BY name"
                    ).fetchall()
                }
            self.assertEqual(protected_after, protected_before)
            self.assertEqual(n16_count, (0,))
            self.assertEqual(
                upgraded_event_rows,
                [tuple(row) + (None,) for row in legacy_event_rows],
            )
            self.assertIsNotNone(event_sql)
            self.assertEqual(
                _n16_normalized_sql(event_sql[0]),
                _n16_normalized_sql(_N16_EVENTS_TABLE_SQL),
            )
            self.assertEqual(
                event_xinfo,
                (
                    ("id", "INTEGER", 0, None, 1, 0),
                    ("occurred_at", "TEXT", 1, None, 0, 0),
                    ("event_type", "TEXT", 1, None, 0, 0),
                    ("symbol", "TEXT", 0, None, 0, 0),
                    ("payload_json", "TEXT", 1, None, 0, 0),
                    ("trade_review_id", "INTEGER", 0, None, 0, 0),
                ),
            )
            self.assertEqual(
                event_indexes,
                {
                    "idx_events_type_time": (0, "c", 0),
                    "idx_n16_terminal_event_trade": (1, "c", 1),
                },
            )
            expected_event_triggers = {
                name: sql
                for name, sql in _N16_TRIGGER_SQL.items()
                if name.startswith("trg_n16_terminal_event_identity")
            }
            self.assertEqual(
                set(event_trigger_sql), set(expected_event_triggers)
            )
            self.assertEqual(
                {
                    name: _n16_normalized_sql(sql)
                    for name, sql in event_trigger_sql.items()
                },
                {
                    name: _n16_normalized_sql(sql)
                    for name, sql in expected_event_triggers.items()
                },
            )

    def test_post_n16_empty_history_schema_removal_is_zero_write_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "review.sqlite3"
            recorder = self.make_recorder(root)
            checkpoint_delete_mode(db)
            with closing(sqlite3.connect(db)) as connection:
                for (trigger_name,) in connection.execute(
                    "SELECT name FROM sqlite_schema "
                    "WHERE type = 'trigger' AND name GLOB 'trg_n16_*'"
                ).fetchall():
                    connection.execute('DROP TRIGGER "%s"' % trigger_name)
                for table in (
                    "n16_consumption_seals",
                    "n16_first_claim_witness",
                    "n16_lifecycle_guard",
                    "n16_trend_support_states",
                ):
                    connection.execute('DROP TABLE "%s"' % table)
                connection.commit()
                self.assertEqual(
                    connection.execute(
                        "SELECT strategy_id, schema_version, rule_version, "
                        "sealed_claim_count "
                        "FROM strategy_lifecycle_installations"
                    ).fetchall(),
                    [("N16", 1, "N16_V1", 0)],
                )
            before = zero_write_database_fingerprint(db)
            with self.assertRaisesRegex(RuntimeError, "N16"):
                ReviewRecorder(
                    db,
                    logging.getLogger("n16-post-install"),
                    n16_claim_ledger_file=recorder.n16_claim_ledger_file,
                )
            self.assertEqual(zero_write_database_fingerprint(db), before)

    def test_terminal_roll_and_fresh_replay_preserve_full_row(self):
        first_rows, checked = n16_klines(elapsed_ms=30_000)
        first = analyze_n16_mature_trend_support(
            "N16USDT", first_rows, quote_volume_rank=7, checked_at_ms=checked
        )
        next_candle = kline(
            122, "108.1", "108.4", "107.9", "108.2", "105", "57"
        )
        rolled = deepcopy(first_rows[1:]) + [next_candle]
        terminal = analyze_n16_mature_trend_support(
            "N16USDT",
            rolled,
            quote_volume_rank=7,
            frozen_evidence=first.state_record.evidence_json,
            checked_at_ms=next_candle[0] + 30_000,
        )
        self.assertEqual(terminal.reason, "N16_HISTORICAL_ENTRY_MISSED")
        self.assertEqual(
            terminal.state_record.evidence["qualified_observation"],
            first.state_record.evidence["qualified_observation"],
        )
        fresh_terminal = analyze_n16_mature_trend_support(
            "N16USDT",
            rolled,
            quote_volume_rank=7,
            checked_at_ms=next_candle[0] + 30_000,
        )
        self.assertEqual(fresh_terminal.reason, terminal.reason)

        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            self.assertEqual(recorder.record_n16_state(first.state_record), "INSERTED")
            self.assertEqual(recorder.record_n16_state(terminal.state_record), "UPDATED")
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                before = connection.execute(
                    "SELECT * FROM n16_trend_support_states WHERE episode_id = ?",
                    (first.structure.episode_id,),
                ).fetchone()
                count_before = connection.execute(
                    "SELECT COUNT(*) FROM n16_trend_support_states"
                ).fetchone()
            for replay_rank in (7, 8, 100):
                replay = analyze_n16_mature_trend_support(
                    "N16USDT",
                    rolled,
                    quote_volume_rank=replay_rank,
                    checked_at_ms=next_candle[0] + 30_000,
                )
                self.assertEqual(replay.reason, terminal.reason)
                self.assertEqual(
                    recorder.record_n16_state(replay.state_record),
                    "UNCHANGED",
                )
            impossible_terminal_rows = deepcopy(rolled)
            impossible_terminal_rows[-2][7] = "101"
            impossible_terminal_rows[-2][10] = "100"
            impossible_terminal = analyze_n16_mature_trend_support(
                "N16USDT",
                impossible_terminal_rows,
                quote_volume_rank=8,
                checked_at_ms=next_candle[0] + 30_000,
            )
            self.assertEqual(impossible_terminal.reason, terminal.reason)
            self.assertEqual(
                recorder.record_n16_state(impossible_terminal.state_record),
                "N16_STATE_INCONSISTENT",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                after = connection.execute(
                    "SELECT * FROM n16_trend_support_states WHERE episode_id = ?",
                    (first.structure.episode_id,),
                ).fetchone()
                count_after = connection.execute(
                    "SELECT COUNT(*) FROM n16_trend_support_states"
                ).fetchone()
            self.assertEqual(after, before)
            self.assertEqual(count_after, count_before)

    def test_terminal_rank_replay_does_not_block_same_batch_healthy_pass(self):
        rows, checked = n16_klines(elapsed_ms=30_000)
        first = analyze_n16_mature_trend_support(
            "TERMUSDT", rows, quote_volume_rank=7, checked_at_ms=checked
        )
        next_candle = kline(122, "108.1", "108.4", "107.9", "108.2", "105", "57")
        rolled = deepcopy(rows[1:]) + [next_candle]
        terminal = analyze_n16_mature_trend_support(
            "TERMUSDT",
            rolled,
            quote_volume_rank=7,
            frozen_evidence=first.state_record.evidence_json,
            checked_at_ms=next_candle[0] + 30_000,
        )
        healthy = deepcopy(rows)
        for row in healthy:
            row[0] += INTERVAL_MS
            row[6] += INTERVAL_MS

        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(recorder.record_n16_state(terminal.state_record), "INSERTED")
            term_candidate = candidate("TERMUSDT", 8)
            healthy_candidate = candidate("HEALTHUSDT", 1)
            scan_id = recorder.begin_scan(
                2, [healthy_candidate, term_candidate], True
            )
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [healthy_candidate, term_candidate]},
                {"TERMUSDT": rolled, "HEALTHUSDT": healthy},
                checked_at_ms=next_candle[0] + 30_000,
            )
            self.assertTrue(result.signal_batch_published)
            decisions = {
                item.candidate.symbol: (item.passed, item.reason)
                for item in result.signals
            }
            self.assertEqual(decisions["HEALTHUSDT"], (True, "PASSED"))
            self.assertEqual(
                decisions["TERMUSDT"],
                (False, "N16_HISTORICAL_ENTRY_MISSED"),
            )

    def test_first_current_entry_terminal_dominates_same_e_and_rolled_replays(self):
        for mode in ("expiry", "low", "extended"):
            with self.subTest(mode=mode):
                rows, checked = n16_klines(elapsed_ms=30_000)
                first = analyze_n16_mature_trend_support(
                    "N16USDT", rows, quote_volume_rank=7, checked_at_ms=checked
                )
                initial_rows = deepcopy(rows)
                initial_checked = checked
                if mode == "expiry":
                    initial_checked = rows[-1][0] + 120_000
                    initial_reason = "N16_ENTRY_WINDOW_EXPIRED"
                elif mode == "low":
                    initial_rows[-1][3] = str(first.structure.p - Decimal("0.1"))
                    initial_rows[-1][7] = "110"
                    initial_rows[-1][10] = "60"
                    initial_reason = "N16_ENTRY_LOW_BROKE_P"
                else:
                    initial_rows[-1][2] = str(
                        first.structure.entry_max_price + Decimal("0.3")
                    )
                    initial_rows[-1][4] = str(
                        first.structure.entry_max_price + Decimal("0.2")
                    )
                    initial_rows[-1][7] = "110"
                    initial_rows[-1][10] = "60"
                    initial_reason = "N16_ENTRY_PRICE_TOO_EXTENDED"
                initial = analyze_n16_mature_trend_support(
                    "N16USDT",
                    initial_rows,
                    quote_volume_rank=7,
                    checked_at_ms=initial_checked,
                )
                self.assertEqual(initial.reason, initial_reason)

                later_rows = deepcopy(initial_rows)
                if mode in {"expiry", "low"}:
                    later_rows[-1][3] = str(first.structure.p - Decimal("0.2"))
                else:
                    later_rows[-1][2] = str(
                        first.structure.entry_max_price + Decimal("0.4")
                    )
                    later_rows[-1][4] = str(
                        first.structure.entry_min_price + Decimal("0.1")
                    )
                later_rows[-1][7] = "120"
                later_rows[-1][10] = "65"
                later = analyze_n16_mature_trend_support(
                    "N16USDT",
                    later_rows,
                    quote_volume_rank=8,
                    checked_at_ms=rows[-1][0] + 120_000,
                )
                self.assertIn(
                    later.reason,
                    {"N16_ENTRY_LOW_BROKE_P", "N16_ENTRY_WINDOW_EXPIRED"},
                )

                next_candle = kline(
                    122, "108.1", "108.4", "107.9", "108.2", "105", "57"
                )
                rolled = deepcopy(later_rows[1:]) + [next_candle]
                historical = analyze_n16_mature_trend_support(
                    "N16USDT",
                    rolled,
                    quote_volume_rank=8,
                    checked_at_ms=next_candle[0] + 30_000,
                )
                self.assertTrue(historical.reason.startswith("N16_HISTORICAL_"))
                next_candle_2 = kline(
                    123, "108.2", "108.5", "108.0", "108.3", "106", "58"
                )
                rolled_again = deepcopy(rolled[1:]) + [next_candle_2]
                historical_again = analyze_n16_mature_trend_support(
                    "N16USDT",
                    rolled_again,
                    quote_volume_rank=100,
                    checked_at_ms=next_candle_2[0] + 30_000,
                )
                self.assertEqual(historical_again.reason, historical.reason)

                with tempfile.TemporaryDirectory() as root:
                    recorder = self.make_recorder(root)
                    self.assertEqual(
                        recorder.record_n16_state(initial.state_record), "INSERTED"
                    )
                    with closing(sqlite3.connect(recorder.db_file)) as connection:
                        before = connection.execute(
                            "SELECT * FROM n16_trend_support_states"
                        ).fetchall()
                    for replay in (later, historical, historical_again):
                        self.assertEqual(
                            recorder.record_n16_state(replay.state_record),
                            "UNCHANGED",
                        )
                    with closing(sqlite3.connect(recorder.db_file)) as connection:
                        after = connection.execute(
                            "SELECT * FROM n16_trend_support_states"
                        ).fetchall()
                    self.assertEqual(after, before)

    def test_terminal_episode_cannot_revive_or_displace_healthy_winner(self):
        rows, checked = n16_klines(elapsed_ms=30_000)
        baseline = analyze_n16_mature_trend_support(
            "STALEUSDT", rows, quote_volume_rank=1, checked_at_ms=checked
        )
        terminal_rows = deepcopy(rows)
        terminal_rows[-1][2] = str(
            baseline.structure.entry_max_price + Decimal("1.0")
        )
        terminal_rows[-1][4] = str(
            baseline.structure.entry_max_price + Decimal("0.5")
        )
        terminal_rows[-1][7] = "110"
        terminal_rows[-1][10] = "60"
        terminal = analyze_n16_mature_trend_support(
            "STALEUSDT",
            terminal_rows,
            quote_volume_rank=1,
            checked_at_ms=checked,
        )
        self.assertEqual(terminal.reason, "N16_ENTRY_PRICE_TOO_EXTENDED")

        recovered_rows = deepcopy(terminal_rows)
        recovered_rows[-1][2] = str(
            baseline.structure.entry_max_price + Decimal("1.1")
        )
        recovered_rows[-1][4] = str(baseline.structure.entry_min_price)
        recovered_rows[-1][7] = "120"
        recovered_rows[-1][10] = "65"
        recovered = analyze_n16_mature_trend_support(
            "STALEUSDT",
            recovered_rows,
            quote_volume_rank=1,
            checked_at_ms=rows[-1][0] + 60_000,
        )
        self.assertTrue(recovered.passed, recovered.reason)

        waiting_rows = deepcopy(recovered_rows)
        waiting_rows[-1][4] = str(
            baseline.structure.entry_min_price - Decimal("0.1")
        )
        waiting = analyze_n16_mature_trend_support(
            "STALEUSDT",
            waiting_rows,
            quote_volume_rank=1,
            checked_at_ms=rows[-1][0] + 60_000,
        )
        self.assertEqual(waiting.reason, "N16_ENTRY_WAITING_PRICE")

        healthy_rows = deepcopy(rows)
        healthy_candidate = candidate("HEALTHUSDT", 2)
        stale_candidate = candidate("STALEUSDT", 1)
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            recorder.upsert_strategy_definitions(load_all_strategies())
            self.assertEqual(
                recorder.record_n16_state(terminal.state_record), "INSERTED"
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                before = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(
                recorder.record_n16_state(waiting.state_record),
                "N16_EPISODE_CONSUMED",
            )
            self.assertEqual(
                recorder.record_n16_state(recovered.state_record),
                "N16_EPISODE_CONSUMED",
            )

            scan_id = recorder.begin_scan(
                2, [stale_candidate, healthy_candidate], True
            )
            scheduler = StrategyScheduler(
                (N16_STRATEGY,), 5, recorder, logging.getLogger("n16")
            )
            result = scheduler.evaluate(
                scan_id,
                {"quote_volume_top": [stale_candidate, healthy_candidate]},
                {
                    "STALEUSDT": recovered_rows,
                    "HEALTHUSDT": healthy_rows,
                },
                checked_at_ms=rows[-1][0] + 60_000,
            )
            self.assertTrue(result.signal_batch_published)
            decisions = {
                item.candidate.symbol: (item.passed, item.reason)
                for item in result.signals
            }
            self.assertEqual(
                decisions["STALEUSDT"],
                (False, "N16_STRUCTURE_CONSUMED"),
            )
            self.assertEqual(decisions["HEALTHUSDT"], (True, "PASSED"))
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                after = connection.execute(
                    "SELECT * FROM n16_trend_support_states "
                    "WHERE symbol = 'STALEUSDT'"
                ).fetchall()
            self.assertEqual(after, before)

    def test_fixed_window_terminal_dominance_is_strict_and_immutable(self):
        rows, _ = n16_klines()
        rows = deepcopy(rows)
        rows[0] = kline(0, "2250", "2251", "2249", "2250")
        rows[119] = kline(119, "106.5", "106.7", "105.9", "106.1")
        rows[120] = kline(120, "106.1", "106.3", "105.8", "106.0")
        rows[121] = kline(121, "106.2", "106.5", "106.0", "106.405")
        terminal = analyze_n16_mature_trend_support(
            "ROLLUSDT",
            rows,
            quote_volume_rank=7,
            checked_at_ms=rows[-1][0] + 30_000,
        )
        self.assertEqual(
            (terminal.state_record.stage, terminal.reason),
            ("INVALID", "N16_TREND_EMA_ALIGNMENT_NOT_MET"),
        )
        entry = kline(122, "106.405", "106.55", "106.3", "106.45")
        rolled = rows[1:] + [entry]
        fresh = analyze_n16_mature_trend_support(
            "ROLLUSDT",
            rolled,
            quote_volume_rank=100,
            checked_at_ms=entry[0] + 30_000,
        )
        self.assertTrue(fresh.passed, fresh.reason)
        self.assertEqual(
            terminal.structure.episode_id, fresh.structure.episode_id
        )
        decode_n16_state_envelope(terminal.state_record.evidence)
        decode_n16_state_envelope(fresh.state_record.evidence)

        conflicting_rows = deepcopy(rolled)
        conflicting_rows[10][7] = "101"
        conflicting = analyze_n16_mature_trend_support(
            "ROLLUSDT",
            conflicting_rows,
            quote_volume_rank=7,
            checked_at_ms=entry[0] + 30_000,
        )
        self.assertTrue(conflicting.passed, conflicting.reason)
        self.assertEqual(
            terminal.structure.episode_id, conflicting.structure.episode_id
        )

        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            self.assertEqual(
                recorder.record_n16_state(terminal.state_record), "INSERTED"
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                before = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(
                recorder.record_n16_state(fresh.state_record),
                "N16_EPISODE_CONSUMED",
            )
            self.assertEqual(
                recorder.record_n16_state(conflicting.state_record),
                "N16_STATE_INCONSISTENT",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                after = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(after, before)

    def test_first_confirmation_expiry_dominates_seed_drift_reason(self):
        rows, _ = n16_klines()
        rows = deepcopy(rows)
        rows[0] = kline(0, "300", "301", "299", "300")
        rows[119] = kline(
            119, "106.5", "106.7", "105.9", "106.1", "100", "40"
        )
        rows[120] = kline(
            120, "105.95", "105.99", "105.8", "105.9", "100", "40"
        )
        rows[121] = kline(
            121, "105.95", "106.10", "105.8", "106.00465", "100", "40"
        )
        initial = analyze_n16_mature_trend_support(
            "DRIFTUSDT",
            rows,
            quote_volume_rank=7,
            checked_at_ms=rows[-1][0] + 30_000,
        )
        self.assertEqual(initial.reason, "N16_CONFIRMATION_PENDING")
        current = kline(
            122, "106.00465", "106.2", "105.9", "106.0", "100", "40"
        )
        rolled = rows[1:] + [current]
        first_terminal = analyze_n16_mature_trend_support(
            "DRIFTUSDT",
            rolled,
            quote_volume_rank=7,
            frozen_evidence=initial.state_record.evidence_json,
            checked_at_ms=current[0] + 30_000,
        )
        replay_terminal = analyze_n16_mature_trend_support(
            "DRIFTUSDT",
            rolled,
            quote_volume_rank=100,
            checked_at_ms=current[0] + 30_000,
        )
        self.assertEqual(
            (first_terminal.stage, first_terminal.reason)
            if hasattr(first_terminal, "stage")
            else (first_terminal.state_record.stage, first_terminal.reason),
            ("EXPIRED", "N16_CONFIRMATION_BELOW_EMA20"),
        )
        self.assertEqual(
            (replay_terminal.state_record.stage, replay_terminal.reason),
            ("EXPIRED", "N16_CONFIRMATION_TAKER_BUY_RATIO_TOO_LOW"),
        )
        self.assertEqual(
            first_terminal.structure.episode_id,
            replay_terminal.structure.episode_id,
        )
        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            self.assertEqual(
                recorder.record_n16_state(first_terminal.state_record),
                "INSERTED",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                before = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(
                recorder.record_n16_state(replay_terminal.state_record),
                "UNCHANGED",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                after = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(after, before)

    def test_terminal_entry_can_become_new_confirmation_only_monotonically(self):
        rows, _ = n16_klines()
        rows = deepcopy(rows)
        rows[0] = kline(0, "1", "2", "0.5", "1")
        rows[119] = kline(
            119, "105.9", "105.99", "105.8", "105.95", "100", "40"
        )
        rows[120] = kline(
            120, "105.95", "106.1", "105.8", "106.0012", "100", "60"
        )
        rows[121] = kline(
            121, "106.00", "106.3", "105.7", "106.25", "100", "60"
        )
        first_terminal = analyze_n16_mature_trend_support(
            "SHIFTUSDT",
            rows,
            quote_volume_rank=7,
            checked_at_ms=rows[-1][0] + 30_000,
        )
        self.assertEqual(first_terminal.reason, "N16_ENTRY_LOW_BROKE_P")

        entry = kline(122, "106.3", "106.5", "106.2", "106.4")
        rolled = deepcopy(rows[1:])
        rolled[-1] = kline(
            121, "106.00", "106.4", "105.65", "106.3", "110", "65"
        )
        rolled.append(entry)
        replay_terminal = analyze_n16_mature_trend_support(
            "SHIFTUSDT",
            rolled,
            quote_volume_rank=100,
            checked_at_ms=entry[0] + 30_000,
        )
        self.assertEqual(
            replay_terminal.reason, "N16_PULLBACK_DEPTH_OUT_OF_RANGE"
        )
        self.assertEqual(
            first_terminal.structure.episode_id,
            replay_terminal.structure.episode_id,
        )

        nonmonotonic_rows = deepcopy(rolled)
        nonmonotonic_rows[-2] = kline(
            121, "106.00", "106.28", "105.7", "106.26", "110", "65"
        )
        nonmonotonic = analyze_n16_mature_trend_support(
            "SHIFTUSDT",
            nonmonotonic_rows,
            quote_volume_rank=7,
            checked_at_ms=entry[0] + 30_000,
        )
        self.assertEqual(
            nonmonotonic.structure.episode_id,
            first_terminal.structure.episode_id,
        )
        decode_n16_state_envelope(nonmonotonic.state_record.evidence)

        with tempfile.TemporaryDirectory() as root:
            recorder = self.make_recorder(root)
            self.assertEqual(
                recorder.record_n16_state(first_terminal.state_record),
                "INSERTED",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                before = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(
                recorder.record_n16_state(replay_terminal.state_record),
                "UNCHANGED",
            )
            self.assertEqual(
                recorder.record_n16_state(nonmonotonic.state_record),
                "N16_STATE_INCONSISTENT",
            )
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                after = connection.execute(
                    "SELECT * FROM n16_trend_support_states"
                ).fetchall()
            self.assertEqual(after, before)

    def test_explicit_install_keeps_original_review_scope_across_ledger_create(self):
        for restore_before_open in (False, True):
            with self.subTest(
                restore_before_open=restore_before_open
            ), tempfile.TemporaryDirectory() as root:
                app_root = Path(root) / "binance-app"
                data_parent = app_root / "data"
                state_parent = app_root / "state"
                data_parent.mkdir(parents=True)
                state_parent.mkdir(parents=True)
                review = data_parent / "review.sqlite3"
                ledger = state_parent / "n16_claim_ledger.sqlite3"
                with closing(sqlite3.connect(review)) as connection:
                    connection.execute(
                        "CREATE TABLE legacy_marker(id INTEGER PRIMARY KEY, value TEXT)"
                    )
                    connection.execute(
                        "INSERT INTO legacy_marker VALUES(1,'preserve')"
                    )
                    connection.commit()
                checkpoint_delete_mode(review)
                original_review_fingerprint = zero_write_database_fingerprint(
                    review
                )
                original_parent_identity = _directory_identity(
                    data_parent, "test Review parent"
                )
                scope = _DatabaseFileScope(
                    path=review.resolve(),
                    main_identity=_regular_file_identity(
                        review, "test Review database"
                    ),
                    parent_identity=original_parent_identity,
                    sidecar_identities=_capture_database_sidecars(
                        review, "test Review database"
                    ),
                    roots=(app_root.resolve(),),
                )
                held_parent = app_root / "data-held"
                replacement_entries = []
                real_create = N16PermanentClaimLedger._create
                external_sentinel_fingerprint = []

                def create_then_replace(ledger_instance, *, now):
                    real_create(ledger_instance, now=now)
                    data_parent.rename(held_parent)
                    data_parent.mkdir()
                    if restore_before_open:
                        replacement_entries.extend(data_parent.iterdir())
                        data_parent.rmdir()
                        held_parent.rename(data_parent)
                    else:
                        sentinel = data_parent / "external-sentinel.txt"
                        sentinel.write_text("preserve", encoding="utf-8")
                        external_sentinel_fingerprint.append(
                            (
                                sentinel.read_bytes(),
                                sentinel.lstat().st_ino,
                                sentinel.lstat().st_nlink,
                            )
                        )

                try:
                    with patch.object(
                        N16PermanentClaimLedger,
                        "_create",
                        new=create_then_replace,
                    ):
                        if restore_before_open:
                            summary = _install_n16_claim_boundary(
                                review.resolve(),
                                ledger.resolve(),
                                database_scope=scope,
                            )
                            self.assertEqual(summary.confirmed_claim_count, 0)
                        else:
                            with self.assertRaisesRegex(
                                SignalRetentionMaintenanceError,
                                "did not complete",
                            ):
                                _install_n16_claim_boundary(
                                    review.resolve(),
                                    ledger.resolve(),
                                    database_scope=scope,
                                )
                            self.assertEqual(
                                [path.name for path in data_parent.iterdir()],
                                ["external-sentinel.txt"],
                            )
                            sentinel = data_parent / "external-sentinel.txt"
                            self.assertEqual(
                                (
                                    sentinel.read_bytes(),
                                    sentinel.lstat().st_ino,
                                    sentinel.lstat().st_nlink,
                                ),
                                external_sentinel_fingerprint[0],
                            )
                            self.assertFalse((data_parent / review.name).exists())
                            moved_review = held_parent / review.name
                            self.assertEqual(
                                zero_write_database_fingerprint(moved_review),
                                original_review_fingerprint,
                            )
                            for suffix in ("-wal", "-shm", "-journal"):
                                self.assertFalse(
                                    Path(str(data_parent / review.name) + suffix).exists()
                                )
                    self.assertEqual(replacement_entries, [])
                    if restore_before_open:
                        self.assertEqual(
                            _directory_identity(
                                data_parent, "restored Review parent"
                            ),
                            original_parent_identity,
                        )
                        self.assertTrue(review.exists())
                finally:
                    if held_parent.exists() and not data_parent.exists():
                        held_parent.rename(data_parent)
                    elif held_parent.exists():
                        if data_parent.exists():
                            for entry in data_parent.iterdir():
                                entry.unlink()
                            data_parent.rmdir()
                        held_parent.rename(data_parent)

    def test_explicit_install_never_creates_a_missing_review_database(self):
        with tempfile.TemporaryDirectory() as root:
            parent = Path(root) / "binance-data"
            state_parent = Path(root) / "binance-state"
            parent.mkdir()
            state_parent.mkdir()
            review = parent / "review.sqlite3"
            ledger = state_parent / "n16_claim_ledger.sqlite3"
            before_entries = tuple(parent.iterdir())
            with self.assertRaisesRegex(
                SignalRetentionMaintenanceError,
                "existing attested Review",
            ):
                _install_n16_claim_boundary(review, ledger)
            self.assertEqual(tuple(parent.iterdir()), before_entries)
            self.assertFalse(review.exists())
            self.assertFalse(ledger.exists())
            for path in (review, ledger):
                for suffix in ("", "-wal", "-shm", "-journal"):
                    self.assertFalse(Path(str(path) + suffix).exists())


class N16ExecutionTests(unittest.TestCase):
    @staticmethod
    def _n16_live_trader(root, client, state_store, *, clock_ms):
        trader = Trader(
            client,
            live_test_config(str(Path(root) / "dry-account.json")),
            state_store,
            logging.getLogger("n16-pre-submit"),
            clock_ms=clock_ms,
        )
        plan = trader.build_trend_support_continuation_margin_capped_trade_plan(
            "N16USDT",
            Decimal("100"),
            Decimal("98"),
            Decimal("5"),
            "a" * 24,
            entry_min_price=Decimal("99"),
            entry_max_price=Decimal("101"),
        )
        plan = replace(
            plan,
            entry_candle_open_time_ms=1_720_000_000_000,
            entry_deadline_ms=1_720_000_120_000,
            structure_context={
                "strategy_id": "N16",
                "signal_id": 41,
                "structure_id": "a" * 24,
            },
        )
        client.open_response = {
            "orderId": 1601,
            "avgPrice": "100",
            "executedQty": str(plan.quantity),
        }
        client.position_quantities = [plan.quantity]
        return trader, plan

    def test_n16_pre_submit_writes_and_deadline_precede_leverage(self):
        class InitialSaveFailure(StateStore):
            def save(self, _state):
                raise OSError("reservation write failed")

        class TransitionFailure(StateStore):
            def compare_and_save(self, _expected, _replacement):
                raise OSError("intent transition failed")

        for mode in ("initial_save", "transition", "deadline_after_reservation"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                client = FakeLiveExecutionClient({}, leverage=10)
                if mode == "initial_save":
                    state_store = InitialSaveFailure(Path(root) / "position.json")
                    clock_values = iter((1_720_000_119_998,))
                elif mode == "transition":
                    state_store = TransitionFailure(Path(root) / "position.json")
                    clock_values = iter(
                        (1_720_000_119_998, 1_720_000_119_999)
                    )
                else:
                    state_store = StateStore(Path(root) / "position.json")
                    clock_values = iter(
                        (1_720_000_119_999, 1_720_000_120_000)
                    )
                trader, plan = self._n16_live_trader(
                    root,
                    client,
                    state_store,
                    clock_ms=lambda: next(clock_values),
                )

                expected_error = (
                    EntryWindowExpiredError
                    if mode == "deadline_after_reservation"
                    else BinanceAPIError
                )
                with self.assertRaises(expected_error):
                    trader.open_long_plan_with_protection(plan)

                self.assertEqual(client.leverage_calls, [])
                self.assertEqual(client.market_calls, [])
                self.assertEqual(client.protection_calls, [])
                self.assertEqual(client.close_calls, [])
                if mode in {"initial_save", "deadline_after_reservation"}:
                    self.assertIsNone(state_store.load())
                else:
                    self.assertEqual(
                        state_store.load().orders["execution_pending"]["phase"],
                        "PRE_SUBMIT_RESERVED",
                    )

    def test_n16_leverage_failure_keeps_recoverable_market_intent(self):
        class LeverageFailureClient(FakeLiveExecutionClient):
            def set_leverage(self, symbol, leverage):
                self.leverage_calls.append((symbol, leverage))
                raise OSError("leverage acknowledgement failed")

            def get_order(self, *_args, **_kwargs):
                raise BinanceAPIError("-2013 Order does not exist")

        with tempfile.TemporaryDirectory() as root:
            client = LeverageFailureClient({}, leverage=10)
            state_store = StateStore(Path(root) / "position.json")
            trader, plan = self._n16_live_trader(
                root,
                client,
                state_store,
                clock_ms=lambda: 1_720_000_119_999,
            )

            with self.assertRaisesRegex(
                BinanceAPIError,
                "LEVERAGE_SETUP_FAILED_WITH_PENDING_JOURNAL",
            ):
                trader.open_long_plan_with_protection(plan)

            pending = state_store.load()
            self.assertEqual(
                pending.orders["execution_pending"]["phase"],
                "MARKET_ORDER_SUBMITTING",
            )
            self.assertEqual(client.leverage_calls, [("N16USDT", 10)])
            self.assertEqual(client.market_calls, [])
            self.assertEqual(client.protection_calls, [])
            client.position_quantities = []
            client.open_response = {}
            first = trader.sync_state_with_exchange()
            self.assertTrue(first.execution_pending)
            self.assertEqual(
                first.pending_detail["reason"],
                "MARKET_ORDER_ABSENCE_CONFIRMATION_PENDING",
            )
            second = trader.sync_state_with_exchange()
            self.assertTrue(second.pending_resolved)
            self.assertFalse(second.has_position)
            self.assertEqual(
                second.pending_detail["resolution"],
                "MARKET_ORDER_ABSENCE_CONFIRMED_TWICE",
            )
            self.assertEqual(client.market_calls, [])
            self.assertEqual(client.protection_calls, [])

    def test_n16_normal_live_open_submits_each_order_once(self):
        with tempfile.TemporaryDirectory() as root:
            client = FakeLiveExecutionClient({}, leverage=10)
            state_store = StateStore(Path(root) / "position.json")
            trader, plan = self._n16_live_trader(
                root,
                client,
                state_store,
                clock_ms=lambda: 1_720_000_119_999,
            )

            state = trader.open_long_plan_with_protection(plan)

            self.assertEqual(client.leverage_calls, [("N16USDT", 10)])
            self.assertEqual(len(client.market_calls), 1)
            self.assertEqual(
                tuple(call[0] for call in client.protection_calls),
                ("STOP_MARKET", "TAKE_PROFIT_MARKET"),
            )
            self.assertNotIn("execution_pending", state.orders)
            self.assertEqual(state.orders["strategy"]["signal_id"], 41)

    def test_dry_account_n16_settlement_receipt_and_high_water_are_exact(self):
        with tempfile.TemporaryDirectory() as root:
            store = DryRunAccountStore(
                str(Path(root) / "dry-account.json"),
                Decimal("1000"),
            )
            store.set_balance(Decimal("1000"))
            self.assertEqual(
                store.settle_balance_once(
                    11,
                    Decimal("1000"),
                    Decimal("950"),
                ),
                Decimal("950"),
            )
            self.assertEqual(
                store.settle_balance_once(
                    11,
                    Decimal("1000"),
                    Decimal("950"),
                ),
                Decimal("950"),
            )
            with self.assertRaises(OSError):
                store.settle_balance_once(
                    11,
                    Decimal("1000"),
                    Decimal("951"),
                )
            with self.assertRaises(OSError):
                store.settle_balance_once(
                    10,
                    Decimal("950"),
                    Decimal("940"),
                )
            self.assertEqual(
                store.settle_balance_once(
                    12,
                    Decimal("950"),
                    Decimal("960"),
                ),
                Decimal("960"),
            )

            # N01-N15 retain the legacy delta mutation.  It must advance from
            # the current balance without erasing or rewinding N16's receipt.
            self.assertEqual(store.apply_pnl(Decimal("5")), Decimal("965"))
            after_legacy = store.load()
            self.assertEqual(after_legacy.balance, "965")
            self.assertEqual(after_legacy.last_settlement_trade_id, 12)
            self.assertEqual(after_legacy.last_settlement_balance, "960")
            with self.assertRaises(OSError):
                store.settle_balance_once(
                    12,
                    Decimal("950"),
                    Decimal("960"),
                )
            self.assertEqual(
                store.settle_balance_once(
                    13,
                    Decimal("965"),
                    Decimal("970"),
                ),
                Decimal("970"),
            )

    def test_n16_final_settlement_retries_are_idempotent_across_state_cas(self):
        def position(*, dry_run=True, symbol="N16USDT"):
            return PositionState(
                symbol=symbol,
                quantity="1",
                entry_price="100",
                stop_loss_price="99",
                take_profit_price="105",
                leverage=10,
                opened_at="2026-07-16T00:00:00+00:00",
                dry_run=dry_run,
                orders={},
            )

        def final_evidence(trade_id=21):
            return SimpleNamespace(
                status="FINAL",
                finalization=SimpleNamespace(
                    trade_id=trade_id,
                    pnl_amount="-50",
                    balance_after="950",
                ),
            )

        for mode in ("account_ack_loss", "cas_exception", "cas_replaced"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                state_store = StateStore(Path(root) / "position.json")
                expected = position()
                state_store.save(expected)
                account = DryRunAccountStore(
                    str(Path(root) / "dry-account.json"),
                    Decimal("1000"),
                )
                bot = TradingBot.__new__(TradingBot)
                bot.logger = logging.getLogger("n16-dry-settlement-%s" % mode)
                bot.state = state_store
                bot.trader = SimpleNamespace(
                    settle_dry_run_balance_once=account.settle_balance_once
                )
                evidence = final_evidence()

                if mode == "account_ack_loss":
                    real_write = account._write_state
                    write_count = []

                    def commit_then_lose_ack(state):
                        real_write(state)
                        write_count.append(1)
                        raise OSError("account replace acknowledgement lost")

                    with patch.object(
                        account,
                        "_write_state",
                        side_effect=commit_then_lose_ack,
                    ), self.assertRaises(OSError):
                        bot._complete_n16_final_evidence(expected, evidence)
                    self.assertEqual(write_count, [1])
                    self.assertEqual(state_store.load(), expected)
                    self.assertEqual(account.preview_balance(), Decimal("950"))
                    self.assertFalse(
                        bot._complete_n16_final_evidence(expected, evidence)
                    )
                    self.assertIsNone(state_store.load())
                elif mode == "cas_exception":
                    with patch.object(
                        state_store,
                        "compare_and_clear",
                        side_effect=OSError("state clear acknowledgement lost"),
                    ):
                        self.assertTrue(
                            bot._complete_n16_final_evidence(expected, evidence)
                        )
                    self.assertEqual(state_store.load(), expected)
                    self.assertEqual(account.preview_balance(), Decimal("950"))
                    self.assertFalse(
                        bot._complete_n16_final_evidence(expected, evidence)
                    )
                    self.assertIsNone(state_store.load())
                else:
                    replacement = position(symbol="REPLACEDUSDT")

                    def replace_and_reject(_expected):
                        state_store.save(replacement)
                        return False

                    with patch.object(
                        state_store,
                        "compare_and_clear",
                        side_effect=replace_and_reject,
                    ):
                        self.assertTrue(
                            bot._complete_n16_final_evidence(expected, evidence)
                        )
                    self.assertEqual(state_store.load(), replacement)
                    self.assertEqual(account.preview_balance(), Decimal("950"))
                    self.assertTrue(
                        bot._complete_n16_final_evidence(expected, evidence)
                    )
                    self.assertEqual(state_store.load(), replacement)
                account_state = account.load()
                self.assertEqual(account_state.last_settlement_trade_id, 21)
                self.assertEqual(account_state.last_settlement_balance, "950")

        # Real FINAL recovery clears only its local state and must never touch
        # the dry-run account mutation path.
        with tempfile.TemporaryDirectory() as root:
            state_store = StateStore(Path(root) / "position.json")
            expected = position(dry_run=False)
            state_store.save(expected)

            def forbidden_settlement(*_args, **_kwargs):
                raise AssertionError("real N16 finalization touched dry account")

            bot = TradingBot.__new__(TradingBot)
            bot.logger = logging.getLogger("n16-real-final-no-dry-account")
            bot.state = state_store
            bot.trader = SimpleNamespace(
                settle_dry_run_balance_once=forbidden_settlement
            )
            self.assertFalse(
                bot._complete_n16_final_evidence(expected, final_evidence(22))
            )
            self.assertIsNone(state_store.load())

    def test_real_dry_finalizer_commit_ack_loss_recovers_exactly_once(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = N16RecorderSchedulerTests()
            recorder, database, signal_ids = fixture.publish_n16_claims(
                root,
                ("N16USDT",),
            )
            with closing(sqlite3.connect(database)) as connection:
                structure_id = connection.execute(
                    "SELECT structure_id FROM n16_consumption_seals "
                    "WHERE seal_ordinal=1"
                ).fetchone()[0]
            state = fixture.strict_n16_live_state(
                signal_ids[0],
                structure_id,
                dry_run=True,
            )
            claim = recorder.claim_strategy_live_open_audit(state, "N16")
            self.assertIsNotNone(claim)
            state_store = StateStore(Path(root) / "position.json")
            state_store.save(state)
            account = DryRunAccountStore(
                str(Path(root) / "dry-account.json"),
                Decimal("1000"),
            )
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(symbol_cooldown_hours=1)
            bot.logger = logging.getLogger("n16-real-dry-finalizer-ack")
            bot.state = state_store
            bot.recorder = recorder
            bot.trader = SimpleNamespace(
                settle_dry_run_balance_once=account.settle_balance_once
            )
            close_result = SimpleNamespace(
                state=state,
                exit_reason="STOP_LOSS",
                exit_price=Decimal("95"),
                mark_price=Decimal("95"),
                pnl_amount=Decimal("-50"),
                pnl_pct=Decimal("-0.05"),
                balance_before=Decimal("1000"),
                balance_after=Decimal("950"),
            )
            real_finalize = recorder.finalize_strategy_dry_run_result
            committed = []

            def commit_then_lose_ack(*args, **kwargs):
                result = real_finalize(*args, **kwargs)
                self.assertIsNotNone(result)
                committed.append(result.trade_id)
                raise RuntimeError("Review finalizer acknowledgement lost")

            with patch.object(
                recorder,
                "finalize_strategy_dry_run_result",
                side_effect=commit_then_lose_ack,
            ), self.assertRaisesRegex(RuntimeError, "acknowledgement lost"):
                bot._finalize_n16_dry_close(close_result)
            self.assertEqual(len(committed), 1)
            self.assertEqual(account.preview_balance(), Decimal("1000"))
            self.assertEqual(state_store.load(), state)

            evidence = recorder.inspect_strategy_live_finalization(
                state,
                "N16",
                allow_dry_run_pending=True,
            )
            self.assertEqual(evidence.status, "FINAL")
            self.assertIsNotNone(evidence.finalization)
            self.assertEqual(evidence.finalization.trade_id, committed[0])
            with closing(sqlite3.connect(database)) as connection:
                before = (
                    connection.execute(
                        "SELECT COUNT(*) FROM trade_reviews WHERE id=?",
                        (committed[0],),
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links "
                        "WHERE trade_review_id=?",
                        (committed[0],),
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM events WHERE trade_review_id=?",
                        (committed[0],),
                    ).fetchone()[0],
                )
            self.assertEqual(before, (1, 1, 1))

            self.assertTrue(
                bot._clear_finalized_n16_state_before_reconciliation(state)
            )
            self.assertIsNone(state_store.load())
            self.assertEqual(account.preview_balance(), Decimal("950"))
            receipt = account.load()
            self.assertEqual(receipt.last_settlement_trade_id, committed[0])
            self.assertEqual(receipt.last_settlement_balance, "950")
            with closing(sqlite3.connect(database)) as connection:
                after = (
                    connection.execute(
                        "SELECT COUNT(*) FROM trade_reviews WHERE id=?",
                        (committed[0],),
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_live_links "
                        "WHERE trade_review_id=?",
                        (committed[0],),
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM events WHERE trade_review_id=?",
                        (committed[0],),
                    ).fetchone()[0],
                )
            self.assertEqual(after, before)

    def test_single_and_multi_reconciliation_share_atomic_n16_dry_close(self):
        state = PositionState(
            symbol="N16USDT",
            quantity="1",
            entry_price="100",
            stop_loss_price="99",
            take_profit_price="105",
            leverage=10,
            opened_at="2026-07-16T00:00:00+00:00",
            dry_run=True,
            orders={},
        )
        close_result = SimpleNamespace(
            state=state,
            exit_reason="STOP_LOSS",
            exit_price=Decimal("99"),
            mark_price=Decimal("99"),
            pnl_amount=Decimal("-1"),
            pnl_pct=Decimal("-0.01"),
            balance_before=Decimal("1000"),
            balance_after=Decimal("999"),
        )

        def forbidden(*_args, **_kwargs):
            raise AssertionError("N16 dry close reached legacy three-step writer")

        for route in ("single", "multi"):
            with self.subTest(route=route):
                bot = TradingBot.__new__(TradingBot)
                bot.logger = logging.getLogger("n16-atomic-route-%s" % route)
                bot.state = SimpleNamespace(load=lambda: None)
                bot.recorder = SimpleNamespace(record_trade_close=forbidden)
                bot.paper_trader = SimpleNamespace(
                    close_triggered_open_trades=lambda *_args, **_kwargs: []
                )
                with patch.object(
                    bot,
                    "_attested_local_execution_state",
                    side_effect=((True, state), (True, None)),
                ), patch.object(
                    bot,
                    "_clear_finalized_n16_state_before_reconciliation",
                    return_value=False,
                ), patch.object(
                    bot,
                    "_repair_n16_live_audit_before_reconciliation",
                    return_value=(True, state),
                ), patch.object(
                    bot,
                    "_close_dry_run_with_attested_state",
                    return_value=close_result,
                ), patch.object(
                    bot,
                    "_n16_local_state_claim_identity",
                    return_value=(1, "N16USDT", "a" * 24, False),
                ), patch.object(
                    bot,
                    "_finalize_n16_dry_close",
                    return_value=True,
                ) as atomic_finalize, patch.object(
                    bot,
                    "_set_symbol_cooldown",
                    side_effect=forbidden,
                ), patch.object(
                    bot,
                    "_record_strategy_live_result_from_state",
                    side_effect=forbidden,
                ), patch.object(
                    bot,
                    "_sync_with_attested_state",
                    side_effect=BinanceAPIError("stop after atomic close"),
                ):
                    if route == "single":
                        self.assertFalse(
                            bot._reconcile_single_strategy_after_publication()
                        )
                    else:
                        self.assertIsNone(
                            bot._reconcile_multi_strategy_after_publication()
                        )
                atomic_finalize.assert_called_once_with(close_result)

    def test_main_representative_plan_failure_does_not_try_runner(self):
        rows, checked = n16_klines()
        winner = candidate("AAAUSDT", 3)
        runner = candidate("BBBUSDT", 8)

        class Client:
            def get_klines(self, _symbol):
                return deepcopy(rows)

            def get_klines_for_interval(self, *_args, **_kwargs):
                return []

            def get_aggregate_trades(self, *_args, **_kwargs):
                return []

        class Monitor:
            def scan_for_strategies(self, _top_n):
                return StrategyMarketScan(2, [], [runner, winner])

        class FailingPlanTrader:
            def __init__(self):
                self.plan_calls = []
                self.open_calls = 0

            def close_dry_run_position_if_triggered(self):
                return None

            def sync_state_with_exchange(self):
                return SyncResult(has_position=False)

            def build_trend_support_continuation_margin_capped_trade_plan(
                self, symbol, *_args, **_kwargs
            ):
                self.plan_calls.append(symbol)
                raise BinanceAPIError("forced representative plan failure")

            def open_long_plan_with_protection(self, _plan):
                self.open_calls += 1
                raise AssertionError("no live order may be attempted")

        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "review.sqlite3"
            claim_ledger = Path(root) / "n16_claim_ledger.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.commit()
            _install_n16_claim_boundary(database, claim_ledger)
            _install_n17_lifecycle_boundary(database, claim_ledger)
            _install_n19_lifecycle_boundary(database, claim_ledger)
            _install_n18_lifecycle_boundary(database, claim_ledger)
            _install_n20_lifecycle_boundary(database, claim_ledger)
            _install_micro_lifecycle_boundary(database, claim_ledger)
            _install_coverage_epoch_boundary(database, claim_ledger)
            recorder = ReviewRecorder(
                database,
                logging.getLogger("n16-main"),
                n16_claim_ledger_file=claim_ledger,
            )
            recorder.upsert_strategy_definitions((N16_STRATEGY,))
            bot = TradingBot.__new__(TradingBot)
            bot.config = SimpleNamespace(dry_run=True)
            bot.logger = logging.getLogger("n16-main")
            bot.client = Client()
            bot.state = StateStore(Path(root) / "state.json")
            bot.recorder = recorder
            bot.monitor = Monitor()
            bot.trader = FailingPlanTrader()
            bot.paper_trader = PaperTrader(recorder, bot.logger)
            bot.strategies = (N16_STRATEGY,)
            bot.strategy_scheduler = StrategyScheduler(
                bot.strategies, 5, recorder, bot.logger
            )
            with patch("trading_bot.main.time.time", return_value=checked / 1000):
                bot._run_once_multi_strategy()
            self.assertEqual(bot.trader.plan_calls, ["AAAUSDT"])
            self.assertEqual(bot.trader.open_calls, 0)
            with closing(sqlite3.connect(recorder.db_file)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM strategy_paper_trades"
                    ).fetchone(),
                    (0,),
                )
                decisions = connection.execute(
                    "SELECT symbol, decision, reason FROM strategy_signals "
                    "WHERE strategy_id = 'N16' ORDER BY symbol"
                ).fetchall()
            self.assertEqual(
                decisions,
                [
                    (
                        "AAAUSDT",
                        "PLAN_REJECTED",
                        "TRADE_PLAN_INVALID",
                    ),
                    ("BBBUSDT", "REJECTED", "N16_NOT_REPRESENTATIVE"),
                ],
            )

    def test_n16_plan_identity_or_overlay_failure_blocks_entire_round(self):
        rows, checked = n16_klines()

        class Client:
            def __init__(self):
                self.interval_calls = 0
                self.aggregate_calls = 0

            def get_klines(self, _symbol):
                return deepcopy(rows)

            def get_klines_for_interval(self, *_args, **_kwargs):
                self.interval_calls += 1
                return []

            def get_aggregate_trades(self, *_args, **_kwargs):
                self.aggregate_calls += 1
                return []

        class Monitor:
            def scan_for_strategies(self, _top_n):
                return StrategyMarketScan(1, [], [candidate()])

        class CountingPaper:
            def __init__(self):
                self.close_calls = 0
                self.open_calls = 0

            def close_triggered_open_trades(self, *_args, **_kwargs):
                self.close_calls += 1
                return []

            def open_trade(self, *_args, **_kwargs):
                self.open_calls += 1
                raise AssertionError("integrity failure reached paper open")

        class PlanTrader:
            def __init__(self, mode):
                self.mode = mode
                self.build_calls = 0
                self.close_calls = 0
                self.sync_calls = 0
                self.open_calls = 0

            def close_dry_run_position_if_triggered(self):
                self.close_calls += 1
                return None

            def sync_state_with_exchange(self):
                self.sync_calls += 1
                return SyncResult(has_position=False)

            def build_trend_support_continuation_margin_capped_trade_plan(
                self, symbol, entry, _p, _rr, **kwargs
            ):
                self.build_calls += 1
                structure_id = kwargs["structure_id"]
                context = {
                    "strategy_id": "N16",
                    "structure_id": structure_id,
                }
                stop_mode = "trend_support_continuation_margin_capped"
                if self.mode == "wrong_context":
                    context["strategy_id"] = "N15"
                if self.mode == "wrong_context_structure":
                    context["structure_id"] = "f" * 24
                if self.mode == "context_signal_bool":
                    context["signal_id"] = True
                if self.mode == "context_signal_float":
                    context["signal_id"] = 1.0
                if self.mode == "wrong_stop_mode":
                    stop_mode = "breadth_recovery_margin_capped"
                return TradePlan(
                    symbol="OTHERUSDT" if self.mode == "wrong_symbol" else symbol,
                    leverage=10,
                    quantity=Decimal("1"),
                    entry_price=entry,
                    stop_loss_price=entry - Decimal("1"),
                    take_profit_price=entry + Decimal("5"),
                    stop_loss_pct=Decimal("0.01"),
                    take_profit_pct=Decimal("0.05"),
                    amplitude_24h_pct=Decimal("0"),
                    high_24h_price=entry,
                    low_24h_price=entry,
                    risk_amount=Decimal("1"),
                    notional_value=entry,
                    required_margin=Decimal("10"),
                    balance=Decimal("1000"),
                    stop_mode=stop_mode,
                    structure_id=structure_id,
                    entry_min_price=kwargs["entry_min_price"],
                    entry_max_price=kwargs["entry_max_price"],
                    structure_context=context,
                )

            def open_long_plan_with_protection(self, _plan):
                self.open_calls += 1
                raise AssertionError("integrity failure reached live open")

        for mode in (
            "wrong_symbol",
            "wrong_context",
            "wrong_context_structure",
            "context_signal_bool",
            "context_signal_float",
            "wrong_stop_mode",
            "reverse_marker",
            "reverse_plan_stop_only",
            "reverse_context_only",
            "reverse_strategy_stop",
            "n16_id_wrong_strategy_stop",
            "overlay_failure",
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                recorder = make_test_recorder(
                    Path(root) / "review.sqlite3",
                    logging.getLogger("n16-plan-integrity-%s" % mode),
                )
                recorder.upsert_strategy_definitions((N16_STRATEGY,))
                client = Client()
                trader = PlanTrader(
                    "valid" if mode == "overlay_failure" else mode
                )
                paper = CountingPaper()
                bot = TradingBot.__new__(TradingBot)
                bot.config = SimpleNamespace(dry_run=True)
                bot.logger = logging.getLogger("n16-plan-integrity-%s" % mode)
                bot.client = client
                bot.state = StateStore(Path(root) / "position.json")
                bot.recorder = recorder
                bot.monitor = Monitor()
                bot.trader = trader
                bot.paper_trader = paper
                bot.strategies = (N16_STRATEGY,)
                scheduler = StrategyScheduler(
                    bot.strategies, 5, recorder, bot.logger
                )
                reverse_signal_modes = {
                    "reverse_marker",
                    "reverse_plan_stop_only",
                    "reverse_context_only",
                    "reverse_strategy_stop",
                    "n16_id_wrong_strategy_stop",
                }
                if mode in reverse_signal_modes:
                    n15_strategy = next(
                        item
                        for item in load_all_strategies()
                        if item.strategy_id == "N15"
                    )

                    class ReverseMarkerScheduler:
                        def evaluate(self, *args, **kwargs):
                            scheduled = scheduler.evaluate(*args, **kwargs)
                            if mode == "n16_id_wrong_strategy_stop":
                                replacement_strategy = replace(
                                    N16_STRATEGY,
                                    stop_mode=n15_strategy.stop_mode,
                                )
                            elif mode == "reverse_strategy_stop":
                                replacement_strategy = replace(
                                    n15_strategy,
                                    stop_mode=(
                                        "trend_support_continuation_margin_capped"
                                    ),
                                )
                            else:
                                replacement_strategy = n15_strategy
                            changed = [
                                replace(item, strategy=replacement_strategy)
                                for item in scheduled.signals
                            ]
                            changed_passed = [
                                item for item in changed if item.passed
                            ]
                            return replace(
                                scheduled,
                                signals=changed,
                                passed_signals=changed_passed,
                                live_candidates=[],
                            )

                    bot.strategy_scheduler = ReverseMarkerScheduler()
                else:
                    bot.strategy_scheduler = scheduler
                update_patch = (
                    patch.object(
                        recorder,
                        "update_strategy_signal",
                        return_value=False,
                    )
                    if mode == "overlay_failure"
                    else nullcontext()
                )
                original_build = bot._build_strategy_plan
                def build_with_context_tamper(signal):
                    if mode in {
                        "reverse_marker",
                        "reverse_plan_stop_only",
                        "reverse_context_only",
                    }:
                        signal = replace(signal, strategy=N16_STRATEGY)
                    built = original_build(signal)
                    context = dict(built.structure_context)
                    if mode == "wrong_context":
                        context["strategy_id"] = "N15"
                    elif mode == "wrong_context_structure":
                        context["structure_id"] = "f" * 24
                    elif mode == "context_signal_bool":
                        context["signal_id"] = True
                    elif mode == "context_signal_float":
                        context["signal_id"] = 1.0
                    elif mode == "reverse_plan_stop_only":
                        context["strategy_id"] = "N15"
                    elif mode == "reverse_context_only":
                        built = replace(
                            built,
                            stop_mode=n15_strategy.stop_mode,
                        )
                    return replace(built, structure_context=context)

                build_patch = (
                    patch.object(
                        bot,
                        "_build_strategy_plan",
                        side_effect=build_with_context_tamper,
                    )
                    if mode
                    in {
                        "wrong_context",
                        "wrong_context_structure",
                        "context_signal_bool",
                        "context_signal_float",
                        "reverse_marker",
                        "reverse_plan_stop_only",
                        "reverse_context_only",
                    }
                    else nullcontext()
                )
                current_claim_patch = (
                    patch.object(
                        bot,
                        "_n16_current_claims_ready",
                        return_value=True,
                    )
                    if mode
                    in {
                        "reverse_marker",
                        "reverse_plan_stop_only",
                        "reverse_context_only",
                    }
                    else nullcontext()
                )
                with update_patch, build_patch, current_claim_patch, patch(
                    "trading_bot.main.time.time", return_value=checked / 1000
                ):
                    bot._run_once_multi_strategy()
                self.assertEqual(
                    (
                        trader.close_calls,
                        trader.sync_calls,
                        trader.open_calls,
                        paper.close_calls,
                        paper.open_calls,
                        client.interval_calls,
                        client.aggregate_calls,
                    ),
                    (0, 0, 0, 0, 0, 0, 0),
                )
                if mode in {
                    "reverse_strategy_stop",
                    "n16_id_wrong_strategy_stop",
                }:
                    self.assertEqual(trader.build_calls, 0)

    def test_n16_stop_mode_and_context_conflicts_fail_before_leverage(self):
        class CountingClient:
            def __init__(self):
                self.leverage_calls = 0

            def set_leverage(self, *_args, **_kwargs):
                self.leverage_calls += 1
                raise AssertionError("N16 identity conflict reached leverage")

        with tempfile.TemporaryDirectory() as root:
            client = CountingClient()
            trader = Trader(
                client,
                test_config(str(Path(root) / "account.json")),
                StateStore(Path(root) / "state.json"),
                logging.getLogger("n16-trader-identity"),
            )
            base = TradePlan(
                symbol="N16USDT",
                leverage=10,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_loss_price=Decimal("99"),
                take_profit_price=Decimal("105"),
                stop_loss_pct=Decimal("0.01"),
                take_profit_pct=Decimal("0.05"),
                amplitude_24h_pct=Decimal("0"),
                high_24h_price=Decimal("101"),
                low_24h_price=Decimal("99"),
                risk_amount=Decimal("1"),
                notional_value=Decimal("100"),
                required_margin=Decimal("10"),
                balance=Decimal("1000"),
                stop_mode="trend_support_continuation_margin_capped",
                structure_id="a" * 24,
                structure_context={
                    "strategy_id": "N16",
                    "structure_id": "a" * 24,
                    "signal_id": 1,
                },
            )
            for plan in (
                replace(base, stop_mode="breadth_recovery_margin_capped"),
                replace(
                    base,
                    structure_context={
                        "strategy_id": "N15",
                        "structure_id": "a" * 24,
                        "signal_id": 1,
                    },
                ),
                replace(
                    base,
                    structure_context={
                        "strategy_id": "N16",
                        "structure_id": "b" * 24,
                        "signal_id": 1,
                    },
                ),
                replace(
                    base,
                    structure_context={
                        "strategy_id": "N16",
                        "structure_id": "a" * 24,
                        "signal_id": True,
                    },
                ),
                replace(
                    base,
                    structure_context={
                        "strategy_id": "N16",
                        "structure_id": "a" * 24,
                        "signal_id": 1.0,
                    },
                ),
            ):
                with self.subTest(stop_mode=plan.stop_mode), self.assertRaises(
                    BinanceAPIError
                ):
                    trader.open_long_plan_with_protection(plan)
            self.assertEqual(client.leverage_calls, 0)

    def test_main_plan_uses_frozen_p_entry_range_deadline_and_evidence(self):
        rows, checked = n16_klines()
        analysis = analyze_n16_mature_trend_support(
            "N16USDT", rows, quote_volume_rank=7, checked_at_ms=checked
        )
        signal = type("Signal", (), {})()
        signal.strategy = N16_STRATEGY
        signal.candidate = candidate()
        signal.analysis = analysis
        base = TradePlan(
            symbol="N16USDT",
            leverage=20,
            quantity=Decimal("1"),
            entry_price=analysis.structure.entry.close,
            stop_loss_price=Decimal("105"),
            take_profit_price=Decimal("123"),
            stop_loss_pct=Decimal("0.02"),
            take_profit_pct=Decimal("0.10"),
            amplitude_24h_pct=Decimal("0"),
            high_24h_price=Decimal("0"),
            low_24h_price=Decimal("0"),
            risk_amount=Decimal("1"),
            notional_value=Decimal("100"),
            required_margin=Decimal("5"),
            balance=Decimal("1000"),
            stop_mode="trend_support_continuation_margin_capped",
        )

        class FakeTrader:
            def build_trend_support_continuation_margin_capped_trade_plan(
                self, *args, **kwargs
            ):
                self.args = args, kwargs
                return replace(
                    base,
                    entry_min_price=kwargs["entry_min_price"],
                    entry_max_price=kwargs["entry_max_price"],
                )

        bot = TradingBot.__new__(TradingBot)
        bot.trader = FakeTrader()
        plan = bot._build_strategy_plan(signal)
        args, kwargs = bot.trader.args
        self.assertEqual(args[1], analysis.structure.entry.close)
        self.assertEqual(args[2], analysis.structure.p)
        self.assertEqual(kwargs["entry_min_price"], analysis.structure.entry_min_price)
        self.assertEqual(kwargs["entry_max_price"], analysis.structure.entry_max_price)
        self.assertEqual(
            plan.entry_deadline_ms,
            analysis.structure.entry.open_time_ms + 120_000,
        )
        self.assertEqual(
            plan.structure_context["evidence_sha256"],
            analysis.state_record.evidence_sha256,
        )

    def test_tick_stop_minimum_extension_exact_five_r_and_actual_fill(self):
        with tempfile.TemporaryDirectory() as root:
            trader = Trader(
                RuleConstrainedClient(leverage=10),
                test_config(str(Path(root) / "account.json")),
                StateStore(str(Path(root) / "state.json")),
                logging.getLogger("n16_trader"),
            )
            plan = trader.build_trend_support_continuation_margin_capped_trade_plan(
                "N16USDT",
                Decimal("108.10"),
                Decimal("107.80"),
                Decimal("5"),
                "n16-structure",
                entry_min_price=Decimal("108.00"),
                entry_max_price=Decimal("108.60"),
            )
            self.assertLess(plan.stop_loss_price, Decimal("107.79"))
            self.assertGreaterEqual(
                (plan.take_profit_price - plan.entry_price)
                / (plan.entry_price - plan.stop_loss_price),
                Decimal("5"),
            )
            post_fill = trader._execution_plan_from_actual_entry(
                plan, Decimal("108.20")
            )
            self.assertGreaterEqual(post_fill.risk_reward_ratio, Decimal("5"))
            tolerated_fill = trader._execution_plan_from_actual_entry(
                plan, Decimal("108.61")
            )
            self.assertGreaterEqual(
                tolerated_fill.risk_reward_ratio, Decimal("5")
            )
            allowed_max = plan.entry_max_price * Decimal("1.005")
            boundary_fill = trader._execution_plan_from_actual_entry(
                plan, allowed_max
            )
            self.assertGreaterEqual(
                boundary_fill.risk_reward_ratio, Decimal("5")
            )
            with self.assertRaisesRegex(BinanceAPIError, "N16_ACTUAL_FILL"):
                trader._execution_plan_from_actual_entry(
                    plan, allowed_max + Decimal("0.000000000000001")
                )
            with self.assertRaisesRegex(BinanceAPIError, "Invalid N16 risk/reward"):
                trader.build_trend_support_continuation_margin_capped_trade_plan(
                    "N16USDT",
                    Decimal("108.10"),
                    Decimal("107.00"),
                    Decimal("4.999"),
                    entry_min_price=Decimal("108.00"),
                    entry_max_price=Decimal("108.60"),
                )
            with self.assertRaisesRegex(BinanceAPIError, "N16_STOP_PCT_OUT_OF_RANGE"):
                trader.build_trend_support_continuation_margin_capped_trade_plan(
                    "N16USDT",
                    Decimal("108.10"),
                    Decimal("100.00"),
                    Decimal("5"),
                    entry_min_price=Decimal("108.00"),
                    entry_max_price=Decimal("108.60"),
                )


if __name__ == "__main__":
    unittest.main()
