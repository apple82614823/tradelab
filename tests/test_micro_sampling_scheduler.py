from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import replace
from decimal import Decimal
import hashlib
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.test_micro_strategies import OPEN_TIME, observation
from tests.recorder_test_utils import make_test_recorder
from trading_bot.micro_observation import (
    MicroObservationError,
    MicroObservationLease,
    MicroObservationSample,
    MicroObservationSampler,
    build_universe_sha256,
    canonical_json,
)
from trading_bot.micro_analyzer import analyze_n22, analyze_n25
from trading_bot.monitor import FundingCandidate
from trading_bot.strategies import load_all_strategies
from trading_bot.strategy_scheduler import StrategyScheduler


class MicroObservationSamplerTests(unittest.TestCase):
    @staticmethod
    def _replace_and_hash(item, **changes):
        unsigned = replace(item, **changes, source_sha256="")
        return replace(
            unsigned,
            source_sha256=hashlib.sha256(
                canonical_json(unsigned.canonical_payload()).encode("utf-8")
            ).hexdigest(),
        )

    @staticmethod
    def _bind_universe(rows):
        universe_sha256 = build_universe_sha256(tuple(
            (item.symbol, item.quote_volume_rank)
            for item in rows.values()
        ))
        authenticated = {}
        for symbol, item in rows.items():
            unsigned = replace(
                item,
                universe_sha256=universe_sha256,
                source_sha256="",
            )
            authenticated[symbol] = replace(
                unsigned,
                source_sha256=hashlib.sha256(
                    canonical_json(unsigned.canonical_payload()).encode(
                        "utf-8"
                    )
                ).hexdigest(),
            )
        return authenticated

    @staticmethod
    def _sample(
        ordinal: int,
        observed_at_ms: int,
        *,
        symbols: tuple[str, ...] | None = None,
    ):
        members = symbols or tuple(
            "BTCUSDT" if rank == 1 else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        rows = {}
        for rank, symbol in enumerate(members, start=1):
            rows[symbol] = observation(
                symbol,
                ordinal,
                ordinal,
                observed_at_ms,
                quote=str(1000 + ordinal * 100),
                trades=100 + ordinal * 10,
                taker=str(500 + ordinal * 55),
                rank=rank,
            )
        return MicroObservationSamplerTests._bind_universe(rows)

    @staticmethod
    def _sample_for_identity(identity, observed_at_ms, *, symbols=None):
        rows = MicroObservationSamplerTests._sample(
            identity.sample_ordinal,
            observed_at_ms,
            symbols=symbols,
        )
        rebound = {}
        for symbol, item in rows.items():
            unsigned = replace(
                item,
                generation=identity.generation,
                boot_id=identity.boot_id,
                source_sha256="",
            )
            rebound[symbol] = replace(
                unsigned,
                source_sha256=hashlib.sha256(
                    canonical_json(unsigned.canonical_payload()).encode()
                ).hexdigest(),
            )
        return rebound

    @classmethod
    def _sample_bundle_for_sampler(
        cls,
        sampler,
        observed_at_ms,
        *,
        symbols,
    ):
        identity = sampler.next_identity()
        current = cls._sample_for_identity(
            identity,
            observed_at_ms,
            symbols=tuple(symbols),
        )
        continuation = {}
        if identity.continuation_ranked_symbols:
            continuation_symbols = tuple(
                symbol
                for symbol, _rank in identity.continuation_ranked_symbols
            )
            continuation = cls._sample_for_identity(
                identity,
                observed_at_ms,
                symbols=continuation_symbols,
            )
            cls.assert_continuation_identity(
                continuation,
                identity.continuation_ranked_symbols,
                identity.continuation_universe_sha256,
            )
        return MicroObservationSample(current, continuation)

    @staticmethod
    def assert_continuation_identity(rows, ranked_symbols, universe_sha256):
        if tuple(
            (symbol, rows[symbol].quote_volume_rank)
            for symbol, _rank in ranked_symbols
        ) != ranked_symbols:
            raise AssertionError("continuation rank fixture is inconsistent")
        if {item.universe_sha256 for item in rows.values()} != {
            universe_sha256
        }:
            raise AssertionError("continuation universe fixture is inconsistent")

    def test_fallback_identity_is_authenticated_before_rebinding(self):
        symbols = tuple(f"S{rank:03d}USDT" for rank in range(1, 101))
        base_sampler = MicroObservationSampler(boot_id="sampler-boot")
        valid = {
            symbol: self._replace_and_hash(item, scan_id=7)
            for symbol, item in self._sample_for_identity(
                base_sampler.next_identity(),
                OPEN_TIME + 1,
                symbols=symbols,
            ).items()
        }
        first = symbols[0]
        variants = {
            "boot": {
                **valid,
                first: self._replace_and_hash(valid[first], boot_id="wrong"),
            },
            "generation": {
                **valid,
                first: self._replace_and_hash(valid[first], generation=999),
            },
            "symbol": {
                **valid,
                first: self._replace_and_hash(valid[first], symbol="WRONGUSDT"),
            },
            "rank": {
                **valid,
                first: self._replace_and_hash(valid[first], quote_volume_rank=2),
            },
            "missing": {
                symbol: item for symbol, item in valid.items()
                if symbol != first
            },
        }
        for name, fallback in variants.items():
            with self.subTest(name=name):
                sampler = MicroObservationSampler(boot_id="sampler-boot")
                with self.assertRaisesRegex(
                    MicroObservationError,
                    "fallback identity",
                ):
                    sampler.freeze_for_scan(
                        scan_id=7,
                        current_symbols=symbols,
                        fallback_observations=fallback,
                        captured_at_ms=OPEN_TIME + 2,
                    )
                self.assertEqual(sampler.snapshot(), {})

        sampler = MicroObservationSampler(boot_id="sampler-boot")
        lease = sampler.freeze_for_scan(
            scan_id=7,
            current_symbols=symbols,
            fallback_observations=valid,
            captured_at_ms=OPEN_TIME + 2,
        )
        self.assertEqual(len(lease.windows), 100)
        self.assertTrue(sampler.confirm(lease, current_scan_id=7))

    def test_source_failure_and_publish_abort_require_fresh_generation(self):
        sampler = MicroObservationSampler(boot_id="boot")
        first = self._sample_for_identity(
            sampler.next_identity(),
            OPEN_TIME + 1,
        )
        self.assertTrue(sampler.commit_sample(first))
        lease = sampler.freeze_for_scan(
            scan_id=7,
            current_symbols=tuple(first),
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 2,
        )
        sampler.mark_failed("injected source failure")
        sampler.abort(lease)
        self.assertEqual(sampler.snapshot(), {})
        recovered_identity = sampler.next_identity()
        self.assertGreater(recovered_identity.generation, 1)
        recovered = self._sample_for_identity(
            recovered_identity,
            OPEN_TIME + 60_001,
        )
        self.assertTrue(sampler.commit_sample(recovered))
        self.assertEqual(len(sampler.snapshot()), 100)

    def test_atomic_fallback_factory_breaks_generation_rotation_failure_loop(self):
        sampler = MicroObservationSampler(boot_id="production-race")
        base = tuple(
            "BTCUSDT" if rank == 1 else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        self.assertTrue(sampler.commit_sample(self._sample_for_identity(
            sampler.next_identity(),
            OPEN_TIME + 1,
            symbols=base,
        )))

        # A slow full round prepares its current Top100 while the background
        # worker publishes a different, equally valid rotation.  The fallback
        # identity must be allocated under the sampler lock at freeze time;
        # otherwise every round collides with the newly committed generation
        # and abort(None) leaves the process in a permanent empty-frame loop.
        for round_index in range(1, 11):
            main_symbols = (*base[:-1], f"M{round_index:02d}USDT")
            worker_symbols = (*base[:-1], f"W{round_index:02d}USDT")
            worker_identity = sampler.next_identity()
            observed_at_ms = OPEN_TIME + round_index * 60_000 + 1
            self.assertTrue(sampler.commit_sample(self._sample_for_identity(
                worker_identity,
                observed_at_ms,
                symbols=worker_symbols,
            )))
            scan_id = 1_000 + round_index
            factory_calls = []

            def fallback_factory(identity):
                factory_calls.append(identity)
                return self._sample_for_identity(
                    identity,
                    observed_at_ms,
                    symbols=main_symbols,
                )

            lease = sampler.freeze_for_scan(
                scan_id=scan_id,
                current_symbols=main_symbols,
                fallback_observations={},
                fallback_factory=fallback_factory,
                captured_at_ms=observed_at_ms + 100,
            )
            self.assertEqual(len(factory_calls), 1)
            self.assertEqual(
                (
                    factory_calls[0].generation,
                    factory_calls[0].boot_id,
                ),
                (sampler.generation, sampler.boot_id),
            )
            self.assertEqual(set(lease.windows), set(main_symbols))
            self.assertEqual(
                lease.windows[main_symbols[-1]].reset_reason,
                "MICRO_OBSERVATION_COLD_START",
            )
            sampler.abort(lease)
            self.assertEqual(sampler.frame_count, 0)

        recovery_identity = sampler.next_identity()
        self.assertTrue(sampler.commit_sample(self._sample_for_identity(
            recovery_identity,
            OPEN_TIME + 660_001,
            symbols=base,
        )))
        self.assertEqual(sampler.frame_count, 1)

    def test_atomic_fallback_factory_refreshes_stale_frame_and_rejects_partial(self):
        sampler = MicroObservationSampler(boot_id="stale-refresh")
        symbols = tuple(
            "BTCUSDT" if rank == 1 else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        self.assertTrue(sampler.commit_sample(self._sample_for_identity(
            sampler.next_identity(),
            OPEN_TIME + 1,
            symbols=symbols,
        )))

        factory_calls = []

        def refresh(identity):
            factory_calls.append(identity)
            return self._sample_for_identity(
                identity,
                OPEN_TIME + 180_001,
                symbols=symbols,
            )

        lease = sampler.freeze_for_scan(
            scan_id=701,
            current_symbols=symbols,
            fallback_observations={},
            fallback_factory=refresh,
            captured_at_ms=OPEN_TIME + 180_100,
        )
        self.assertEqual(len(factory_calls), 1)
        self.assertEqual(sampler.generation, 2)
        self.assertEqual(
            {
                window.observations[-1].generation
                for window in lease.windows.values()
            },
            {2},
        )
        self.assertTrue(sampler.confirm(lease, current_scan_id=701))

        before_generation = sampler.generation
        before_snapshot = sampler.snapshot()

        def malformed(identity, *, partial=False, future=False):
            complete = self._sample_for_identity(
                identity,
                (
                    OPEN_TIME + 360_101
                    if future
                    else OPEN_TIME + 360_001
                ),
                symbols=symbols,
            )
            if not partial:
                return complete
            return {
                symbol: item for symbol, item in complete.items()
                if symbol != symbols[-1]
            }

        for name, factory in (
            ("partial", lambda identity: malformed(identity, partial=True)),
            ("future", lambda identity: malformed(identity, future=True)),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                MicroObservationError,
                "fallback identity",
            ):
                sampler.freeze_for_scan(
                    scan_id=702,
                    current_symbols=symbols,
                    fallback_observations={},
                    fallback_factory=factory,
                    captured_at_ms=OPEN_TIME + 360_100,
                )
            self.assertEqual(sampler.generation, before_generation)
            self.assertEqual(sampler.frame_count, 2)
            self.assertEqual(sampler.snapshot(), before_snapshot)

        recovered = sampler.freeze_for_scan(
            scan_id=703,
            current_symbols=symbols,
            fallback_observations={},
            fallback_factory=lambda identity: malformed(identity),
            captured_at_ms=OPEN_TIME + 360_100,
        )
        self.assertEqual(sampler.generation, before_generation + 1)
        self.assertTrue(sampler.confirm(recovered, current_scan_id=703))

    def test_shared_forged_universe_digest_is_rejected(self):
        sampler = MicroObservationSampler(boot_id="boot")
        rows = self._sample(1, OPEN_TIME + 1)
        forged = {}
        for symbol, item in rows.items():
            unsigned = replace(
                item,
                universe_sha256="f" * 64,
                source_sha256="",
            )
            forged[symbol] = replace(
                unsigned,
                source_sha256=hashlib.sha256(
                    canonical_json(unsigned.canonical_payload()).encode()
                ).hexdigest(),
            )
        self.assertFalse(sampler.commit_sample(forged))
        self.assertEqual(sampler.snapshot(), {})

    def test_lease_capture_time_cannot_precede_latest_observation(self):
        sampler = MicroObservationSampler(boot_id="boot")
        rows = self._sample(1, OPEN_TIME + 60_001)
        self.assertTrue(sampler.commit_sample(rows))
        with self.assertRaisesRegex(
            MicroObservationError,
            "capture time",
        ):
            sampler.freeze_for_scan(
                scan_id=7,
                current_symbols=tuple(rows),
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 60_000,
            )

    def test_stale_or_forged_abort_cannot_clear_current_lease(self):
        sampler = MicroObservationSampler(boot_id="boot")
        rows = self._sample(1, OPEN_TIME + 1)
        self.assertTrue(sampler.commit_sample(rows))
        lease = sampler.freeze_for_scan(
            scan_id=7,
            current_symbols=tuple(rows),
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 2,
        )
        forged = MicroObservationLease(
            lease.expected_generation,
            8,
            lease.boot_id,
            lease.captured_at_ms,
            lease.windows,
            lease.source_sha256,
        )
        with self.assertRaisesRegex(MicroObservationError, "lease identity"):
            sampler.abort(forged)
        with self.assertRaisesRegex(MicroObservationError, "lease identity"):
            sampler.abort(None)
        self.assertTrue(sampler.confirm(lease, current_scan_id=7))

    def test_slow_full_round_warms_three_and_four_point_windows(self):
        sampler = MicroObservationSampler(boot_id="boot")
        symbols = tuple(self._sample(1, OPEN_TIME + 1))

        # A 170-second full round plus a 60-second poll delay contains four
        # independently authenticated 60-second samples.  They must not be
        # reduced to one sample merely because full CURRENT scans are slow.
        for ordinal, offset in enumerate((1, 60_001, 120_001, 180_001), 1):
            self.assertTrue(
                sampler.commit_sample(
                    self._sample(ordinal, OPEN_TIME + offset)
                )
            )

        lease = sampler.freeze_for_scan(
            scan_id=500,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 180_100,
        )
        self.assertEqual(len(lease.windows), 100)
        self.assertEqual(
            {len(window.observations) for window in lease.windows.values()},
            {4},
        )
        for window in lease.windows.values():
            self.assertEqual(
                [item.scan_id for item in window.observations],
                [500, 500, 500, 500],
            )
            self.assertEqual(
                [item.generation for item in window.observations],
                [1, 2, 3, 4],
            )
            self.assertEqual(
                [increment.interval_ms for increment in window.increments],
                [60_000, 60_000, 60_000],
            )
        self.assertTrue(sampler.confirm(lease, current_scan_id=500))

    def test_lease_survives_170_seconds_while_four_frames_keep_advancing(self):
        sampler = MicroObservationSampler(boot_id="boot")
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        self.assertTrue(sampler.commit_sample(
            self._sample(1, OPEN_TIME + 1)
        ))
        lease = sampler.freeze_for_scan(
            scan_id=700,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 2,
        )
        for ordinal, offset in enumerate((60_001, 120_001, 170_001), 2):
            self.assertTrue(sampler.commit_sample(
                self._sample(ordinal, OPEN_TIME + offset)
            ))
        self.assertEqual(sampler.frame_count, 4)
        self.assertEqual(sampler.generation, 4)
        self.assertTrue(sampler.confirm(lease, current_scan_id=700))
        self.assertEqual(len(sampler.snapshot()), 100)
        self.assertEqual(
            {len(window.observations) for window in sampler.snapshot().values()},
            {4},
        )

    def test_rotation_uses_frozen_n25_cohort_and_only_new_member_is_cold(self):
        sampler = MicroObservationSampler(boot_id="boot")
        original = tuple(self._sample(1, OPEN_TIME + 1))
        for ordinal, offset in enumerate((1, 60_001, 120_001, 180_001), 1):
            self.assertTrue(sampler.commit_sample(
                self._sample(ordinal, OPEN_TIME + offset)
            ))
        rotated = (*original[:-1], "NEWUSDT")
        self.assertTrue(sampler.commit_sample(
            self._sample_bundle_for_sampler(
                sampler,
                OPEN_TIME + 240_001,
                symbols=rotated,
            )
        ))
        lease = sampler.freeze_for_scan(
            scan_id=701,
            current_symbols=rotated,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 240_100,
        )
        existing = rotated[0]
        n25 = analyze_n25(
            lease.windows[existing], lease.windows,
            lease.captured_at_ms,
        )
        self.assertNotEqual(n25.reason, "N25_MARKET_CONTEXT_INSUFFICIENT")
        n22 = analyze_n22(
            lease.windows[existing], lease.windows,
            lease.captured_at_ms,
        )
        self.assertNotIn(n22.reason, {
            "N22_MARKET_CONTEXT_WARMING",
            "N22_MICRO_OBSERVATION_COLD_START",
        })
        self.assertEqual(
            analyze_n25(
                lease.windows["NEWUSDT"], lease.windows,
                lease.captured_at_ms,
            ).reason,
            "N25_MICRO_OBSERVATION_COLD_START",
        )
        self.assertEqual(
            analyze_n22(
                lease.windows["NEWUSDT"], lease.windows,
                lease.captured_at_ms,
            ).reason,
            "N22_MICRO_OBSERVATION_COLD_START",
        )
        self.assertTrue(sampler.confirm(lease, current_scan_id=701))

        self.assertTrue(sampler.commit_sample(
            self._sample_bundle_for_sampler(
                sampler,
                OPEN_TIME + 300_001,
                symbols=rotated,
            )
        ))
        recovered_lease = sampler.freeze_for_scan(
            scan_id=703,
            current_symbols=rotated,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 300_100,
        )
        self.assertNotEqual(
            analyze_n22(
                recovered_lease.windows[existing],
                recovered_lease.windows,
                recovered_lease.captured_at_ms,
            ).reason,
            "N22_MARKET_CONTEXT_WARMING",
        )
        self.assertEqual(
            analyze_n22(
                recovered_lease.windows["NEWUSDT"],
                recovered_lease.windows,
                recovered_lease.captured_at_ms,
            ).reason,
            "N22_MICRO_OBSERVATION_COLD_START",
        )
        self.assertTrue(sampler.confirm(recovered_lease, current_scan_id=703))

    def test_every_generation_rotation_keeps_mature_n22_n25_candidates_live(self):
        sampler = MicroObservationSampler(boot_id="boot")
        symbols = list(self._sample(1, OPEN_TIME + 1))
        for ordinal, offset in enumerate((1, 60_001, 120_001, 180_001), 1):
            self.assertTrue(sampler.commit_sample(
                self._sample(ordinal, OPEN_TIME + offset, symbols=tuple(symbols))
            ))
        mature_counts = []
        for ordinal in range(5, 13):
            replaced_index = 100 - ordinal
            symbols[replaced_index] = f"ROTATE{ordinal}USDT"
            observed_at = OPEN_TIME + (ordinal - 1) * 60_000 + 1
            self.assertTrue(sampler.commit_sample(
                self._sample_bundle_for_sampler(
                    sampler,
                    observed_at,
                    symbols=tuple(symbols),
                )
            ))
            lease = sampler.freeze_for_scan(
                scan_id=800 + ordinal,
                current_symbols=tuple(symbols),
                fallback_observations={},
                captured_at_ms=observed_at + 100,
            )
            mature = tuple(
                symbol
                for symbol, window in lease.windows.items()
                if len(window.observations) == 4
            )
            warming = tuple(
                symbol
                for symbol in mature
                if analyze_n22(
                    lease.windows[symbol],
                    lease.windows,
                    lease.captured_at_ms,
                ).reason in {
                    "N22_MARKET_CONTEXT_WARMING",
                    "N22_MICRO_OBSERVATION_COLD_START",
                }
            )
            n25_warming = tuple(
                symbol
                for symbol in mature
                if analyze_n25(
                    lease.windows[symbol],
                    lease.windows,
                    lease.captured_at_ms,
                ).reason in {
                    "N25_MARKET_CONTEXT_INSUFFICIENT",
                    "N25_MICRO_OBSERVATION_COLD_START",
                }
            )
            self.assertEqual(warming, ())
            self.assertEqual(n25_warming, ())
            self.assertEqual(
                analyze_n22(
                    lease.windows[symbols[replaced_index]],
                    lease.windows,
                    lease.captured_at_ms,
                ).reason,
                "N22_MICRO_OBSERVATION_COLD_START",
            )
            self.assertEqual(
                analyze_n25(
                    lease.windows[symbols[replaced_index]],
                    lease.windows,
                    lease.captured_at_ms,
                ).reason,
                "N25_MICRO_OBSERVATION_COLD_START",
            )
            mature_counts.append(len(mature))
            self.assertEqual(sampler.frame_count, 4)
            self.assertLessEqual(
                sum(
                    len(window.observations)
                    for window in lease.windows.values()
                ),
                400,
            )
            self.assertTrue(sampler.confirm(
                lease, current_scan_id=800 + ordinal
            ))
        self.assertEqual(mature_counts, [99, 98, 97, 97, 97, 97, 97, 97])

    def test_publish_abort_preserves_only_frames_newer_than_frozen_lease(self):
        sampler = MicroObservationSampler(boot_id="boot")
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        self.assertTrue(sampler.commit_sample(
            self._sample(1, OPEN_TIME + 1)
        ))
        lease = sampler.freeze_for_scan(
            scan_id=702,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 2,
        )
        self.assertTrue(sampler.commit_sample(
            self._sample(2, OPEN_TIME + 60_001)
        ))
        sampler.abort(lease)
        snapshot = sampler.snapshot()
        self.assertEqual(len(snapshot), 100)
        self.assertEqual(
            {
                window.observations[-1].generation
                for window in snapshot.values()
            },
            {2},
        )
        self.assertEqual(
            {len(window.observations) for window in snapshot.values()},
            {1},
        )

    def test_five_130_second_rounds_plus_poll_do_not_starve_windows(self):
        sampler = MicroObservationSampler(boot_id="boot")
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        next_sample_ordinal = 1
        lengths = []
        # A 130-second analysis round plus the configured 60-second poll
        # produces publication checkpoints at 130, 320, 510, 700 and 890s.
        # The independent 60-second sampler keeps running between them.
        for round_index, evaluation_offset in enumerate(
            (130_000, 320_000, 510_000, 700_000, 890_000),
            start=1,
        ):
            while (next_sample_ordinal - 1) * 60_000 <= evaluation_offset:
                observed_at_ms = (
                    OPEN_TIME + 1
                    + (next_sample_ordinal - 1) * 60_000
                )
                self.assertTrue(sampler.commit_sample(self._sample(
                    next_sample_ordinal,
                    observed_at_ms,
                )))
                next_sample_ordinal += 1
            lease = sampler.freeze_for_scan(
                scan_id=600 + round_index,
                current_symbols=symbols,
                fallback_observations={},
                captured_at_ms=OPEN_TIME + evaluation_offset,
            )
            lengths.append(len(lease.windows[symbols[0]].observations))
            self.assertTrue(sampler.confirm(
                lease,
                current_scan_id=600 + round_index,
            ))
        self.assertEqual(lengths, [3, 4, 4, 4, 4])

    def test_rotation_drops_old_member_and_new_member_is_cold(self):
        sampler = MicroObservationSampler(boot_id="boot")
        original = tuple(self._sample(1, OPEN_TIME + 1))
        for ordinal, offset in enumerate((1, 60_001, 120_001, 180_001), 1):
            sampler.commit_sample(
                self._sample(ordinal, OPEN_TIME + offset)
            )
        rotated = (*original[:-1], "NEWUSDT")
        sampler.commit_sample(
            self._sample(5, OPEN_TIME + 240_001, symbols=rotated)
        )

        lease = sampler.freeze_for_scan(
            scan_id=501,
            current_symbols=rotated,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 240_100,
        )
        self.assertNotIn(original[-1], lease.windows)
        self.assertEqual(len(lease.windows["NEWUSDT"].observations), 1)
        self.assertEqual(
            lease.windows["NEWUSDT"].reset_reason,
            "MICRO_OBSERVATION_COLD_START",
        )
        self.assertEqual(
            len(lease.windows[original[0]].observations),
            4,
        )
        sampler.abort(lease)
        self.assertEqual(sampler.snapshot(), {})

    def test_new_15m_candle_resets_every_window_without_crossing_source(self):
        sampler = MicroObservationSampler(boot_id="boot")
        first = self._sample(1, OPEN_TIME + 1)
        self.assertTrue(sampler.commit_sample(first))
        shifted = {}
        for symbol, item in self._sample(
            2,
            OPEN_TIME + 900_001,
        ).items():
            unsigned = replace(
                item,
                kline_open_time_ms=OPEN_TIME + 900_000,
                kline_close_time_ms=OPEN_TIME + 1_800_000 - 1,
                source_sha256="",
            )
            shifted[symbol] = replace(
                unsigned,
                source_sha256=hashlib.sha256(
                    canonical_json(unsigned.canonical_payload()).encode(
                        "utf-8"
                    )
                ).hexdigest(),
            )
        self.assertTrue(sampler.commit_sample(shifted))
        self.assertEqual(
            {len(window.observations) for window in sampler.snapshot().values()},
            {1},
        )
        self.assertEqual(
            {window.reset_reason for window in sampler.snapshot().values()},
            {"observation crossed 15m boundary"},
        )

    def test_interval_failure_and_lease_cas_fail_closed(self):
        sampler = MicroObservationSampler(boot_id="boot")
        first = self._sample(1, OPEN_TIME + 1)
        sampler.commit_sample(first)
        too_late = self._sample(2, OPEN_TIME + 150_002)
        sampler.commit_sample(too_late)
        self.assertEqual(
            {len(window.observations) for window in sampler.snapshot().values()},
            {1},
        )

        symbols = tuple(too_late)
        lease = sampler.freeze_for_scan(
            scan_id=502,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 150_100,
        )
        self.assertTrue(sampler.commit_sample(
            self._sample(3, OPEN_TIME + 210_002)
        ))
        self.assertTrue(sampler.confirm(lease, current_scan_id=502))

        second_lease = sampler.freeze_for_scan(
            scan_id=503,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 210_100,
        )
        sampler.mark_failed("forced source failure")
        self.assertTrue(
            sampler.confirm(second_lease, current_scan_id=503)
        )
        self.assertEqual(len(sampler.snapshot()), 100)
        with self.assertRaises(MicroObservationError):
            sampler.freeze_for_scan(
                scan_id=504,
                current_symbols=symbols,
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 360_003,
            )

    def test_single_worker_owner_stops_without_leak(self):
        sampler = MicroObservationSampler(
            boot_id="boot",
            sample_interval_ms=45_000,
        )
        entered = threading.Event()
        release = threading.Event()

        def collector(_identity, stop_event):
            entered.set()
            while not release.is_set() and not stop_event.is_set():
                stop_event.wait(0.001)
            return self._sample(1, OPEN_TIME + 1)

        self.assertTrue(sampler.start(collector, thread_name="micro-test"))
        self.assertTrue(entered.wait(1))
        self.assertFalse(sampler.start(collector, thread_name="duplicate"))
        release.set()
        self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(sampler.is_running)

    def test_worker_identity_race_is_epoch_bound_and_recovers_next_generation(self):
        sampler = MicroObservationSampler(
            boot_id="epoch-race",
            sample_interval_ms=45_000,
        )
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        first_barrier = threading.Barrier(2)
        release_first = threading.Event()
        late_commit_rejected = threading.Event()
        recovered = threading.Event()
        identities = []
        attempts = [0]

        def collector(identity, stop_event):
            attempts[0] += 1
            identities.append(identity)
            if attempts[0] == 1:
                first_barrier.wait(timeout=1)
                release_first.wait(1)
                return self._sample_for_identity(
                    identity,
                    OPEN_TIME + 1,
                    symbols=symbols,
                )
            if attempts[0] == 2:
                recovered.set()
                return self._sample_for_identity(
                    identity,
                    OPEN_TIME + 60_001,
                    symbols=symbols,
                )
            stop_event.wait()
            raise MicroObservationError("sampler stopped")

        def on_error(_name):
            late_commit_rejected.set()

        ticks = iter(range(0, 10_000, 46))
        with patch(
            "trading_bot.micro_observation.time.monotonic",
            side_effect=lambda: next(ticks),
        ):
            try:
                self.assertTrue(sampler.start(
                    collector,
                    thread_name="micro-epoch-race",
                    on_error=on_error,
                ))
                first_barrier.wait(timeout=1)
                self.assertEqual(len(identities), 1)
                worker_identity = identities[0]
                fallback_identities = []

                def fallback(identity):
                    fallback_identities.append(identity)
                    return self._sample_for_identity(
                        identity,
                        OPEN_TIME + 1,
                        symbols=symbols,
                    )

                lease = sampler.freeze_for_scan(
                    scan_id=950,
                    current_symbols=symbols,
                    fallback_observations={},
                    fallback_factory=fallback,
                    captured_at_ms=OPEN_TIME + 2,
                )
                self.assertEqual(len(fallback_identities), 1)
                self.assertEqual(
                    fallback_identities[0].lifecycle_epoch,
                    worker_identity.lifecycle_epoch,
                )
                self.assertEqual(
                    fallback_identities[0].generation,
                    worker_identity.generation,
                )
                release_first.set()
                self.assertTrue(late_commit_rejected.wait(1))
                self.assertTrue(recovered.wait(1))
                deadline = time.perf_counter() + 1
                while sampler.generation < 2:
                    if time.perf_counter() >= deadline:
                        self.fail("sampler did not recover the next generation")
                    time.sleep(0.001)
                self.assertEqual(sampler.frame_count, 2)
                self.assertTrue(sampler.confirm(
                    lease,
                    current_scan_id=950,
                ))
            finally:
                release_first.set()
                self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(sampler.is_running)

    def test_stop_and_restart_strictly_rotate_the_lease_epoch(self):
        sampler = MicroObservationSampler(
            boot_id="stop-epoch",
            sample_interval_ms=45_000,
        )
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        identities = []

        def start_one_generation(label, observed_at_ms):
            barrier = threading.Barrier(2)
            calls = [0]

            def collector(identity, stop_event):
                calls[0] += 1
                identities.append(identity)
                if calls[0] == 1:
                    barrier.wait(timeout=1)
                    return self._sample_for_identity(
                        identity,
                        observed_at_ms,
                        symbols=symbols,
                    )
                stop_event.wait()
                raise MicroObservationError("sampler stopped")

            self.assertTrue(sampler.start(
                collector,
                thread_name=f"micro-{label}",
            ))
            barrier.wait(timeout=1)
            deadline = time.perf_counter() + 1
            while len(sampler.snapshot()) != 100:
                if time.perf_counter() >= deadline:
                    self.fail(f"{label} generation did not commit")
                time.sleep(0.001)
            self.assertTrue(sampler.is_running)
            return identities[-1]

        first_identity = start_one_generation(
            "stop-before-freeze",
            OPEN_TIME + 1,
        )
        self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(sampler.is_running)
        with self.assertRaisesRegex(
            MicroObservationError,
            "lifecycle is stopped",
        ):
            sampler.freeze_for_scan(
                scan_id=960,
                current_symbols=symbols,
                fallback_observations={},
                fallback_factory=lambda identity: self._sample_for_identity(
                    identity,
                    OPEN_TIME + 2,
                    symbols=symbols,
                ),
                captured_at_ms=OPEN_TIME + 3,
            )

        second_identity = start_one_generation(
            "stop-after-freeze",
            OPEN_TIME + 60_001,
        )
        second_lease = sampler.freeze_for_scan(
            scan_id=961,
            current_symbols=symbols,
            fallback_observations={},
            captured_at_ms=OPEN_TIME + 60_100,
        )
        self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(sampler.confirm(
            second_lease,
            current_scan_id=961,
        ))
        self.assertFalse(sampler.commit_sample(
            self._sample_for_identity(
                second_identity,
                OPEN_TIME + 60_001,
                symbols=symbols,
            ),
            identity=second_identity,
        ))

        third_identity = start_one_generation(
            "restart",
            OPEN_TIME + 120_001,
        )
        try:
            self.assertGreater(
                third_identity.lifecycle_epoch,
                second_identity.lifecycle_epoch,
            )
            self.assertGreater(
                second_identity.lifecycle_epoch,
                first_identity.lifecycle_epoch,
            )
            self.assertFalse(sampler.confirm(
                second_lease,
                current_scan_id=961,
            ))
            third_lease = sampler.freeze_for_scan(
                scan_id=962,
                current_symbols=symbols,
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 120_100,
            )
            self.assertEqual(
                third_lease.lifecycle_epoch,
                third_identity.lifecycle_epoch,
            )
            self.assertTrue(sampler.confirm(
                third_lease,
                current_scan_id=962,
            ))
        finally:
            self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(any(
            thread.name.startswith("micro-stop-")
            or thread.name == "micro-restart"
            for thread in threading.enumerate()
        ))

    def test_thread_start_failure_rolls_back_owner_and_stop_never_joins_it(self):
        sampler = MicroObservationSampler(
            boot_id="start-failure",
            sample_interval_ms=45_000,
        )

        with patch.object(
            threading.Thread,
            "start",
            side_effect=RuntimeError("injected thread start failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected thread"):
                sampler.start(lambda _identity, _stop: {})

        self.assertFalse(sampler.is_running)
        with patch.object(threading.Thread, "join") as join:
            self.assertTrue(sampler.stop(timeout_seconds=1))
            join.assert_not_called()
        self.assertTrue(sampler.stop(timeout_seconds=1))

        entered = threading.Barrier(2)

        def collector(identity, stop_event):
            entered.wait(timeout=1)
            return self._sample_for_identity(
                identity,
                OPEN_TIME + 1,
            )

        try:
            self.assertTrue(sampler.start(
                collector,
                thread_name="micro-start-retry",
            ))
            entered.wait(timeout=1)
            deadline = time.perf_counter() + 1
            while len(sampler.snapshot()) != 100:
                if time.perf_counter() >= deadline:
                    self.fail("sampler did not recover after start failure")
                time.sleep(0.001)
            self.assertTrue(sampler.is_running)
        finally:
            self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(any(
            thread.name == "micro-start-retry"
            for thread in threading.enumerate()
        ))

    def test_worker_death_retires_epoch_and_aba_identity_cannot_cross_restart(self):
        sampler = MicroObservationSampler(
            boot_id="worker-death",
            sample_interval_ms=45_000,
        )
        failed_barrier = threading.Barrier(2)
        error_called = threading.Event()
        dead_identities = []

        def failing_collector(identity, _stop_event):
            dead_identities.append(identity)
            failed_barrier.wait(timeout=1)
            raise OSError("injected collector failure")

        def failing_on_error(_error_type):
            error_called.set()
            raise RuntimeError("injected error callback failure")

        with patch("threading.excepthook") as exception_hook:
            self.assertTrue(sampler.start(
                failing_collector,
                thread_name="micro-worker-death",
                on_error=failing_on_error,
            ))
            failed_barrier.wait(timeout=1)
            self.assertTrue(error_called.wait(1))
            deadline = time.perf_counter() + 1
            while (
                sampler.is_running
                or any(
                    thread.name == "micro-worker-death"
                    for thread in threading.enumerate()
                )
            ):
                if time.perf_counter() >= deadline:
                    self.fail("dead worker retained the active lifecycle")
                time.sleep(0.001)
            exception_hook.assert_called_once()
        self.assertEqual(len(dead_identities), 1)
        dead_identity = dead_identities[0]
        with self.assertRaisesRegex(
            MicroObservationError,
            "lifecycle is stopped",
        ):
            sampler.freeze_for_scan(
                scan_id=970,
                current_symbols=tuple(self._sample(1, OPEN_TIME + 1)),
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 2,
            )
        self.assertTrue(sampler.stop(timeout_seconds=1))

        restart_barrier = threading.Barrier(2)
        release_restart = threading.Event()
        restarted_identities = []

        def restarted_collector(identity, stop_event):
            restarted_identities.append(identity)
            restart_barrier.wait(timeout=1)
            release_restart.wait(1)
            if stop_event.is_set():
                raise MicroObservationError("sampler stopped")
            return self._sample_for_identity(
                identity,
                OPEN_TIME + 60_001,
            )

        try:
            self.assertTrue(sampler.start(
                restarted_collector,
                thread_name="micro-worker-restart",
            ))
            restart_barrier.wait(timeout=1)
            self.assertEqual(len(restarted_identities), 1)
            restarted_identity = restarted_identities[0]
            aba_identity = replace(
                restarted_identity,
                lifecycle_epoch=dead_identity.lifecycle_epoch,
            )
            self.assertEqual(
                (
                    aba_identity.sample_ordinal,
                    aba_identity.generation,
                    aba_identity.boot_id,
                ),
                (
                    restarted_identity.sample_ordinal,
                    restarted_identity.generation,
                    restarted_identity.boot_id,
                ),
            )
            self.assertFalse(sampler.commit_sample(
                self._sample_for_identity(
                    restarted_identity,
                    OPEN_TIME + 60_001,
                ),
                identity=aba_identity,
            ))
            release_restart.set()
            deadline = time.perf_counter() + 1
            while len(sampler.snapshot()) != 100:
                if time.perf_counter() >= deadline:
                    self.fail("sampler did not recover after worker death")
                time.sleep(0.001)
            symbols = tuple(sampler.snapshot())
            lease = sampler.freeze_for_scan(
                scan_id=971,
                current_symbols=symbols,
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 60_002,
            )
            forged_lease = replace(
                lease,
                lifecycle_epoch=dead_identity.lifecycle_epoch,
            )
            self.assertFalse(sampler.confirm(
                forged_lease,
                current_scan_id=971,
            ))
            replacement = sampler.freeze_for_scan(
                scan_id=972,
                current_symbols=symbols,
                fallback_observations={},
                captured_at_ms=OPEN_TIME + 60_003,
            )
            self.assertTrue(sampler.confirm(
                replacement,
                current_scan_id=972,
            ))
        finally:
            release_restart.set()
            self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(any(
            thread.name in {"micro-worker-death", "micro-worker-restart"}
            for thread in threading.enumerate()
        ))

    def test_blocked_excepthook_retains_owner_until_thread_is_really_dead(self):
        sampler = MicroObservationSampler(
            boot_id="blocked-excepthook",
            sample_interval_ms=45_000,
        )
        symbols = tuple(self._sample(1, OPEN_TIME + 1))
        collector_barrier = threading.Barrier(2)
        release_collector = threading.Event()
        hook_entered = threading.Event()
        release_hook = threading.Event()
        worker_identities = []

        def collector(identity, _stop_event):
            worker_identities.append(identity)
            collector_barrier.wait(timeout=1)
            release_collector.wait(1)
            return self._sample_for_identity(
                identity,
                OPEN_TIME + 1,
                symbols=symbols,
            )

        def fail_on_rejected_commit(_error_type):
            raise RuntimeError("force worker excepthook")

        def blocking_excepthook(_args):
            hook_entered.set()
            release_hook.wait(1)

        owner = None
        with patch("threading.excepthook", side_effect=blocking_excepthook):
            try:
                self.assertTrue(sampler.start(
                    collector,
                    thread_name="micro-blocked-excepthook",
                    on_error=fail_on_rejected_commit,
                ))
                collector_barrier.wait(timeout=1)
                self.assertEqual(len(worker_identities), 1)

                lease = sampler.freeze_for_scan(
                    scan_id=980,
                    current_symbols=symbols,
                    fallback_observations={},
                    fallback_factory=lambda identity: (
                        self._sample_for_identity(
                            identity,
                            OPEN_TIME + 1,
                            symbols=symbols,
                        )
                    ),
                    captured_at_ms=OPEN_TIME + 2,
                )
                release_collector.set()
                self.assertTrue(hook_entered.wait(1))
                owner = next(
                    thread for thread in threading.enumerate()
                    if thread.name == "micro-blocked-excepthook"
                )
                self.assertTrue(owner.is_alive())
                self.assertIs(sampler._thread, owner)
                self.assertFalse(sampler.is_running)
                self.assertFalse(sampler.stop(timeout_seconds=0.01))
                self.assertIs(sampler._thread, owner)
                self.assertFalse(sampler.start(
                    lambda _identity, _stop_event: {},
                    thread_name="micro-overlap-must-not-start",
                ))
                with self.assertRaisesRegex(
                    MicroObservationError,
                    "lifecycle is stopped",
                ):
                    sampler.freeze_for_scan(
                        scan_id=981,
                        current_symbols=symbols,
                        fallback_observations={},
                        captured_at_ms=OPEN_TIME + 3,
                    )
                self.assertFalse(sampler.confirm(
                    lease,
                    current_scan_id=980,
                ))
            finally:
                release_collector.set()
                release_hook.set()

        self.assertIsNotNone(owner)
        deadline = time.perf_counter() + 1
        while owner.is_alive():
            if time.perf_counter() >= deadline:
                self.fail("worker remained alive after excepthook release")
            time.sleep(0.001)
        self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertIsNone(sampler._thread)

        restarted = threading.Barrier(2)

        def restarted_collector(identity, stop_event):
            restarted.wait(timeout=1)
            return self._sample_for_identity(
                identity,
                OPEN_TIME + 60_001,
                symbols=symbols,
            )

        try:
            self.assertTrue(sampler.start(
                restarted_collector,
                thread_name="micro-after-excepthook",
            ))
            restarted.wait(timeout=1)
            deadline = time.perf_counter() + 1
            while len(sampler.snapshot()) != 100:
                if time.perf_counter() >= deadline:
                    self.fail("sampler did not restart after owner death")
                time.sleep(0.001)
        finally:
            self.assertTrue(sampler.stop(timeout_seconds=1))
        self.assertFalse(any(
            thread.name in {
                "micro-blocked-excepthook",
                "micro-overlap-must-not-start",
                "micro-after-excepthook",
            }
            for thread in threading.enumerate()
        ))

    def test_successful_collection_due_time_preserves_interval_bounds(self):
        sampler = MicroObservationSampler(
            boot_id="bounded-due",
            sample_interval_ms=60_000,
        )
        self.assertEqual(
            sampler._next_collection_due(0, 14, succeeded=True),
            60,
        )
        self.assertEqual(
            sampler._next_collection_due(0, 45, succeeded=True),
            90,
        )
        self.assertEqual(
            sampler._next_collection_due(60, 75, succeeded=True),
            120,
        )
        self.assertEqual(
            sampler._next_collection_due(0, 75, succeeded=False),
            120,
        )

    def test_worker_source_failure_clears_then_recovers_next_generation(self):
        sampler = MicroObservationSampler(
            boot_id="boot",
            sample_interval_ms=45_000,
        )
        failed = threading.Event()
        recovery_collector_entered = threading.Event()
        attempts = [0]

        def collector(identity, stop_event):
            attempts[0] += 1
            if attempts[0] == 1:
                raise OSError("injected public source failure")
            if attempts[0] == 2:
                recovery_collector_entered.set()
                return self._sample_for_identity(
                    identity,
                    OPEN_TIME + 60_001,
                )
            stop_event.wait()
            raise MicroObservationError("sampler stopped")

        ticks = iter(range(0, 10_000, 46))
        with patch(
            "trading_bot.micro_observation.time.monotonic",
            side_effect=lambda: next(ticks),
        ):
            try:
                self.assertTrue(sampler.start(
                    collector,
                    thread_name="micro-recovery",
                    on_error=lambda _name: failed.set(),
                ))
                self.assertTrue(failed.wait(1))
                self.assertTrue(
                    recovery_collector_entered.wait(1),
                    "recovery collector was not entered",
                )
                deadline = time.perf_counter() + 1
                while len(sampler.snapshot()) != 100:
                    if time.perf_counter() >= deadline:
                        self.fail("sampler did not commit the recovery generation")
                    time.sleep(0.001)
                self.assertGreaterEqual(attempts[0], 2)
                self.assertEqual(
                    {
                        item.observations[-1].generation
                        for item in sampler.snapshot().values()
                    },
                    {2},
                )
            finally:
                self.assertTrue(sampler.stop(timeout_seconds=1))

    def test_worker_rejects_invalid_commit_during_lease_and_recovers(self):
        for variant in ("incomplete", "identity", "digest"):
            with self.subTest(variant=variant):
                sampler = MicroObservationSampler(
                    boot_id="boot",
                    sample_interval_ms=45_000,
                )
                symbols = tuple(self._sample(1, OPEN_TIME + 1))
                errors = []
                failed = threading.Event()
                recovered = threading.Event()
                invalid_entered = threading.Event()
                release_invalid = threading.Event()
                attempts = [0]

                def collector(identity, stop_event):
                    attempts[0] += 1
                    rows = self._sample_for_identity(
                        identity,
                        OPEN_TIME + attempts[0] * 60_000 + 1,
                    )
                    if attempts[0] == 1:
                        return rows
                    if attempts[0] == 2:
                        if variant == "incomplete":
                            rows.pop(next(iter(rows)))
                        elif variant == "identity":
                            first = next(iter(rows))
                            rows[first] = self._replace_and_hash(
                                rows[first],
                                boot_id="wrong-boot",
                            )
                        else:
                            first = next(iter(rows))
                            rows[first] = replace(
                                rows[first],
                                source_sha256="0" * 64,
                            )
                        invalid_entered.set()
                        release_invalid.wait(1)
                        return rows
                    if attempts[0] == 3:
                        recovered.set()
                        return rows
                    stop_event.wait()
                    raise MicroObservationError("sampler stopped")

                def on_error(name):
                    errors.append(name)
                    failed.set()

                ticks = iter(range(0, 10_000, 46))
                with patch(
                    "trading_bot.micro_observation.time.monotonic",
                    side_effect=lambda: next(ticks),
                ):
                    try:
                        self.assertTrue(sampler.start(
                            collector,
                            thread_name=f"micro-invalid-{variant}",
                            on_error=on_error,
                        ))
                        self.assertTrue(invalid_entered.wait(1))
                        lease = sampler.freeze_for_scan(
                            scan_id=910,
                            current_symbols=symbols,
                            fallback_observations={},
                            captured_at_ms=OPEN_TIME + 60_100,
                        )
                        release_invalid.set()
                        self.assertTrue(failed.wait(1))
                        self.assertTrue(recovered.wait(1))
                        deadline = time.perf_counter() + 1
                        while sampler.generation < 3:
                            if time.perf_counter() >= deadline:
                                self.fail("valid recovery generation was not committed")
                            time.sleep(0.001)
                        self.assertEqual(len(errors), 1)
                        self.assertTrue(sampler.confirm(
                            lease,
                            current_scan_id=910,
                        ))
                        self.assertEqual(len(sampler.snapshot()), 100)
                    finally:
                        release_invalid.set()
                        self.assertTrue(sampler.stop(timeout_seconds=1))
                self.assertFalse(sampler.is_running)

    def test_sampler_file_descriptor_churn_does_not_break_database_attestation(self):
        with tempfile.TemporaryDirectory() as directory:
            review_path = Path(directory) / "review.sqlite3"
            noise_path = Path(directory) / "public-source-ca-bundle"
            noise_path.write_bytes(b"public sampler fixture")
            recorder = make_test_recorder(
                str(review_path),
                logging.getLogger("micro-sampler-fd-interleave"),
            )
            original_connect = sqlite3.connect

            def run_during_sampler_fd_churn(index, operation):
                sampler = MicroObservationSampler(
                    boot_id=f"sampler-fd-interleave-{index}",
                    sample_interval_ms=60_000,
                )
                connect_window = threading.Event()
                unrelated_fd_open = threading.Event()
                release_collector = threading.Event()

                def collector(identity, stop_event):
                    if not connect_window.wait(5):
                        raise RuntimeError(
                            "database connection window was not reached"
                        )
                    descriptor = os.open(str(noise_path), os.O_RDONLY)
                    try:
                        unrelated_fd_open.set()
                        if not release_collector.wait(5):
                            raise RuntimeError("collector release timed out")
                        if stop_event.is_set():
                            return {}
                        return self._sample_for_identity(
                            identity,
                            OPEN_TIME + 60_000,
                        )
                    finally:
                        os.close(descriptor)

                first_connect = [True]

                def interleaved_connect(*args, **kwargs):
                    if first_connect[0]:
                        first_connect[0] = False
                        connect_window.set()
                        if not unrelated_fd_open.wait(5):
                            raise RuntimeError(
                                "sampler did not open its unrelated file"
                            )
                    return original_connect(*args, **kwargs)

                self.assertTrue(sampler.start(collector))
                try:
                    with patch(
                        "trading_bot.recorder.sqlite3.connect",
                        side_effect=interleaved_connect,
                    ):
                        return operation()
                finally:
                    release_collector.set()
                    self.assertTrue(sampler.stop(timeout_seconds=5))

            self.assertIsNone(run_during_sampler_fd_churn(
                1,
                recorder.current_strategy_signal_scan_id,
            ))
            self.assertTrue(run_during_sampler_fd_churn(
                2,
                lambda: recorder.record_event(
                    "MICRO_SAMPLER_FD_INTERLEAVE_TEST",
                    {"bounded": True},
                ),
            ))
            self.assertEqual(
                run_during_sampler_fd_churn(
                    3,
                    recorder.n16_claim_ledger.metadata_phase,
                ),
                "READY",
            )
            self.assertIsNotNone(run_during_sampler_fd_churn(
                4,
                recorder.n16_claim_ledger.protected_generation_highwater,
            ))

    def test_delayed_scheduler_lease_publishes_warmed_n21_analysis(self):
        symbols = tuple(
            "BTCUSDT" if rank == 1
            else "FLOWUSDT" if rank == 7
            else f"S{rank:03d}USDT"
            for rank in range(1, 101)
        )
        sampler = MicroObservationSampler(boot_id="boot")
        flow_values = (
            ("100", "1000", 100, "500"),
            ("100.05", "1100", 110, "550"),
            ("100.10", "1200", 120, "602"),
            ("100.30", "1300", 130, "658"),
        )
        for ordinal, offset in enumerate((1, 60_001, 120_001, 180_001), 1):
            sample = self._sample(
                ordinal,
                OPEN_TIME + offset,
                symbols=symbols,
            )
            close, quote, trades, taker = flow_values[ordinal - 1]
            sample["FLOWUSDT"] = observation(
                "FLOWUSDT",
                ordinal,
                ordinal,
                OPEN_TIME + offset,
                close=close,
                low="99.5",
                quote=quote,
                trades=trades,
                taker=taker,
                rank=7,
            )
            sample = self._bind_universe(sample)
            self.assertTrue(sampler.commit_sample(sample))

        with tempfile.TemporaryDirectory() as directory:
            recorder = make_test_recorder(
                Path(directory) / "review.sqlite3",
                logging.getLogger("micro-delayed-lease"),
            )
            for _ in range(3):
                warm_scan = recorder.begin_scan(0, [], True)
                self.assertIsInstance(
                    recorder.record_strategy_signal(
                        scan_id=warm_scan,
                        strategy_id="N21",
                        symbol="WARMUPUSDT",
                        funding_rate="",
                        matched_patterns=(),
                        trend_slope="",
                        current_bullish=False,
                        passed=False,
                        decision="REJECTED",
                        reason="N21_MICRO_OBSERVATION_COLD_START",
                    ),
                    int,
                )
                self.assertTrue(
                    recorder.publish_strategy_signal_batch(warm_scan, 1)
                )
            candidate = FundingCandidate(
                "FLOWUSDT",
                None,
                Decimal("100.30"),
                quote_volume=Decimal("1000000"),
                quote_volume_rank=7,
                candidate_universe="quote_volume_top",
            )
            scan_id = recorder.begin_scan(100, [candidate], True)
            self.assertEqual(scan_id, 4)
            lease = None
            provider_calls = 0

            def provider():
                nonlocal lease, provider_calls
                provider_calls += 1
                lease = sampler.freeze_for_scan(
                    scan_id=scan_id,
                    current_symbols=symbols,
                    fallback_observations={},
                    captured_at_ms=OPEN_TIME + 180_100,
                )
                return lease.windows, lease.captured_at_ms

            strategy = next(
                item for item in load_all_strategies()
                if item.strategy_id == "N21"
            )
            result = StrategyScheduler(
                (strategy,),
                96,
                recorder,
                logging.getLogger("micro-delayed-lease"),
            ).evaluate(
                scan_id,
                {"quote_volume_top": [candidate]},
                {},
                checked_at_ms=OPEN_TIME + 1,
                micro_window_provider=provider,
            )
            self.assertEqual(provider_calls, 1)
            self.assertTrue(result.signal_batch_published)
            self.assertEqual(len(result.passed_signals), 1)
            self.assertEqual(result.passed_signals[0].reason, "PASSED")
            self.assertIsNotNone(lease)
            self.assertTrue(sampler.confirm(lease, current_scan_id=scan_id))


if __name__ == "__main__":
    unittest.main()
