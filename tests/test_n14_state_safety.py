from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import logging
import tempfile
import unittest
from unittest.mock import patch

from trading_bot.n14_snapshot import build_n14_snapshot, decode_n14_snapshot
from trading_bot.strategies import N14_STRATEGY
from trading_bot.strategy_scheduler import (
    N14BatchContext,
    StrategyScheduler,
    StrategySignalDecision,
    _n14_terminal_envelope,
)
from tests.test_n14 import (
    analyze,
    candidate,
    full_batch,
    kline,
    n14_klines,
    recorder_scheduler,
    resign_snapshot,
    scenario,
)


def _canonical_sha256(value):
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class N14StateSafetyRegressionTests(unittest.TestCase):
    def _evaluate_published(
        self,
        recorder,
        scheduler,
        groups,
        raws,
        checked_at_ms,
    ):
        candidates_by_symbol = {
            item.symbol: item
            for group in groups.values()
            for item in group
        }
        candidates = list(candidates_by_symbol.values())
        scan_id = recorder.begin_scan(len(candidates), candidates, True)
        self.assertIsNotNone(scan_id)
        result = scheduler.evaluate(
            scan_id,
            groups,
            raws,
            checked_at_ms=checked_at_ms,
        )
        self.assertTrue(result.signal_batch_published)
        return result

    def test_resigned_snapshot_cannot_change_shock_evidence_or_frozen_config(self):
        items, raws, _ = full_batch()
        payload, _, _ = build_n14_snapshot(
            N14_STRATEGY,
            items,
            {symbol: rows[:120] for symbol, rows in raws.items()},
        )

        broken_shock = deepcopy(payload)
        broken_shock["rows"][0]["shock_assessment"][
            "body_atr_multiple"
        ] = "999"
        resign_snapshot(broken_shock)
        with self.assertRaises(ValueError):
            decode_n14_snapshot(broken_shock, "N14")

        broken_config = deepcopy(payload)
        broken_config["config"]["shock_body_atr_min"] = "0.81"
        broken_config["config_signature"] = _canonical_sha256(
            broken_config["config"]
        )
        resign_snapshot(broken_config)
        with self.assertRaises(ValueError):
            decode_n14_snapshot(broken_config, "N14")

    def test_scheduler_rejects_replaced_raw_s_candle_and_preserves_active(self):
        items, raws, _ = full_batch()
        target = items[0].symbol
        s_time = str(raws[target][118][0])
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            for current_index in range(113, 120):
                batch = {
                    symbol: rows[: current_index + 1]
                    for symbol, rows in raws.items()
                }
                self._evaluate_published(
                    recorder,
                    scheduler,
                    {"quote_volume_top": items, "negative_funding": []},
                    batch,
                    batch[target][-1][0] + 30_000,
                )

            active_before = recorder.get_validated_n14_active_episode(
                "N14", target, s_time
            )
            self.assertIsNotNone(active_before)
            self.assertEqual(active_before["stage"], "S_LOCKED")

            changed = deepcopy(raws)
            changed[target][118][2] = "100.06"
            batch = {
                symbol: rows[:121] for symbol, rows in changed.items()
            }
            result = self._evaluate_published(
                recorder,
                scheduler,
                {"quote_volume_top": items, "negative_funding": []},
                batch,
                batch[target][-1][0] + 30_000,
            )
            target_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == target
            )
            self.assertEqual(target_signal.reason, "N14_STATE_INCONSISTENT")
            self.assertEqual(result.passed_signals, [])
            self.assertEqual(result.live_candidates, [])
            active_after = recorder.get_validated_n14_active_episode(
                "N14", target, s_time
            )
            self.assertEqual(active_after, active_before)

    def test_c_confirmed_waiting_crosses_e_after_restart_and_is_consumed(self):
        items, raws, _ = full_batch()
        target = items[0].symbol
        s_time = str(raws[target][118][0])
        waiting = deepcopy(raws)
        waiting[target][121][4] = "99.95"

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            final_waiting = None
            for current_index in range(113, 122):
                batch = {
                    symbol: rows[: current_index + 1]
                    for symbol, rows in waiting.items()
                }
                final_waiting = self._evaluate_published(
                    recorder,
                    scheduler,
                    {"quote_volume_top": items, "negative_funding": []},
                    batch,
                    batch[target][-1][0] + 30_000,
                )
            assert final_waiting is not None
            waiting_signal = next(
                signal
                for signal in final_waiting.signals
                if signal.candidate.symbol == target
            )
            self.assertEqual(waiting_signal.reason, "N14_WAITING_ENTRY_PRICE")
            active = recorder.get_validated_n14_active_episode(
                "N14", target, s_time
            )
            self.assertIsNotNone(active)
            self.assertEqual(active["stage"], "C_CONFIRMED")

            restarted = StrategyScheduler(
                (N14_STRATEGY,),
                96,
                recorder,
                logging.getLogger("n14_state_safety_restart"),
            )
            crossed = deepcopy(waiting)
            for rows in crossed.values():
                rows.append(kline(122))
            result = self._evaluate_published(
                recorder,
                restarted,
                {"quote_volume_top": items, "negative_funding": []},
                crossed,
                crossed[target][-1][0] + 30_000,
            )
            target_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == target
            )
            self.assertEqual(
                target_signal.reason, "HISTORICAL_N14_ENTRY_MISSED"
            )
            terminal = recorder.get_n14_sell_impact_state(
                "N14", target, s_time
            )
            self.assertIsNotNone(terminal)
            self.assertEqual(terminal[5], "MISSED")
            self.assertEqual(terminal[6], "HISTORICAL_N14_ENTRY_MISSED")
            self.assertIsNone(
                recorder.get_n14_active_episode("N14", target, s_time)
            )
            self.assertEqual(result.passed_signals, [])

    def test_resigned_terminal_status_reason_mismatches_are_rejected(self):
        raw, checked = n14_klines()
        structure = analyze(raw, checked).structure
        self.assertIsNotNone(structure)
        s_time = str(structure.s.open_time_ms)
        valid = _n14_terminal_envelope(
            "N14",
            "N14USDT",
            s_time,
            structure.structure_id,
            "CONSUMED",
            "PASSED",
            "fixed-config-signature",
            structure,
            {},
        )
        mismatches = (
            ("INVALID", "PASSED"),
            ("CONSUMED", "N14_ENTRY_WINDOW_EXPIRED"),
            ("MISSED", "N14_SYSTEMIC_CRASH_VETO"),
            ("MISSED", "ARBITRARY_RESIGNED_REASON"),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, _ = recorder_scheduler(tmpdir)
            for status, reason in mismatches:
                with self.subTest(status=status, reason=reason):
                    tampered = deepcopy(valid)
                    tampered["status"] = status
                    tampered["reason"] = reason
                    resign_snapshot(tampered)
                    record = {
                        "strategy_id": "N14",
                        "symbol": "N14USDT",
                        "s_time": s_time,
                        "structure_id": structure.structure_id,
                        "status": status,
                        "reason": reason,
                        "detail": tampered,
                    }
                    self.assertEqual(
                        recorder.record_n14_state_batch_atomically(
                            [record], []
                        ),
                        "N14_STATE_INCONSISTENT",
                    )

    def test_best_candidate_state_failure_never_promotes_runner_up(self):
        raw, checked = n14_klines()
        top_raw = deepcopy(raw)
        top_raw[120][10] = "48"
        runner_raw = deepcopy(raw)
        runner_raw[120][10] = "46"
        top_analysis = analyze(
            top_raw,
            checked,
            market_scenario=replace(
                scenario(top_raw), quote_volume_rank=1
            ),
        )
        runner_analysis = analyze(
            runner_raw,
            checked,
            market_scenario=replace(
                scenario(runner_raw), quote_volume_rank=2
            ),
        )
        self.assertTrue(top_analysis.passed)
        self.assertTrue(runner_analysis.passed)
        self.assertGreater(top_analysis.flow_flip, runner_analysis.flow_flip)

        top = candidate("TOPUSDT", 1)
        runner = candidate("RUNNERUSDT", 2)
        groups = {
            "quote_volume_top": [top, runner],
            "negative_funding": [],
        }
        raws = {top.symbol: top_raw, runner.symbol: runner_raw}

        for failure_reason in (
            "N14_STATE_READ_FAILED",
            "N14_STATE_PERSIST_FAILED",
        ):
            with self.subTest(failure_reason=failure_reason), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)

                def evaluate_candidate(strategy, item, *args, **kwargs):
                    if item.symbol == top.symbol:
                        return StrategySignalDecision(
                            strategy,
                            item,
                            top_analysis,
                            False,
                            "REJECTED",
                            failure_reason,
                        )
                    return StrategySignalDecision(
                        strategy,
                        item,
                        runner_analysis,
                        True,
                        "PASSED",
                        "PASSED",
                    )

                with patch.object(
                    scheduler,
                    "_build_n14_batch_context",
                    return_value=N14BatchContext({}, checked, True),
                ), patch.object(
                    scheduler,
                    "_evaluate_candidate",
                    side_effect=evaluate_candidate,
                ):
                    result = self._evaluate_published(
                        recorder,
                        scheduler,
                        groups,
                        raws,
                        checked,
                    )

                reasons = {
                    signal.candidate.symbol: signal.reason
                    for signal in result.signals
                }
                self.assertEqual(reasons[top.symbol], failure_reason)
                self.assertEqual(reasons[runner.symbol], "PASSED")
                self.assertEqual(result.passed_signals, [])
                self.assertEqual(result.live_candidates, [])

    def test_runner_state_failure_does_not_block_healthy_intrinsic_winner(self):
        raw, checked = n14_klines()
        top_raw = deepcopy(raw)
        top_raw[120][10] = "48"
        runner_raw = deepcopy(raw)
        runner_raw[120][10] = "46"
        top_analysis = analyze(
            top_raw,
            checked,
            market_scenario=replace(
                scenario(top_raw), quote_volume_rank=1
            ),
        )
        runner_analysis = analyze(
            runner_raw,
            checked,
            market_scenario=replace(
                scenario(runner_raw), quote_volume_rank=2
            ),
        )
        top = candidate("TOPUSDT", 1)
        runner = candidate("RUNNERUSDT", 2)

        for failure_reason in (
            "N14_STATE_READ_FAILED",
            "N14_STATE_PERSIST_FAILED",
        ):
            with self.subTest(
                failure_reason=failure_reason
            ), tempfile.TemporaryDirectory() as tmpdir:
                recorder, scheduler = recorder_scheduler(tmpdir)

                def evaluate_candidate(strategy, item, *args, **kwargs):
                    if item.symbol == top.symbol:
                        return StrategySignalDecision(
                            strategy,
                            item,
                            top_analysis,
                            True,
                            "PASSED",
                            "PASSED",
                        )
                    return StrategySignalDecision(
                        strategy,
                        item,
                        runner_analysis,
                        False,
                        "REJECTED",
                        failure_reason,
                    )

                with patch.object(
                    scheduler,
                    "_build_n14_batch_context",
                    return_value=N14BatchContext({}, checked, True),
                ), patch.object(
                    scheduler,
                    "_evaluate_candidate",
                    side_effect=evaluate_candidate,
                ):
                    result = self._evaluate_published(
                        recorder,
                        scheduler,
                        {
                            "quote_volume_top": [top, runner],
                            "negative_funding": [],
                        },
                        {top.symbol: top_raw, runner.symbol: runner_raw},
                        checked,
                    )

                self.assertEqual(
                    [item.candidate.symbol for item in result.passed_signals],
                    [top.symbol],
                )
                self.assertEqual(
                    [
                        item.signal.candidate.symbol
                        for item in result.live_candidates
                    ],
                    [top.symbol],
                )

    def test_c_confirmed_same_entry_candle_reuses_frozen_breadth_and_gate(self):
        items, raws, _ = full_batch()
        target = items[0].symbol
        s_time = str(raws[target][118][0])
        waiting = deepcopy(raws)
        waiting[target][121][4] = "99.95"

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)
            for current_index in range(113, 122):
                batch = {
                    symbol: rows[: current_index + 1]
                    for symbol, rows in waiting.items()
                }
                self._evaluate_published(
                    recorder,
                    scheduler,
                    {"quote_volume_top": items, "negative_funding": []},
                    batch,
                    batch[target][-1][0] + 30_000,
                )
            frozen = recorder.get_validated_n14_active_episode(
                "N14", target, s_time
            )
            self.assertIsNotNone(frozen)
            self.assertEqual(frozen["stage"], "C_CONFIRMED")
            self.assertEqual(frozen["evidence"]["bullish_breadth_c"], "0.4")
            self.assertEqual(frozen["evidence"]["cascade_gate"], "BREADTH_MIN")

            changed = deepcopy(waiting)
            changed[items[1].symbol][120][4] = "99.95"
            result = self._evaluate_published(
                recorder,
                scheduler,
                {"quote_volume_top": items, "negative_funding": []},
                changed,
                changed[target][-1][0] + 30_000,
            )
            target_signal = next(
                signal
                for signal in result.signals
                if signal.candidate.symbol == target
            )
            self.assertEqual(target_signal.reason, "N14_WAITING_ENTRY_PRICE")
            self.assertEqual(
                recorder.get_validated_n14_active_episode(
                    "N14", target, s_time
                ),
                frozen,
            )

            # Drop the live recomputation below both cascade gates and move E
            # into the entry band.  The already confirmed C must still use its
            # frozen 0.40/BREADTH_MIN evidence and remain the executable winner.
            deeply_changed = deepcopy(waiting)
            for item in items[1:32]:
                deeply_changed[item.symbol][120][4] = "99.95"
            deeply_changed[target][121][4] = "99.97"
            entered = self._evaluate_published(
                recorder,
                scheduler,
                {"quote_volume_top": items, "negative_funding": []},
                deeply_changed,
                deeply_changed[target][-1][0] + 30_000,
            )
            self.assertEqual(
                [item.candidate.symbol for item in entered.passed_signals],
                [target],
            )
            terminal = recorder.get_n14_sell_impact_state(
                "N14", target, s_time
            )
            self.assertIsNotNone(terminal)
            terminal_detail = json.loads(terminal[7])
            self.assertEqual(
                terminal_detail["evidence"]["bullish_breadth_c"], "0.4"
            )
            self.assertEqual(
                terminal_detail["evidence"]["cascade_gate"], "BREADTH_MIN"
            )

    def test_consumed_raw_best_does_not_block_fresh_runner(self):
        raw, checked = n14_klines()
        top_raw = deepcopy(raw)
        top_raw[120][10] = "48"
        runner_raw = deepcopy(raw)
        runner_raw[120][10] = "46"
        top_analysis = analyze(
            top_raw,
            checked,
            market_scenario=replace(
                scenario(top_raw), quote_volume_rank=1
            ),
        )
        runner_analysis = analyze(
            runner_raw,
            checked,
            market_scenario=replace(
                scenario(runner_raw), quote_volume_rank=2
            ),
        )
        top = candidate("CONSUMEDUSDT", 1)
        runner = candidate("FRESHUSDT", 2)

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder, scheduler = recorder_scheduler(tmpdir)

            def evaluate_candidate(strategy, item, *args, **kwargs):
                if item.symbol == top.symbol:
                    return StrategySignalDecision(
                        strategy,
                        item,
                        top_analysis,
                        False,
                        "REJECTED",
                        "N14_EPISODE_CONSUMED",
                    )
                return StrategySignalDecision(
                    strategy,
                    item,
                    runner_analysis,
                    True,
                    "PASSED",
                    "PASSED",
                )

            with patch.object(
                scheduler,
                "_build_n14_batch_context",
                return_value=N14BatchContext({}, checked, True),
            ), patch.object(
                scheduler,
                "_probe_n14_candidate",
                side_effect=(top_analysis, runner_analysis),
            ), patch.object(
                scheduler,
                "_evaluate_candidate",
                side_effect=evaluate_candidate,
            ):
                result = self._evaluate_published(
                    recorder,
                    scheduler,
                    {
                        "quote_volume_top": [top, runner],
                        "negative_funding": [],
                    },
                    {top.symbol: top_raw, runner.symbol: runner_raw},
                    checked,
                )

            self.assertEqual(
                [item.candidate.symbol for item in result.passed_signals],
                [runner.symbol],
            )


if __name__ == "__main__":
    unittest.main()
