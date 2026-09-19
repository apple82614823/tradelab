from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from trading_bot.state import PositionState, StateStore


def position(symbol: str) -> PositionState:
    return PositionState(
        symbol=symbol,
        quantity="1",
        entry_price="100",
        stop_loss_price="99",
        take_profit_price="105",
        leverage=10,
        opened_at="2026-07-12T00:00:00+00:00",
        dry_run=False,
        orders={"strategy": {"strategy_id": "N14"}},
    )


class StateStoreAtomicWriteTests(unittest.TestCase):
    def test_fresh_store_recovers_clear_tombstone_after_process_interruption(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            tombstone = state_path.with_name(
                f".{state_path.name}.clear-tombstone"
            )
            original = position("N15USDT")
            StateStore(str(state_path)).save(original)
            os.replace(state_path, tombstone)

            fresh = StateStore(str(state_path))
            self.assertEqual(fresh.load(), original)
            self.assertTrue(state_path.exists())
            self.assertFalse(tombstone.exists())
            self.assertTrue(fresh.compare_and_clear(original))
            self.assertIsNone(StateStore(str(state_path)).load())

    def test_failed_compare_rollback_leaves_tombstone_recoverable_by_fresh_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            tombstone = state_path.with_name(
                f".{state_path.name}.clear-tombstone"
            )
            store = StateStore(str(state_path))
            original = position("N15USDT")
            store.save(original)
            real_replace = os.replace
            replace_calls = []

            def fail_rollback(source, destination):
                replace_calls.append((Path(source), Path(destination)))
                if len(replace_calls) == 1:
                    return real_replace(source, destination)
                raise OSError("forced rollback replace failure")

            with patch(
                "trading_bot.state.os.replace", side_effect=fail_rollback
            ), patch.object(
                store,
                "_fsync_parent_directory",
                side_effect=OSError("forced clear fsync failure"),
            ):
                with self.assertRaisesRegex(
                    OSError, "recovery was incomplete"
                ):
                    store.compare_and_clear(original)

            self.assertFalse(state_path.exists())
            self.assertTrue(tombstone.exists())
            self.assertEqual(StateStore(str(state_path)).load(), original)

    def test_path_and_tombstone_ambiguity_blocks_load_and_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            store = StateStore(str(state_path))
            original = position("N15USDT")
            store.save(original)
            tombstone = state_path.with_name(
                f".{state_path.name}.clear-tombstone"
            )
            tombstone.write_text(state_path.read_text(), encoding="utf-8")
            with self.assertRaisesRegex(OSError, "ambiguous state evidence"):
                store.load()
            with self.assertRaisesRegex(OSError, "ambiguous state evidence"):
                store.save(position("NEWUSDT"))

    def test_interrupted_temp_write_never_replaces_valid_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            store = StateStore(str(state_path))
            original = position("ORIGINALUSDT")
            store.save(original)

            def partial_dump(value, file, **kwargs):
                file.write('{"symbol":')
                file.flush()
                raise RuntimeError("simulated write interruption")

            with patch(
                "trading_bot.state.json.dump", side_effect=partial_dump
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "simulated write interruption"
                ):
                    store.save(position("NEWUSDT"))

            self.assertEqual(store.load(), original)
            self.assertEqual(
                list(state_path.parent.glob(f".{state_path.name}.*.tmp")),
                [],
            )

    def test_replace_failure_preserves_old_json_and_removes_temp_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            store = StateStore(str(state_path))
            original = position("ORIGINALUSDT")
            store.save(original)

            with patch(
                "trading_bot.state.os.replace",
                side_effect=OSError("simulated replace interruption"),
            ):
                with self.assertRaisesRegex(
                    OSError, "simulated replace interruption"
                ):
                    store.save(position("NEWUSDT"))

            self.assertEqual(store.load(), original)
            self.assertEqual(
                list(state_path.parent.glob(f".{state_path.name}.*.tmp")),
                [],
            )

    def test_complete_same_directory_json_is_atomically_replaced(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            store = StateStore(str(state_path))
            store.save(position("ORIGINALUSDT"))
            replacement = position("NEWUSDT")
            real_replace = os.replace
            observed = []

            def inspect_then_replace(source, destination):
                source_path = Path(source)
                destination_path = Path(destination)
                self.assertEqual(source_path.parent, state_path.parent)
                self.assertEqual(destination_path, state_path)
                with source_path.open("r", encoding="utf-8") as file:
                    observed.append(json.load(file))
                real_replace(source, destination)

            with patch(
                "trading_bot.state.os.replace",
                side_effect=inspect_then_replace,
            ):
                store.save(replacement)

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0]["symbol"], "NEWUSDT")
            self.assertEqual(store.load(), replacement)

    def test_compare_and_clear_rejects_numeric_type_coercion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "position.json"
            store = StateStore(str(state_path))
            expected = replace(
                position("N16USDT"),
                orders={
                    "strategy": {
                        "strategy_id": "N16",
                        "signal_id": 1,
                        "structure_id": "a" * 24,
                    }
                },
            )
            for value in (True, 1.0):
                with self.subTest(value_type=type(value).__name__):
                    replacement_orders = {
                        "strategy": {
                            **expected.orders["strategy"],
                            "signal_id": value,
                        }
                    }
                    store.save(replace(expected, orders=replacement_orders))
                    before = state_path.read_bytes()
                    self.assertFalse(store.compare_and_clear(expected))
                    self.assertTrue(state_path.exists())
                    self.assertEqual(state_path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
