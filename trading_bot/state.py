from __future__ import annotations

import errno
import json
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .precision import decimal_to_api


_STATE_LOCKS_GUARD = threading.Lock()
_STATE_LOCKS: dict[str, threading.RLock] = {}


def _state_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _STATE_LOCKS_GUARD:
        return _STATE_LOCKS.setdefault(key, threading.RLock())


@dataclass(frozen=True)
class PositionState:
    symbol: str
    quantity: str
    entry_price: str
    stop_loss_price: str
    take_profit_price: str
    leverage: int
    opened_at: str
    dry_run: bool
    orders: dict[str, Any]


def _strict_state_value_equal(left: Any, right: Any) -> bool:
    """Compare persisted state without bool/int or int/float coercion."""

    if type(left) is not type(right):
        return False
    if type(left) is PositionState:
        return all(
            _strict_state_value_equal(
                getattr(left, field_name),
                getattr(right, field_name),
            )
            for field_name in (
                "symbol",
                "quantity",
                "entry_price",
                "stop_loss_price",
                "take_profit_price",
                "leverage",
                "opened_at",
                "dry_run",
                "orders",
            )
        )
    if type(left) is dict:
        if (
            len(left) != len(right)
            or any(type(key) is not str for key in left)
            or any(type(key) is not str for key in right)
            or left.keys() != right.keys()
        ):
            return False
        return all(
            _strict_state_value_equal(left[key], right[key])
            for key in left
        )
    if type(left) in (list, tuple):
        return len(left) == len(right) and all(
            _strict_state_value_equal(left_item, right_item)
            for left_item, right_item in zip(left, right)
        )
    return left == right


class StateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = _state_lock(self.path)

    @property
    def _clear_tombstone(self) -> Path:
        return self.path.with_name(f".{self.path.name}.clear-tombstone")

    def _recover_clear_tombstone_locked(self) -> None:
        tombstone = self._clear_tombstone
        path_exists = self.path.exists()
        tombstone_exists = tombstone.exists()
        if path_exists and tombstone_exists:
            raise OSError(
                f"ambiguous state evidence: both {self.path.name} and "
                f"{tombstone.name} exist"
            )
        if not path_exists and tombstone_exists:
            os.replace(tombstone, self.path)
            self._fsync_parent_directory()

    def load(self) -> PositionState | None:
        with self._lock:
            self._recover_clear_tombstone_locked()
            if not self.path.exists():
                return None
            with self.path.open("r", encoding="utf-8") as file:
                data = json.load(file)
            return PositionState(**data)

    def save(self, state: PositionState) -> None:
        with self._lock:
            self._recover_clear_tombstone_locked()
            self._save_locked(state)

    def _save_locked(self, state: PositionState) -> None:
        file_descriptor, temporary_name = tempfile.mkstemp(
            dir=str(self.path.parent),
            prefix=f".{self.path.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        descriptor_open = True
        try:
            with os.fdopen(
                file_descriptor, "w", encoding="utf-8"
            ) as file:
                descriptor_open = False
                json.dump(
                    asdict(state),
                    file,
                    ensure_ascii=False,
                    indent=2,
                )
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, self.path)
            self._fsync_parent_directory()
        finally:
            if descriptor_open:
                os.close(file_descriptor)
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass

    def clear(self) -> None:
        with self._lock:
            self._recover_clear_tombstone_locked()
            if self.path.exists():
                self.path.unlink()
                self._fsync_parent_directory()

    def compare_and_save(
        self,
        expected: PositionState,
        replacement: PositionState,
    ) -> bool:
        """Replace one exact state snapshot without overwriting a newer one."""

        with self._lock:
            self._recover_clear_tombstone_locked()
            current = self.load()
            if not _strict_state_value_equal(current, expected):
                return False
            self._save_locked(replacement)
            return True

    def compare_and_clear(self, expected: PositionState) -> bool:
        with self._lock:
            self._recover_clear_tombstone_locked()
            current = self.load()
            if not _strict_state_value_equal(current, expected):
                return False
            tombstone = self._clear_tombstone
            serialized = json.dumps(
                asdict(expected), ensure_ascii=False, indent=2
            )
            moved = False
            try:
                os.replace(self.path, tombstone)
                moved = True
                self._fsync_parent_directory()
                tombstone.unlink()
                self._fsync_parent_directory()
            except Exception as primary_exc:
                recovery_exc = None
                if moved and not self.path.exists():
                    try:
                        if tombstone.exists():
                            os.replace(tombstone, self.path)
                        else:
                            with tombstone.open("w", encoding="utf-8") as file:
                                file.write(serialized)
                                file.flush()
                                os.fsync(file.fileno())
                            os.replace(tombstone, self.path)
                        self._fsync_parent_directory()
                    except Exception as exc:
                        recovery_exc = exc
                if recovery_exc is not None:
                    raise OSError(
                        f"state clear failed and recovery was incomplete: "
                        f"{recovery_exc}"
                    ) from primary_exc
                raise
            return True

    def _fsync_parent_directory(self) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        try:
            directory_descriptor = os.open(str(self.path.parent), flags)
        except OSError as exc:
            if exc.errno in {errno.EINVAL, errno.ENOTSUP, errno.EBADF}:
                return
            raise
        try:
            try:
                os.fsync(directory_descriptor)
            except OSError as exc:
                if exc.errno not in {
                    errno.EINVAL,
                    errno.ENOTSUP,
                    errno.EBADF,
                }:
                    raise
        finally:
            os.close(directory_descriptor)


@dataclass(frozen=True)
class DryRunAccountState:
    starting_balance: str
    balance: str
    updated_at: str
    last_settlement_trade_id: int | None = None
    last_settlement_balance: str | None = None


class DryRunAccountStore:
    def __init__(self, path: str, starting_balance: Decimal):
        if (
            type(starting_balance) is not Decimal
            or not starting_balance.is_finite()
            or starting_balance < 0
        ):
            raise ValueError("dry-run starting balance is invalid")
        self.path = Path(path)
        self.starting_balance = starting_balance
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = _state_lock(self.path)

    def load(self) -> DryRunAccountState | None:
        if not self.path.exists():
            return None
        def reject_duplicate_keys(pairs):
            result = {}
            for key, value in pairs:
                if type(key) is not str or key in result:
                    raise ValueError("dry-run account JSON contains duplicate keys")
                result[key] = value
            return result

        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(
                file,
                object_pairs_hook=reject_duplicate_keys,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError("dry-run account contains non-finite JSON: %s" % value)
                ),
            )
        required = {"starting_balance", "balance", "updated_at"}
        allowed = required | {
            "last_settlement_trade_id",
            "last_settlement_balance",
        }
        if type(data) is not dict or set(data) - allowed or not required.issubset(data):
            raise ValueError("dry-run account shape is invalid")
        if any(type(data[key]) is not str for key in required):
            raise ValueError("dry-run account fields are not built-in strings")

        legacy_shape = (
            "last_settlement_trade_id" not in data
            and "last_settlement_balance" not in data
        )

        def canonical_decimal(
            raw_value: str,
            *,
            nonnegative: bool,
            require_canonical: bool,
        ) -> Decimal:
            if not 1 <= len(raw_value) <= 128:
                raise ValueError("dry-run account decimal is invalid")
            try:
                value = Decimal(raw_value)
            except Exception as exc:
                raise ValueError("dry-run account decimal is invalid") from exc
            if (
                not value.is_finite()
                or (value == 0 and value.is_signed())
                or (nonnegative and value < 0)
                or (
                    require_canonical
                    and decimal_to_api(value) != raw_value
                )
            ):
                raise ValueError("dry-run account decimal is not canonical")
            return value

        starting_balance = canonical_decimal(
            data["starting_balance"],
            nonnegative=True,
            require_canonical=not legacy_shape,
        )
        balance = canonical_decimal(
            data["balance"],
            nonnegative=True,
            require_canonical=not legacy_shape,
        )
        if starting_balance != self.starting_balance:
            raise ValueError("dry-run account starting balance conflicts with config")
        try:
            updated_at = datetime.fromisoformat(data["updated_at"])
        except (TypeError, ValueError) as exc:
            raise ValueError("dry-run account timestamp is invalid") from exc
        if (
            updated_at.tzinfo is None
            or updated_at.utcoffset() != timedelta(0)
            or updated_at.astimezone(timezone.utc).isoformat()
            != data["updated_at"]
        ):
            raise ValueError("dry-run account timestamp is not canonical UTC")
        settlement_id = data.get("last_settlement_trade_id")
        settlement_balance = data.get("last_settlement_balance")
        if (settlement_id is None) != (settlement_balance is None):
            raise ValueError("dry-run account settlement fields are incomplete")
        if settlement_id is not None:
            if type(settlement_id) is not int or settlement_id <= 0:
                raise ValueError("dry-run account settlement id is invalid")
            if type(settlement_balance) is not str:
                raise ValueError("dry-run account settlement balance is invalid")
            canonical_decimal(
                settlement_balance,
                nonnegative=True,
                require_canonical=True,
            )
        return DryRunAccountState(
            starting_balance=decimal_to_api(starting_balance),
            balance=decimal_to_api(balance),
            updated_at=data["updated_at"],
            last_settlement_trade_id=settlement_id,
            last_settlement_balance=settlement_balance,
        )

    def balance(self) -> Decimal:
        with self._lock:
            state = self.load()
            if state is None:
                self.set_balance(self.starting_balance)
                return self.starting_balance
            return Decimal(state.balance)

    def preview_balance(self) -> Decimal:
        """Read the effective balance without creating or changing the file."""

        with self._lock:
            state = self.load()
            return self.starting_balance if state is None else Decimal(state.balance)

    def _write_state(self, state: DryRunAccountState) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=str(self.path.parent),
            prefix=f".{self.path.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        descriptor_open = True
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                descriptor_open = False
                json.dump(asdict(state), file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if descriptor_open:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def set_balance(self, balance: Decimal) -> None:
        if type(balance) is not Decimal or not balance.is_finite() or balance < 0:
            raise ValueError("dry-run balance must be a finite nonnegative Decimal")
        with self._lock:
            existing = self.load()
            canonical_balance = decimal_to_api(balance)
            state = DryRunAccountState(
                starting_balance=decimal_to_api(self.starting_balance),
                balance=canonical_balance,
                updated_at=datetime.now(timezone.utc).isoformat(),
                last_settlement_trade_id=(
                    existing.last_settlement_trade_id
                    if existing is not None
                    else None
                ),
                last_settlement_balance=(
                    existing.last_settlement_balance
                    if existing is not None
                    else None
                ),
            )
            self._write_state(state)

    def apply_pnl(self, pnl_amount: Decimal) -> Decimal:
        if type(pnl_amount) is not Decimal or not pnl_amount.is_finite():
            raise ValueError("dry-run PnL must be a finite Decimal")
        with self._lock:
            balance_after = self.preview_balance() + pnl_amount
            if not balance_after.is_finite() or balance_after < 0:
                raise ValueError("dry-run balance after PnL is invalid")
            self.set_balance(balance_after)
            return balance_after

    def settle_balance_once(
        self,
        trade_review_id: int,
        expected_balance_before: Decimal,
        balance_after: Decimal,
    ) -> Decimal:
        """Atomically apply one durable Review settlement exactly once."""

        if type(trade_review_id) is not int or trade_review_id <= 0:
            raise ValueError("dry-run settlement trade id is invalid")
        if (
            type(expected_balance_before) is not Decimal
            or type(balance_after) is not Decimal
            or not expected_balance_before.is_finite()
            or expected_balance_before < 0
            or not balance_after.is_finite()
            or balance_after < 0
        ):
            raise ValueError("dry-run settlement balance is invalid")
        with self._lock:
            existing = self.load()
            if existing is not None:
                last_id = existing.last_settlement_trade_id
                last_balance = existing.last_settlement_balance
                if last_id == trade_review_id:
                    if (
                        type(last_id) is not int
                        or type(last_balance) is not str
                        or Decimal(last_balance) != balance_after
                        or Decimal(existing.balance) != balance_after
                    ):
                        raise OSError("dry-run settlement replay conflicts")
                    return balance_after
                if type(last_id) is int and last_id > trade_review_id:
                    raise OSError("dry-run settlement would roll back its high-water")
                if last_id is not None and type(last_id) is not int:
                    raise OSError("dry-run settlement identity is malformed")
            current_balance = (
                self.starting_balance
                if existing is None
                else Decimal(existing.balance)
            )
            if current_balance != expected_balance_before:
                raise OSError("dry-run settlement starting balance changed")
            state = DryRunAccountState(
                starting_balance=decimal_to_api(self.starting_balance),
                balance=decimal_to_api(balance_after),
                updated_at=datetime.now(timezone.utc).isoformat(),
                last_settlement_trade_id=trade_review_id,
                last_settlement_balance=decimal_to_api(balance_after),
            )
            self._write_state(state)
            return balance_after
