"""Identity-attested, stopped-service release backup for Binance assets.

This module is deliberately narrower than the normal application runtime.  It
does not load ``.env``, start or stop services, or infer paths.  The release
operator supplies the already-redacted effective paths after proving that the
Binance unit is stopped.  The helper then acquires the existing instance lock,
re-attests every persistence path inside that lock, and creates one private
generation backup.

An absent pre-N16 claim ledger is an authenticated state, not a SQLite file to
open.  In that branch the helper never passes the ledger path to SQLite or
``lsof`` and records only an absence marker after proving continuous absence.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sqlite3
import stat
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Sequence, Tuple, Union

from .instance_lock import (
    InstanceLock,
    InstanceLockError,
    validate_official_instance_lock,
)
from .n16_claim_ledger import N16ClaimLedgerError, N16ClaimLedgerFileScope
from .signal_retention import (
    SignalRetentionMaintenanceError,
    _DatabaseFileScope,
    _attested_maintenance_connection,
    _capture_database_sidecars,
    _directory_identity,
    _is_within,
    _regular_file_identity,
    _validate_database_sidecars,
    _verify_sqlite_main_identity,
)


_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_ABSENCE_MARKER = "ABSENT_PRE_N16\n"


class ReleaseBackupError(RuntimeError):
    """Raised when the stopped-service release backup is not provable."""


@dataclass(frozen=True)
class _EffectiveFileProof:
    name: str
    path: Path
    parent_identity: Tuple[int, int]
    main_identity: Optional[Tuple[int, int]]
    sidecar_identities: Tuple[Optional[Tuple[int, int]], ...]


@dataclass(frozen=True)
class _ReleaseProof:
    app_root: Path
    backup_root: Path
    app_root_identity: Tuple[int, int]
    backup_root_identity: Tuple[int, int]
    current_dir: Path
    current_dir_identity: Tuple[int, int]
    env_file: Path
    env_identity: Tuple[int, int]
    state_dir: Path
    state_dir_identity: Tuple[int, int]
    files: Tuple[_EffectiveFileProof, ...]

    def by_name(self) -> Dict[str, _EffectiveFileProof]:
        return {proof.name: proof for proof in self.files}


def _absolute_path(
    value: Union[os.PathLike[str], str], name: str
) -> Path:
    path = Path(os.path.abspath(os.fspath(value)))
    if not path.is_absolute():
        raise ReleaseBackupError("%s must be absolute" % name)
    return path


def _assert_below(root: Path, path: Path, name: str) -> None:
    try:
        resolved_root = root.resolve(strict=True)
        resolved = path.resolve(strict=False)
    except OSError as exc:
        raise ReleaseBackupError("%s cannot be resolved safely" % name) from exc
    if not _is_within(resolved_root, resolved):
        raise ReleaseBackupError("%s escaped its dedicated Binance root" % name)


def _assert_real_parent_chain(root: Path, parent: Path, name: str) -> None:
    _assert_below(root, parent, name)
    relative = parent.resolve(strict=False).relative_to(root.resolve(strict=True))
    current = root
    _directory_identity(current, "%s root" % name)
    for component in relative.parts:
        current = current / component
        _directory_identity(current, "%s parent" % name)


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved paths are equal or contain one another."""

    return (
        left == right
        or _is_within(left, right)
        or _is_within(right, left)
    )


def _assert_backup_tree_is_disjoint(
    backup_root: Path,
    runtime_paths: Sequence[Tuple[str, Path]],
) -> None:
    """Reject a backup tree that intersects any live Binance path.

    This proof deliberately runs during both pre-lock and post-lock scope
    capture, before SQLite is opened and before a ``release-*`` directory can
    be created.  ``Path.relative_to`` (through ``_is_within``) compares path
    components rather than textual prefixes, while resolving each candidate
    also closes symlink/``..`` aliases.
    """

    resolved_backup = backup_root.resolve(strict=True)
    backup_identity = _directory_identity(
        resolved_backup, "resolved Binance backup root"
    )
    for name, runtime_path in runtime_paths:
        try:
            resolved_runtime = runtime_path.resolve(strict=False)
        except OSError as exc:
            raise ReleaseBackupError(
                "%s cannot be resolved for backup isolation" % name
            ) from exc
        if _paths_overlap(resolved_backup, resolved_runtime):
            raise ReleaseBackupError(
                "Binance backup root overlaps %s" % name
            )
        try:
            runtime_details = os.lstat(str(resolved_runtime))
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ReleaseBackupError(
                "%s identity cannot be authenticated" % name
            ) from exc
        if stat.S_ISDIR(runtime_details.st_mode) and (
            int(runtime_details.st_dev),
            int(runtime_details.st_ino),
        ) == backup_identity:
            raise ReleaseBackupError(
                "Binance backup root aliases %s" % name
            )


def _capture_effective_file(
    name: str,
    path: Path,
    *,
    app_root: Path,
    database: bool,
    required: bool,
) -> _EffectiveFileProof:
    _assert_real_parent_chain(app_root, path.parent, name)
    if os.path.lexists(str(path)):
        identity = _regular_file_identity(path, name)
        sidecars = (
            _capture_database_sidecars(path, name)
            if database
            else (None, None, None)
        )
    else:
        if required:
            raise ReleaseBackupError("%s is missing" % name)
        identity = None
        if database:
            _validate_database_sidecars(path, name, require_absent=True)
        sidecars = (None, None, None)
    return _EffectiveFileProof(
        name=name,
        path=path,
        parent_identity=_directory_identity(path.parent, "%s parent" % name),
        main_identity=identity,
        sidecar_identities=sidecars,
    )


def _capture_release_proof(
    *,
    app_root: Path,
    backup_root: Path,
    current_dir: Path,
    env_file: Path,
    state_dir: Path,
    state_file: Path,
    dry_run_account_file: Path,
    log_file: Path,
    review_db: Path,
    lock_file: Path,
    claim_ledger: Path,
) -> _ReleaseProof:
    app_identity = _directory_identity(app_root, "Binance application root")
    backup_identity = _directory_identity(backup_root, "Binance backup root")
    app_root = app_root.resolve(strict=True)
    backup_root = backup_root.resolve(strict=True)
    shared = app_root / "shared"
    runtime_paths = [
        ("Binance application root", app_root),
        ("current code directory", current_dir),
        ("shared runtime tree", shared),
        ("state directory", state_dir),
        ("Review database parent", review_db.parent),
        ("log parent", log_file.parent),
    ]
    for name, path in (
        ("STATE_FILE", state_file),
        ("DRY_RUN_ACCOUNT_FILE", dry_run_account_file),
        ("LOG_FILE", log_file),
        ("REVIEW_DB_FILE", review_db),
        ("INSTANCE_LOCK_FILE", lock_file),
        ("N16_CLAIM_LEDGER", claim_ledger),
    ):
        runtime_paths.append(("%s parent" % name, path.parent))
        runtime_paths.append((name, path))
    _assert_backup_tree_is_disjoint(backup_root, runtime_paths)
    if app_identity == backup_identity:
        raise ReleaseBackupError("application and backup roots must differ")
    expected_directories = {
        current_dir.resolve(strict=True): app_root / "current",
        state_dir.resolve(strict=True): shared / "state",
    }
    if any(actual != expected for actual, expected in expected_directories.items()):
        raise ReleaseBackupError("formal release directories are not exact")
    if env_file.resolve(strict=True) != shared / ".env":
        raise ReleaseBackupError("formal environment file is not exact")
    if stat.S_IMODE(os.lstat(str(env_file)).st_mode) != 0o600:
        raise ReleaseBackupError("environment file must have mode 0600")
    expected_parents = {
        state_file: shared / "state",
        dry_run_account_file: shared / "state",
        log_file: shared / "logs",
        review_db: shared / "data",
        lock_file: shared / "state",
        claim_ledger: shared / "state",
    }
    if any(
        path.resolve(strict=False).parent != expected
        for path, expected in expected_parents.items()
    ) or claim_ledger.resolve(strict=False) != (
        shared / "state" / "n16_claim_ledger.sqlite3"
    ):
        raise ReleaseBackupError("effective persistence path is outside its exact parent")
    _assert_real_parent_chain(app_root, current_dir, "current code directory")
    _assert_real_parent_chain(app_root, state_dir, "state directory")
    current_identity = _directory_identity(current_dir, "current code directory")
    state_identity = _directory_identity(state_dir, "state directory")
    _assert_real_parent_chain(app_root, env_file.parent, "environment file")
    env_identity = _regular_file_identity(env_file, "environment file")

    definitions = (
        ("STATE_FILE", state_file, False, False),
        ("DRY_RUN_ACCOUNT_FILE", dry_run_account_file, False, False),
        ("LOG_FILE", log_file, False, False),
        ("REVIEW_DB_FILE", review_db, True, True),
        ("INSTANCE_LOCK_FILE", lock_file, False, True),
        ("N16_CLAIM_LEDGER", claim_ledger, True, False),
    )
    proofs = tuple(
        _capture_effective_file(
            name,
            path,
            app_root=app_root,
            database=database,
            required=required,
        )
        for name, path, database, required in definitions
    )
    visible_paths = [proof.path for proof in proofs]
    if len(set(visible_paths)) != len(visible_paths):
        raise ReleaseBackupError("effective persistence paths must differ")
    identities = [
        proof.main_identity
        for proof in proofs
        if proof.main_identity is not None
    ]
    if len(set(identities)) != len(identities):
        raise ReleaseBackupError("effective persistence inodes must differ")
    return _ReleaseProof(
        app_root=app_root,
        backup_root=backup_root,
        app_root_identity=app_identity,
        backup_root_identity=backup_identity,
        current_dir=current_dir,
        current_dir_identity=current_identity,
        env_file=env_file,
        env_identity=env_identity,
        state_dir=state_dir,
        state_dir_identity=state_identity,
        files=proofs,
    )


def _assert_same_generation(before: _ReleaseProof, after: _ReleaseProof) -> None:
    if before != after:
        raise ReleaseBackupError(
            "effective persistence scope changed before the instance lock"
        )


def _assert_no_openers(paths: Sequence[Path]) -> None:
    command = ["lsof", "-t", "--"] + [str(path) for path in paths]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise ReleaseBackupError("lsof is required for the stopped-service gate") from exc
    if result.returncode == 0:
        raise ReleaseBackupError("an attested SQLite asset still has an opener")
    if result.returncode != 1:
        raise ReleaseBackupError("SQLite opener verification failed")


@contextmanager
def _open_attested_rw(
    path: Path,
    scope: _DatabaseFileScope,
) -> Iterator[sqlite3.Connection]:
    scope.validate_before_open(path)
    opener = lambda: sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)
    try:
        with _attested_maintenance_connection(
            opener, scope.main_identity
        ) as connection:
            _verify_sqlite_main_identity(connection, path, scope.main_identity)
            yield connection
    finally:
        scope.refresh_after_close(path)


def _checkpoint_and_validate(connection: sqlite3.Connection, name: str) -> None:
    mode_row = connection.execute("PRAGMA journal_mode").fetchone()
    if (
        type(mode_row) not in (tuple, list)
        or len(mode_row) != 1
        or type(mode_row[0]) is not str
    ):
        raise ReleaseBackupError("%s journal mode is invalid" % name)
    checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if (
        type(checkpoint) not in (tuple, list)
        or len(checkpoint) != 3
        or type(checkpoint[0]) is not int
        or checkpoint[0] != 0
    ):
        raise ReleaseBackupError("%s WAL checkpoint did not complete" % name)
    delete_row = connection.execute("PRAGMA journal_mode=DELETE").fetchone()
    if (
        type(delete_row) not in (tuple, list)
        or len(delete_row) != 1
        or str(delete_row[0]).lower() != "delete"
    ):
        raise ReleaseBackupError("%s did not enter DELETE journal mode" % name)
    integrity = connection.execute("PRAGMA integrity_check").fetchall()
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if integrity != [("ok",)] or foreign_keys:
        raise ReleaseBackupError("%s SQLite integrity validation failed" % name)


def _read_schema_version(
    connection: sqlite3.Connection,
    name: str,
) -> int:
    row = connection.execute("PRAGMA schema_version").fetchone()
    if (
        type(row) not in (tuple, list)
        or len(row) != 1
        or type(row[0]) is not int
        or row[0] <= 0
        or row[0] > 2_147_483_647
    ):
        raise ReleaseBackupError(
            "%s schema version is invalid" % name
        )
    return row[0]


def _write_backup_schema_version(
    descriptor: int,
    schema_version: int,
    name: str,
) -> None:
    if (
        type(schema_version) is not int
        or schema_version <= 0
        or schema_version > 2_147_483_647
    ):
        raise ReleaseBackupError(
            "%s source schema version is invalid" % name
        )
    try:
        header = os.pread(descriptor, 100, 0)
    except OSError as exc:
        raise ReleaseBackupError(
            "%s backup schema header read failed" % name
        ) from exc
    if (
        len(header) != 100
        or header[:16] != b"SQLite format 3\x00"
        or int.from_bytes(header[40:44], "big") <= 0
    ):
        raise ReleaseBackupError(
            "%s backup schema header is invalid" % name
        )
    try:
        written = os.pwrite(
            descriptor,
            schema_version.to_bytes(4, "big"),
            40,
        )
    except OSError as exc:
        raise ReleaseBackupError(
            "%s backup schema version restore failed" % name
        ) from exc
    if written != 4:
        raise ReleaseBackupError(
            "%s backup schema version restore was incomplete" % name
        )


def _commit_backup_schema_version(
    descriptor: int,
    name: str,
) -> None:
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise ReleaseBackupError(
            "%s backup schema version commit failed" % name
        ) from exc


def _verify_backup_schema_version(
    connection: sqlite3.Connection,
    expected: int,
    name: str,
) -> None:
    if _read_schema_version(connection, "%s backup" % name) != expected:
        raise ReleaseBackupError(
            "%s backup schema version conflicts" % name
        )


def _write_new_file(directory_fd: int, name: str, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_regular_file(
    source: Path,
    directory_fd: int,
    target_name: str,
    *,
    expected_identity: Optional[Tuple[int, int]] = None,
) -> None:
    current = _regular_file_identity(source, "backup source %s" % source.name)
    expected = current if expected_identity is None else expected_identity
    if current != expected:
        raise ReleaseBackupError("backup source identity changed")
    source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    source_fd = os.open(str(source), source_flags)
    try:
        details = os.fstat(source_fd)
        if (
            not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
            or (int(details.st_dev), int(details.st_ino)) != expected
        ):
            raise ReleaseBackupError("backup source identity changed")
        target_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        target_flags |= getattr(os, "O_CLOEXEC", 0)
        target_flags |= getattr(os, "O_NOFOLLOW", 0)
        target_fd = os.open(target_name, target_flags, 0o600, dir_fd=directory_fd)
        try:
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                offset = 0
                while offset < len(chunk):
                    offset += os.write(target_fd, chunk[offset:])
            os.fsync(target_fd)
        finally:
            os.close(target_fd)
    finally:
        os.close(source_fd)


def _copy_tree_from_descriptors(
    source_fd: int,
    target_fd: int,
    *,
    excluded_names: frozenset[str],
    expected_files: Dict[str, Tuple[int, int]],
) -> None:
    for name in sorted(os.listdir(source_fd)):
        if name in excluded_names:
            continue
        details = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
        if stat.S_ISDIR(details.st_mode):
            os.mkdir(name, 0o700, dir_fd=target_fd)
            child_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            child_flags |= getattr(os, "O_CLOEXEC", 0)
            child_flags |= getattr(os, "O_NOFOLLOW", 0)
            child_source = os.open(name, child_flags, dir_fd=source_fd)
            child_target = os.open(name, child_flags, dir_fd=target_fd)
            try:
                _copy_tree_from_descriptors(
                    child_source,
                    child_target,
                    excluded_names=frozenset(),
                    expected_files={},
                )
            finally:
                os.close(child_target)
                os.close(child_source)
            continue
        if not stat.S_ISREG(details.st_mode) or int(details.st_nlink) != 1:
            raise ReleaseBackupError("state backup contains an unsafe entry")
        if name in expected_files and (
            int(details.st_dev), int(details.st_ino)
        ) != expected_files[name]:
            raise ReleaseBackupError("effective state file identity changed")
        source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        source_flags |= getattr(os, "O_NOFOLLOW", 0)
        source_child = os.open(name, source_flags, dir_fd=source_fd)
        target_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        target_flags |= getattr(os, "O_CLOEXEC", 0)
        target_flags |= getattr(os, "O_NOFOLLOW", 0)
        target_child = os.open(name, target_flags, 0o600, dir_fd=target_fd)
        try:
            opened = os.fstat(source_child)
            if (
                not stat.S_ISREG(opened.st_mode)
                or int(opened.st_nlink) != 1
                or (int(opened.st_dev), int(opened.st_ino))
                != (int(details.st_dev), int(details.st_ino))
            ):
                raise ReleaseBackupError("state source identity changed")
            while True:
                chunk = os.read(source_child, 1024 * 1024)
                if not chunk:
                    break
                offset = 0
                while offset < len(chunk):
                    offset += os.write(target_child, chunk[offset:])
            os.fsync(target_child)
        finally:
            os.close(target_child)
            os.close(source_child)


def _copy_state_directory(
    source: Path,
    target_parent_fd: int,
    *,
    excluded_names: frozenset[str],
    expected_directory_identity: Tuple[int, int],
    expected_files: Dict[str, Tuple[int, int]],
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    source_fd = os.open(str(source), flags)
    os.mkdir("state", 0o700, dir_fd=target_parent_fd)
    target_fd = os.open("state", flags, dir_fd=target_parent_fd)
    try:
        source_details = os.fstat(source_fd)
        if (
            int(source_details.st_dev), int(source_details.st_ino)
        ) != expected_directory_identity:
            raise ReleaseBackupError("state directory identity changed")
        _copy_tree_from_descriptors(
            source_fd,
            target_fd,
            excluded_names=excluded_names,
            expected_files=expected_files,
        )
        os.fsync(target_fd)
    finally:
        os.close(target_fd)
        os.close(source_fd)


def _remove_tree(directory_fd: int) -> None:
    for name in os.listdir(directory_fd):
        details = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISDIR(details.st_mode):
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            child = os.open(name, flags, dir_fd=directory_fd)
            try:
                _remove_tree(child)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=directory_fd)
        else:
            os.unlink(name, dir_fd=directory_fd)


def _backup_sqlite(
    source: Path,
    source_scope: _DatabaseFileScope,
    backup_dir: Path,
    backup_dir_fd: int,
    target_name: str,
    name: str,
) -> None:
    target_flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
    target_flags |= getattr(os, "O_CLOEXEC", 0)
    target_flags |= getattr(os, "O_NOFOLLOW", 0)
    target_fd = os.open(target_name, target_flags, 0o600, dir_fd=backup_dir_fd)
    try:
        details = os.fstat(target_fd)
        target_identity = (int(details.st_dev), int(details.st_ino))
    finally:
        os.close(target_fd)
    target = backup_dir / target_name
    try:
        with _open_attested_rw(source, source_scope) as source_connection:
            _checkpoint_and_validate(source_connection, name)
            source_schema_version = _read_schema_version(
                source_connection, name
            )
            target_opener = lambda: sqlite3.connect(
                target.as_uri() + "?mode=rw", uri=True
            )
            with _attested_maintenance_connection(
                target_opener, target_identity
            ) as target_connection:
                _verify_sqlite_main_identity(
                    target_connection, target, target_identity
                )
                source_connection.backup(target_connection)
            rewrite_flags = os.O_RDWR
            rewrite_flags |= getattr(os, "O_CLOEXEC", 0)
            rewrite_flags |= getattr(os, "O_NOFOLLOW", 0)
            rewrite_fd = os.open(
                target_name,
                rewrite_flags,
                dir_fd=backup_dir_fd,
            )
            try:
                rewrite_details = os.fstat(rewrite_fd)
                if (
                    not stat.S_ISREG(rewrite_details.st_mode)
                    or int(rewrite_details.st_nlink) != 1
                    or (
                        int(rewrite_details.st_dev),
                        int(rewrite_details.st_ino),
                    )
                    != target_identity
                ):
                    raise ReleaseBackupError(
                        "%s backup identity changed" % name
                    )
                _write_backup_schema_version(
                    rewrite_fd,
                    source_schema_version,
                    name,
                )
                _commit_backup_schema_version(rewrite_fd, name)
            finally:
                os.close(rewrite_fd)
            with _attested_maintenance_connection(
                target_opener, target_identity
            ) as target_connection:
                _verify_sqlite_main_identity(
                    target_connection, target, target_identity
                )
                _verify_backup_schema_version(
                    target_connection,
                    source_schema_version,
                    name,
                )
                target_integrity = target_connection.execute(
                    "PRAGMA integrity_check"
                ).fetchall()
                target_foreign_keys = target_connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
                if target_integrity != [("ok",)] or target_foreign_keys:
                    raise ReleaseBackupError(
                        "%s backup integrity validation failed" % name
                    )
            if (
                _read_schema_version(source_connection, name)
                != source_schema_version
            ):
                raise ReleaseBackupError(
                    "%s source schema version changed" % name
                )
        _validate_database_sidecars(source, name, require_absent=True)
        _regular_file_identity(target, "%s backup" % name)
        _validate_database_sidecars(target, "%s backup" % name, require_absent=True)
    except BaseException:
        for suffix in _SIDECAR_SUFFIXES:
            sidecar_name = target_name + suffix
            try:
                os.unlink(sidecar_name, dir_fd=backup_dir_fd)
            except FileNotFoundError:
                pass
        try:
            os.unlink(target_name, dir_fd=backup_dir_fd)
        except FileNotFoundError:
            pass
        raise


def create_release_backup(
    *,
    app_root: Union[os.PathLike[str], str],
    backup_root: Union[os.PathLike[str], str],
    current_dir: Union[os.PathLike[str], str],
    env_file: Union[os.PathLike[str], str],
    state_dir: Union[os.PathLike[str], str],
    state_file: Union[os.PathLike[str], str],
    dry_run_account_file: Union[os.PathLike[str], str],
    log_file: Union[os.PathLike[str], str],
    review_db: Union[os.PathLike[str], str],
    lock_file: Union[os.PathLike[str], str],
    claim_ledger: Union[os.PathLike[str], str],
    before_lock_hook: Optional[Callable[[], None]] = None,
) -> Path:
    """Create one generation backup under a single attested instance lock."""

    paths = {
        name: _absolute_path(value, name)
        for name, value in {
            "app_root": app_root,
            "backup_root": backup_root,
            "current_dir": current_dir,
            "env_file": env_file,
            "state_dir": state_dir,
            "state_file": state_file,
            "dry_run_account_file": dry_run_account_file,
            "log_file": log_file,
            "review_db": review_db,
            "lock_file": lock_file,
            "claim_ledger": claim_ledger,
        }.items()
    }
    try:
        official_lock = validate_official_instance_lock(
            paths["lock_file"],
            paths["claim_ledger"],
        )
        pre_lock = _capture_release_proof(**paths)
    except (InstanceLockError, SignalRetentionMaintenanceError, OSError) as exc:
        raise ReleaseBackupError(str(exc)) from exc
    pre_files = pre_lock.by_name()
    lock_proof = pre_files["INSTANCE_LOCK_FILE"]
    if before_lock_hook is not None:
        before_lock_hook()
    backup_root_fd = None
    backup_dir_fd = None
    backup_name = None
    try:
        with InstanceLock(
            str(paths["lock_file"]),
            expected_identity=official_lock.identity,
            expected_parent_identity=official_lock.parent_identity,
            require_single_link=True,
            exclusive_create=False,
        ):
            post_lock = _capture_release_proof(**paths)
            _assert_same_generation(pre_lock, post_lock)
            post_files = post_lock.by_name()
            review_proof = post_files["REVIEW_DB_FILE"]
            ledger_proof = post_files["N16_CLAIM_LEDGER"]
            review_scope = _DatabaseFileScope(
                path=review_proof.path,
                main_identity=review_proof.main_identity,
                parent_identity=review_proof.parent_identity,
                sidecar_identities=review_proof.sidecar_identities,
                roots=(post_lock.app_root,),
            )
            if review_scope.main_identity is None:
                raise ReleaseBackupError("Review database is missing")
            review_opener_paths = [review_proof.path] + [
                Path(str(review_proof.path) + suffix)
                for suffix in _SIDECAR_SUFFIXES
                if os.path.lexists(str(review_proof.path) + suffix)
            ]
            _assert_no_openers(review_opener_paths)
            ledger_scope = None
            if ledger_proof.main_identity is None:
                ledger_absent_scope = N16ClaimLedgerFileScope(
                    path=ledger_proof.path,
                    main_identity=None,
                    parent_identity=ledger_proof.parent_identity,
                    sidecar_identities=(None, None, None),
                    roots=(post_lock.app_root,),
                )
                ledger_absent_scope.validate_absent()
            else:
                ledger_scope = _DatabaseFileScope(
                    path=ledger_proof.path,
                    main_identity=ledger_proof.main_identity,
                    parent_identity=ledger_proof.parent_identity,
                    sidecar_identities=ledger_proof.sidecar_identities,
                    roots=(post_lock.app_root,),
                )
                ledger_opener_paths = [ledger_proof.path] + [
                    Path(str(ledger_proof.path) + suffix)
                    for suffix in _SIDECAR_SUFFIXES
                    if os.path.lexists(str(ledger_proof.path) + suffix)
                ]
                _assert_no_openers(ledger_opener_paths)

            backup_root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            backup_root_flags |= getattr(os, "O_CLOEXEC", 0)
            backup_root_flags |= getattr(os, "O_NOFOLLOW", 0)
            backup_root_fd = os.open(str(post_lock.backup_root), backup_root_flags)
            if (
                _directory_identity(post_lock.backup_root, "Binance backup root")
                != post_lock.backup_root_identity
            ):
                raise ReleaseBackupError("backup root identity changed")
            for _attempt in range(32):
                backup_name = "release-%s" % secrets.token_hex(16)
                try:
                    os.mkdir(backup_name, 0o700, dir_fd=backup_root_fd)
                except FileExistsError:
                    backup_name = None
                    continue
                break
            if backup_name is None:
                raise ReleaseBackupError("unable to allocate a private backup")
            backup_dir = post_lock.backup_root / backup_name
            backup_dir_fd = os.open(
                backup_name,
                backup_root_flags,
                dir_fd=backup_root_fd,
            )
            directory_details = os.fstat(backup_dir_fd)
            backup_directory_identity = (
                int(directory_details.st_dev),
                int(directory_details.st_ino),
            )
            if (
                stat.S_IMODE(directory_details.st_mode) != 0o700
                or _directory_identity(backup_dir, "release backup directory")
                != backup_directory_identity
            ):
                raise ReleaseBackupError("private backup directory is invalid")

            commit = subprocess.run(
                ["git", "-C", str(post_lock.current_dir), "rev-parse", "--verify", "HEAD"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            commit_text = commit.stdout.strip()
            if commit.returncode != 0 or len(commit_text) != 40:
                raise ReleaseBackupError("current commit could not be recorded")
            _write_new_file(
                backup_dir_fd, "old-commit.txt", (commit_text + "\n").encode("ascii")
            )
            _copy_regular_file(
                post_lock.env_file,
                backup_dir_fd,
                ".env",
                expected_identity=post_lock.env_identity,
            )
            excluded = {
                paths["lock_file"].name,
                paths["claim_ledger"].name,
            }
            excluded.update(
                paths["claim_ledger"].name + suffix
                for suffix in _SIDECAR_SUFFIXES
            )
            _copy_state_directory(
                post_lock.state_dir,
                backup_dir_fd,
                excluded_names=frozenset(excluded),
                expected_directory_identity=post_lock.state_dir_identity,
                expected_files={
                    proof.path.name: proof.main_identity
                    for proof in (
                        post_files["STATE_FILE"],
                        post_files["DRY_RUN_ACCOUNT_FILE"],
                    )
                    if proof.main_identity is not None
                },
            )
            _backup_sqlite(
                review_proof.path,
                review_scope,
                backup_dir,
                backup_dir_fd,
                "trading_review.sqlite3",
                "Review database",
            )
            if ledger_scope is None:
                ledger_absent_scope.validate_absent()
                _write_new_file(
                    backup_dir_fd,
                    "n16_claim_ledger.absent",
                    _ABSENCE_MARKER.encode("ascii"),
                )
                ledger_absent_scope.validate_absent()
                ledger_status = "ABSENT_PRE_N16"
            else:
                _backup_sqlite(
                    ledger_proof.path,
                    ledger_scope,
                    backup_dir,
                    backup_dir_fd,
                    "n16_claim_ledger.sqlite3",
                    "N16 claim ledger",
                )
                ledger_status = "PRESENT"

            final_proof = _capture_release_proof(**paths)
            final_files = final_proof.by_name()
            for name, post in post_files.items():
                final = final_files[name]
                if (
                    final.path != post.path
                    or final.parent_identity != post.parent_identity
                    or final.main_identity != post.main_identity
                ):
                    raise ReleaseBackupError(
                        "effective persistence identity changed during backup"
                    )
            _validate_database_sidecars(
                review_proof.path, "Review database", require_absent=True
            )
            if ledger_scope is None:
                ledger_absent_scope.validate_absent()
            else:
                _validate_database_sidecars(
                    ledger_proof.path,
                    "N16 claim ledger",
                    require_absent=True,
                )
            manifest = {
                "schema_version": 1,
                "commit": commit_text,
                "ledger": ledger_status,
                "review_identity": list(review_proof.main_identity),
                "ledger_identity": (
                    None
                    if ledger_proof.main_identity is None
                    else list(ledger_proof.main_identity)
                ),
            }
            _write_new_file(
                backup_dir_fd,
                "release_backup_manifest.json",
                (
                    json.dumps(
                        manifest,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8"),
            )
            os.fsync(backup_dir_fd)
            if ledger_scope is None:
                ledger_absent_scope.validate_absent()
            result = backup_dir
            backup_dir_fd_to_close = backup_dir_fd
            backup_dir_fd = None
            os.close(backup_dir_fd_to_close)
            backup_root_fd_to_close = backup_root_fd
            backup_root_fd = None
            os.close(backup_root_fd_to_close)
            return result
    except (
        InstanceLockError,
        N16ClaimLedgerError,
        SignalRetentionMaintenanceError,
        sqlite3.Error,
        OSError,
    ) as exc:
        raise ReleaseBackupError(str(exc)) from exc
    finally:
        if backup_dir_fd is not None:
            try:
                _remove_tree(backup_dir_fd)
            finally:
                os.close(backup_dir_fd)
        if backup_name is not None and backup_root_fd is not None:
            try:
                os.rmdir(backup_name, dir_fd=backup_root_fd)
            except FileNotFoundError:
                pass
        if backup_root_fd is not None:
            os.close(backup_root_fd)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create one identity-attested Binance release backup",
    )
    parser.add_argument("--binance-root", required=True)
    parser.add_argument("--backup-root", required=True)
    parser.add_argument("--current-dir", required=True)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--dry-run-account-file", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--review-db", required=True)
    parser.add_argument("--lock-file", required=True)
    parser.add_argument("--n16-claim-ledger", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        backup = create_release_backup(
            app_root=args.binance_root,
            backup_root=args.backup_root,
            current_dir=args.current_dir,
            env_file=args.env_file,
            state_dir=args.state_dir,
            state_file=args.state_file,
            dry_run_account_file=args.dry_run_account_file,
            log_file=args.log_file,
            review_db=args.review_db,
            lock_file=args.lock_file,
            claim_ledger=args.n16_claim_ledger,
        )
    except ReleaseBackupError as exc:
        print("release backup NO-GO: %s" % exc, file=sys.stderr)
        return 1
    print("release backup PASS: %s" % backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
