import errno
import fcntl
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TextIO, Tuple, Union


class InstanceLockError(RuntimeError):
    pass


OFFICIAL_INSTANCE_LOCK_BASENAME = "trading_bot.lock"


@dataclass(frozen=True)
class OfficialInstanceLockProof:
    path: Path
    identity: Tuple[int, int]
    parent_identity: Tuple[int, int]


def validate_official_instance_lock(
    lock_file: Union[os.PathLike[str], str],
    n16_claim_ledger_file: Union[os.PathLike[str], str],
) -> OfficialInstanceLockProof:
    """Prove the one pre-existing production lock beside the N16 ledger.

    This is a cooperative application contract for repository-authorized
    service and stopped-service maintenance entry points.  It does not freeze
    the operating-system namespace against an uncooperative same-UID process.
    """

    lock_path = Path(lock_file)
    ledger_path = Path(n16_claim_ledger_file)
    if not lock_path.is_absolute() or not ledger_path.is_absolute():
        raise InstanceLockError(
            "Official instance lock and N16 claim ledger paths must be absolute"
        )
    if lock_path.name != OFFICIAL_INSTANCE_LOCK_BASENAME:
        raise InstanceLockError(
            "Official instance lock basename must be trading_bot.lock"
        )
    if lock_path.parent != ledger_path.parent:
        raise InstanceLockError(
            "Official instance lock and N16 claim ledger must share the same real parent"
        )
    try:
        parent_details = os.lstat(str(lock_path.parent))
    except OSError as exc:
        raise InstanceLockError(
            "Official instance lock parent is unavailable"
        ) from exc
    if stat.S_ISLNK(parent_details.st_mode) or not stat.S_ISDIR(
        parent_details.st_mode
    ):
        raise InstanceLockError(
            "Official instance lock parent must be a real directory"
        )
    parent_identity = (
        int(parent_details.st_dev),
        int(parent_details.st_ino),
    )
    try:
        ledger_parent_details = os.lstat(str(ledger_path.parent))
    except OSError as exc:
        raise InstanceLockError(
            "N16 claim ledger parent is unavailable"
        ) from exc
    ledger_parent_identity = (
        int(ledger_parent_details.st_dev),
        int(ledger_parent_details.st_ino),
    )
    if ledger_parent_identity != parent_identity:
        raise InstanceLockError(
            "Official instance lock and N16 claim ledger must share the same real parent"
        )
    try:
        lock_details = os.lstat(str(lock_path))
    except FileNotFoundError as exc:
        raise InstanceLockError(
            "Official instance lock must already exist"
        ) from exc
    except OSError as exc:
        raise InstanceLockError(
            "Official instance lock identity is unavailable"
        ) from exc
    if stat.S_ISLNK(lock_details.st_mode) or not stat.S_ISREG(
        lock_details.st_mode
    ):
        raise InstanceLockError(
            "Official instance lock must be a non-symlink regular file"
        )
    if int(lock_details.st_nlink) != 1:
        raise InstanceLockError(
            "Official instance lock must have exactly one hard link"
        )
    try:
        if os.path.lexists(str(ledger_path)):
            ledger_details = os.lstat(str(ledger_path))
            if (
                int(ledger_details.st_dev),
                int(ledger_details.st_ino),
            ) == (int(lock_details.st_dev), int(lock_details.st_ino)):
                raise InstanceLockError(
                    "Official instance lock must not alias the N16 claim ledger"
                )
    except OSError as exc:
        raise InstanceLockError(
            "N16 claim ledger identity is unavailable"
        ) from exc
    return OfficialInstanceLockProof(
        path=lock_path,
        identity=(int(lock_details.st_dev), int(lock_details.st_ino)),
        parent_identity=parent_identity,
    )


class InstanceLock:
    """Process-wide lifetime lock backed by an independent file descriptor."""

    def __init__(
        self,
        path: str,
        *,
        expected_identity: Optional[Tuple[int, int]] = None,
        expected_parent_identity: Optional[Tuple[int, int]] = None,
        require_single_link: bool = False,
        exclusive_create: bool = False,
    ):
        # Normal bot startup keeps the long-standing resolve/open behaviour.
        # Offline maintenance opts into the fd-level checks below so an
        # allowlisted lock pathname cannot be swapped to another product's
        # inode before the PID write.
        secure = expected_parent_identity is not None or require_single_link
        candidate = Path(path)
        self.path = str(candidate if secure else candidate.resolve())
        self._expected_identity = expected_identity
        self._expected_parent_identity = expected_parent_identity
        self._require_single_link = require_single_link
        self._exclusive_create = exclusive_create
        self._handle: Optional[TextIO] = None

    @property
    def _secure_open(self) -> bool:
        return (
            self._expected_parent_identity is not None
            or self._require_single_link
        )

    @staticmethod
    def _identity(details: os.stat_result) -> Tuple[int, int]:
        return int(details.st_dev), int(details.st_ino)

    def _validate_secure_handle(self, handle: TextIO) -> Tuple[int, int]:
        lock_path = Path(self.path)
        try:
            parent_details = os.lstat(str(lock_path.parent))
            handle_details = os.fstat(handle.fileno())
            path_details = os.lstat(str(lock_path))
        except OSError as exc:
            raise InstanceLockError(
                f"Secure lock identity is unavailable: {self.path}"
            ) from exc
        if (
            not stat.S_ISDIR(parent_details.st_mode)
            or stat.S_ISLNK(parent_details.st_mode)
            or self._identity(parent_details) != self._expected_parent_identity
        ):
            raise InstanceLockError(
                f"Secure lock parent identity changed: {self.path}"
            )
        if (
            not stat.S_ISREG(handle_details.st_mode)
            or not stat.S_ISREG(path_details.st_mode)
            or stat.S_ISLNK(path_details.st_mode)
        ):
            raise InstanceLockError(
                f"Secure lock must remain a non-symlink regular file: {self.path}"
            )
        if self._require_single_link and (
            int(handle_details.st_nlink) != 1 or int(path_details.st_nlink) != 1
        ):
            raise InstanceLockError(
                f"Secure lock must have exactly one hard link: {self.path}"
            )
        handle_identity = self._identity(handle_details)
        if self._identity(path_details) != handle_identity:
            raise InstanceLockError(
                f"Secure lock pathname changed before PID write: {self.path}"
            )
        if (
            self._expected_identity is not None
            and handle_identity != self._expected_identity
        ):
            raise InstanceLockError(
                f"Secure lock inode changed before PID write: {self.path}"
            )
        return handle_identity

    def _open_secure_handle(self) -> TextIO:
        lock_path = Path(self.path)
        if not lock_path.is_absolute():
            raise InstanceLockError("Secure lock path must be absolute")
        flags = os.O_RDWR
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        if self._exclusive_create:
            flags |= os.O_CREAT | os.O_EXCL
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_flags |= getattr(os, "O_CLOEXEC", 0)
        directory_flags |= getattr(os, "O_NOFOLLOW", 0)
        directory_fd = None
        file_fd = None
        handle = None  # type: Optional[TextIO]
        try:
            directory_fd = os.open(str(lock_path.parent), directory_flags)
            parent_details = os.fstat(directory_fd)
            if (
                not stat.S_ISDIR(parent_details.st_mode)
                or self._identity(parent_details) != self._expected_parent_identity
            ):
                raise InstanceLockError(
                    f"Secure lock parent identity changed: {self.path}"
                )
            file_fd = os.open(
                lock_path.name,
                flags,
                0o600,
                dir_fd=directory_fd,
            )
            handle = os.fdopen(file_fd, "r+", encoding="utf-8")
            file_fd = None
            opened_identity = self._validate_secure_handle(handle)
            if self._expected_identity is None:
                self._expected_identity = opened_identity
                self._exclusive_create = False
            result = handle
            handle = None
            return result
        except FileExistsError as exc:
            raise InstanceLockError(
                f"Secure lock appeared before acquire: {self.path}"
            ) from exc
        except OSError as exc:
            raise InstanceLockError(
                f"Unable to open secure lock without following aliases: {self.path}"
            ) from exc
        finally:
            if handle is not None:
                handle.close()
            if file_fd is not None:
                os.close(file_fd)
            if directory_fd is not None:
                os.close(directory_fd)

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    def acquire(self) -> "InstanceLock":
        if self.acquired:
            return self

        lock_path = Path(self.path)
        if self._secure_open:
            handle = self._open_secure_handle()
        else:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+", encoding="utf-8")
        flock_acquired = False
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            flock_acquired = True
            if self._secure_open:
                # Recheck the fd, visible pathname, link count, and parent
                # after flock and immediately before the first destructive
                # operation on the inode.
                self._validate_secure_handle(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n")
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            if flock_acquired:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except BaseException:
                    pass
            try:
                handle.close()
            except BaseException:
                pass
            if (
                not flock_acquired
                and isinstance(exc, OSError)
                and exc.errno in (errno.EACCES, errno.EAGAIN)
            ):
                raise InstanceLockError(
                    f"Another trading bot instance holds INSTANCE_LOCK_FILE={self.path}"
                ) from exc
            raise

        self._handle = handle
        return self

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> "InstanceLock":
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()
