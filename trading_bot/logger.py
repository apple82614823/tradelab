import logging
import stat
from datetime import date, datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


APPLICATION_LOG_RETENTION_DAYS = 15


class ApplicationTimedRotatingFileHandler(TimedRotatingFileHandler):
    def getFilesToDelete(self):
        # TimedRotatingFileHandler normally enforces ``backupCount`` by
        # quantity.  That would delete the exact 15-day boundary whenever a
        # directory contains today plus fifteen daily backups.  This
        # application promises an age boundary instead: only its own files
        # strictly older than the retention window may be removed.
        files_to_delete = set()
        base_path = Path(self.baseFilename)
        prefix = f"{base_path.name}."
        cutoff = date.today() - timedelta(days=APPLICATION_LOG_RETENTION_DAYS)

        try:
            candidates = list(base_path.parent.iterdir())
        except OSError:
            return []

        for candidate in candidates:
            if not candidate.name.startswith(prefix):
                continue
            try:
                details = candidate.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(details.st_mode) or int(details.st_nlink) != 1:
                continue
            suffix = candidate.name[len(prefix) :]
            if self.extMatch.match(suffix) is None:
                continue
            try:
                rotation_date = datetime.strptime(suffix, "%Y-%m-%d").date()
            except ValueError:
                continue
            if rotation_date < cutoff:
                files_to_delete.add(str(candidate))

        return sorted(files_to_delete)


def setup_logging(log_file: str) -> logging.Logger:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("trading_bot")
    logger.setLevel(logging.INFO)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = ApplicationTimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=APPLICATION_LOG_RETENTION_DAYS,
        encoding="utf-8",
        utc=False,
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
