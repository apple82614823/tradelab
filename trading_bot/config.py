import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional, Union


LIVE_TRADING_CONFIRMATION = "I_UNDERSTAND_REAL_MONEY_RISK"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
FALSE_ENV_VALUES = frozenset({"0", "false", "no", "off"})


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in TRUE_ENV_VALUES:
        return True
    if normalized in FALSE_ENV_VALUES:
        return False
    raise ValueError(
        f"{name} must be one of 1,true,yes,on,0,false,no,off; got {value!r}."
    )


def _decimal_env(name: str, default: str) -> Decimal:
    return Decimal(os.getenv(name, default))


def _load_env_file(path: Optional[Union[str, Path]] = None) -> None:
    env_path = Path(path) if path is not None else PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _project_path_env(name: str, default: str) -> str:
    configured_path = Path(os.getenv(name, default))
    if not configured_path.is_absolute():
        configured_path = PROJECT_ROOT / configured_path
    return str(configured_path.resolve())


@dataclass(frozen=True)
class Config:
    base_url: str
    api_key: str
    api_secret: str
    dry_run: bool
    live_confirmation: str
    dry_run_balance_usdt: Decimal
    poll_interval_seconds: int
    funding_rate_abs_threshold: Decimal
    kline_interval: str
    kline_limit: int
    trend_window: int
    dry_run_max_leverage: int
    dry_run_tradifi_max_leverage: int
    risk_balance_fraction: Decimal
    take_profit_r_multiple: Decimal
    stop_loss_amplitude_ratio: Decimal
    min_stop_loss_pct: Decimal
    max_stop_loss_pct: Decimal
    max_margin_balance_fraction: Decimal
    symbol_cooldown_hours: int
    state_file: str
    dry_run_account_file: str
    log_file: str
    review_db_file: str
    multi_strategy_enabled: bool = False
    instance_lock_file: str = str(PROJECT_ROOT / "state/trading_bot.lock")
    recv_window: int = 5000
    request_timeout_seconds: int = 10
    request_retries: int = 3
    request_retry_delay_seconds: int = 5
    network_reconnect_delay_seconds: int = 30

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def n16_claim_ledger_file(self) -> str:
        """Permanent N16 claims share the configured state generation.

        This is intentionally derived from ``STATE_FILE`` instead of being a
        second independently redirectable environment path.  Deployment and
        rollback therefore move the position state directory and the N16
        permanent ledger as one application-owned generation, while clearing
        ``position.json`` can never clear the independent claim database.
        """

        return str(
            Path(self.state_file).resolve().with_name(
                "n16_claim_ledger.sqlite3"
            )
        )

    def validate(self) -> None:
        if self.poll_interval_seconds <= 0:
            raise ValueError("POLL_INTERVAL_SECONDS must be positive.")
        if self.kline_interval != "15m":
            raise ValueError("KLINE_INTERVAL must be exactly 15m.")
        if self.kline_limit < self.trend_window + 1:
            raise ValueError("KLINE_LIMIT must be at least TREND_WINDOW + 1.")
        if self.multi_strategy_enabled and self.kline_limit < 122:
            raise ValueError(
                "KLINE_LIMIT must be at least 122 when MULTI_STRATEGY_ENABLED=true "
                "for the N09 80-bar decline plus 40-bar rebound boundary."
            )
        if self.funding_rate_abs_threshold <= 0:
            raise ValueError("FUNDING_RATE_ABS_THRESHOLD must be positive.")
        if self.dry_run_max_leverage <= 0:
            raise ValueError("DRY_RUN_MAX_LEVERAGE must be positive.")
        if self.dry_run_tradifi_max_leverage <= 0:
            raise ValueError("DRY_RUN_TRADIFI_MAX_LEVERAGE must be positive.")
        if not Decimal("0") < self.risk_balance_fraction < Decimal("1"):
            raise ValueError("RISK_BALANCE_FRACTION must be between 0 and 1.")
        if self.take_profit_r_multiple <= 0:
            raise ValueError("TAKE_PROFIT_R_MULTIPLE must be positive.")
        if self.stop_loss_amplitude_ratio <= 0:
            raise ValueError("STOP_LOSS_AMPLITUDE_RATIO must be positive.")
        if not Decimal("0") < self.min_stop_loss_pct <= self.max_stop_loss_pct:
            raise ValueError("Stop loss pct bounds must satisfy 0 < MIN <= MAX.")
        if not Decimal("0") < self.max_margin_balance_fraction <= Decimal("1"):
            raise ValueError("MAX_MARGIN_BALANCE_FRACTION must be between 0 and 1.")
        if self.symbol_cooldown_hours <= 0:
            raise ValueError("SYMBOL_COOLDOWN_HOURS must be positive.")
        runtime_paths = {
            "STATE_FILE": Path(self.state_file).resolve(),
            "DRY_RUN_ACCOUNT_FILE": Path(
                self.dry_run_account_file
            ).resolve(),
            "LOG_FILE": Path(self.log_file).resolve(),
            "REVIEW_DB_FILE": Path(self.review_db_file).resolve(),
            "INSTANCE_LOCK_FILE": Path(self.instance_lock_file).resolve(),
            "DERIVED_N16_CLAIM_LEDGER": Path(
                self.n16_claim_ledger_file
            ).resolve(),
        }
        names = tuple(runtime_paths)
        for index, left_name in enumerate(names):
            left = runtime_paths[left_name]
            for right_name in names[index + 1 :]:
                right = runtime_paths[right_name]
                same_resolved_path = left == right
                same_existing_inode = False
                if not same_resolved_path and left.exists() and right.exists():
                    try:
                        same_existing_inode = os.path.samefile(left, right)
                    except OSError:
                        same_existing_inode = True
                if same_resolved_path or same_existing_inode:
                    raise ValueError(
                        "Runtime persistence paths must be pairwise distinct: "
                        f"{left_name} conflicts with {right_name}."
                    )
        if not self.dry_run:
            if not self.has_credentials:
                raise ValueError("Live trading requires BINANCE_API_KEY and BINANCE_API_SECRET.")
            if self.live_confirmation != LIVE_TRADING_CONFIRMATION:
                raise ValueError(
                    "Live trading requires BINANCE_CONFIRM_LIVE_TRADING="
                    f"{LIVE_TRADING_CONFIRMATION}."
                )


def load_config() -> Config:
    _load_env_file()
    config = Config(
        base_url=os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/"),
        api_key=os.getenv("BINANCE_API_KEY", ""),
        api_secret=os.getenv("BINANCE_API_SECRET", ""),
        dry_run=_bool_env("BINANCE_DRY_RUN", True),
        live_confirmation=os.getenv("BINANCE_CONFIRM_LIVE_TRADING", ""),
        dry_run_balance_usdt=_decimal_env("DRY_RUN_BALANCE_USDT", "1000"),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        funding_rate_abs_threshold=_decimal_env(
            "FUNDING_RATE_ABS_THRESHOLD",
            os.getenv("FUNDING_RATE_THRESHOLD", "0.015"),
        ),
        kline_interval=os.getenv("KLINE_INTERVAL", "15m"),
        kline_limit=int(os.getenv("KLINE_LIMIT", "122")),
        trend_window=int(os.getenv("TREND_WINDOW", "96")),
        dry_run_max_leverage=int(os.getenv("DRY_RUN_MAX_LEVERAGE", os.getenv("MAX_LEVERAGE_CAP", "50"))),
        dry_run_tradifi_max_leverage=int(os.getenv("DRY_RUN_TRADIFI_MAX_LEVERAGE", "20")),
        risk_balance_fraction=_decimal_env(
            "RISK_BALANCE_FRACTION",
            os.getenv("STOP_LOSS_BALANCE_FRACTION", "0.20"),
        ),
        take_profit_r_multiple=_decimal_env("TAKE_PROFIT_R_MULTIPLE", "5"),
        stop_loss_amplitude_ratio=_decimal_env("STOP_LOSS_AMPLITUDE_RATIO", "0.12"),
        min_stop_loss_pct=_decimal_env("MIN_STOP_LOSS_PCT", "0.01"),
        max_stop_loss_pct=_decimal_env("MAX_STOP_LOSS_PCT", "0.05"),
        max_margin_balance_fraction=_decimal_env("MAX_MARGIN_BALANCE_FRACTION", "0.95"),
        symbol_cooldown_hours=int(os.getenv("SYMBOL_COOLDOWN_HOURS", "4")),
        state_file=_project_path_env("STATE_FILE", "state/position.json"),
        dry_run_account_file=_project_path_env(
            "DRY_RUN_ACCOUNT_FILE", "state/dry_run_account.json"
        ),
        log_file=_project_path_env("LOG_FILE", "logs/trading.log"),
        review_db_file=_project_path_env(
            "REVIEW_DB_FILE", "data/trading_review.sqlite3"
        ),
        multi_strategy_enabled=_bool_env("MULTI_STRATEGY_ENABLED", False),
        instance_lock_file=_project_path_env(
            "INSTANCE_LOCK_FILE", "state/trading_bot.lock"
        ),
    )
    config.validate()
    return config
