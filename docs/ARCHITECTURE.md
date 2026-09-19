# Architecture and scope

This document describes the inherited implementation, not a redesigned platform.

| Area | Main modules | Role |
| --- | --- | --- |
| Configuration | `config.py` | Environment loading, validation, runtime paths |
| Evaluation | `analyzer.py`, `strategies.py`, `strategy_scheduler.py` | Market conditions, active strategy selection, plans |
| Exchange | `binance_client.py`, `trader.py`, `precision.py` | Requests, order handling, quantities and price increments |
| Runtime | `main.py`, `monitor.py`, `state.py`, `instance_lock.py` | Scanning, persisted state, monitoring, single-instance lock |
| Audit | `recorder.py`, `signal_retention.py` | SQLite records, CURRENT/STAGING lifecycle, explicit maintenance |
| Compatibility | `n*_analyzer.py`, schema modules, `paper_trader.py` | Historical strategies, evidence and old paper-trade handling |

Paths in the table are relative to `trading_bot/`.

In multi-strategy mode, `ACTIVE_STRATEGIES` selects N01–N05. N06–N25 code and
schemas remain because compatibility and historical evidence have dependencies
beyond merely selecting which analyzer runs. Their presence is not proof that
all 25 strategies are active.

The recorder distinguishes replaceable scan decisions from permanent audit
evidence. A completed signal batch is published before downstream execution;
failed persistence must not become an opportunity to trade without audit state.
Several legacy schema families and a separate claim ledger remain, so initial
database preparation is not equivalent to creating an empty SQLite file.

Tests exercise individual modules and combinations with generated data and
exchange mocks. They do not replace a live exchange integration assessment or
measure statistical trading efficacy. Resource-scale scripts are opt-in.

No real database or production state accompanies this release. Do not recreate
someone else's environment from historical operations examples. Study the
[engineering reference](LEGACY_ENGINEERING.md) as implementation context, not a
deployment prescription for an existing account.
