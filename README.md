# TradeLab — experimental trading engineering reference

[中文说明](README.zh-CN.md)

A Python research project for Binance USD-M futures, with strategy evaluation,
order execution, persistent trade state, SQLite audit records, and bounded signal
retention. Developed privately with assistance from ChatGPT and Codex; this is a
candidate for its first public release, not a claim of an established community.

**Experimental software, not a verified profitable trading product.** Passing
tests does not establish investment performance or safety for your account.
The project is not affiliated with or endorsed by Binance or OpenAI.

## What is included

- N01–N05 are the active multi-strategy set. They do not require a winning streak
  before live execution; ordinary validation and execution checks still apply.
- N06–N25 implementations and their tests remain for historical compatibility.
  They do not generate new opportunities in the current active configuration.
- Order handling, persisted execution state, audit events, and maintenance tools.
- CURRENT/STAGING signal publication and historical evidence retention.
- Generated fixtures and regression tests; no production database, credentials,
  account history, or server configuration is included.

The default configuration still selects the earlier single-strategy path.
Set `MULTI_STRATEGY_ENABLED=true` to select N01–N05. This switch does not enable
live trading. Existing historical paper-trade handling is not a promise of a
complete backtesting or paper-trading platform for every strategy.

## Start with the tests

Use macOS or Linux, Python 3.9+ and Git. The code uses POSIX locking and
filesystem behavior; native Windows is not supported by this release.
Only interpreter/OS combinations listed in a completed verification report
should be treated as tested. Run commands from the repository root.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -p 'test_*.py'
```

Tests use local fixtures and mocks, not a funded account. Do not place real
credentials in a development checkout or CI environment. Larger resource
benchmarks under `tests/*scale_gate.py` are separate, opt-in commands; ordinary
test discovery does not run them.

## Optional dry-run exploration

Copy `.env.example` to `.env`, keep `BINANCE_DRY_RUN=true`, and leave both API
credential fields empty. Review all parameters before any execution.

```sh
cp .env.example .env
python main.py
```

Unlike the offline tests, running the application contacts public exchange APIs
and writes local state, logs, and a review database. Exchange access depends on
your location and account eligibility. No exchange connection is required to
study the code or run the regression tests.

Database initialization and migration are intentionally explicit. A fresh or
older database can be rejected until the appropriate schema preparation has
been completed. This first public candidate does **not** promise a one-command,
ready-to-trade installation. Never use a production database for exploration.

## Important limitations

- No claim of profitability, fixed signal frequency, or verified backtest returns.
- A private, one-record legacy maintenance identity is replaced with a fixed
  synthetic example in this public edition. Its schema is intentionally **not
  compatible with the original private database**. Do not point this edition at
  private production data or treat the example as authorization for a real record.
- The sample uses a 20% target risk fraction and a 95% margin cap. These are
  inherited, aggressive research parameters, **not recommended account settings**.
- A 5R take-profit rule describes intended order construction, not guaranteed
  realized reward or execution price.
- Live trading requires credentials and two explicit configuration changes.
  Do not enable it just to evaluate this repository. Stopping a process does not
  itself close positions already held at an exchange.
- The inherited maintenance architecture is substantial. We retain it rather
  than delete dependencies merely to make the first release appear simpler.
- API behavior and exchange rules can change; this snapshot is not certification
  of current exchange compatibility.

## Documentation and contribution

- [Architecture and current scope](docs/ARCHITECTURE.md)
- [Detailed legacy engineering reference (Chinese)](docs/LEGACY_ENGINEERING.md)
- [Contributing](CONTRIBUTING.md)
- [Reporting security concerns](SECURITY.md)
- [Changes](CHANGELOG.md)
- [Dependency license inventory](docs/DEPENDENCIES.md)

The legacy reference preserves historical design details and generic example
maintenance commands. It is not an instruction to run a migration on a real
account or a description of a currently running service.

We welcome reproducible bug reports, regression tests, documentation corrections,
and small improvements. AI-assisted contributions are welcome; contributors
remain responsible for understanding, testing, and reviewing their changes.

## License

[MIT](LICENSE). Third-party dependencies retain their own licenses. No exchange
market dataset or personal trading records are distributed with this project.
