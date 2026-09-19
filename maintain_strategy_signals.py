"""Binance-only wrapper for explicit offline strategy-signal maintenance.

This script does not load .env and never operates any service or global logging
configuration.  It touches only paths explicitly supplied on its command line.
"""

from trading_bot.signal_retention import main


if __name__ == "__main__":
    raise SystemExit(main())
