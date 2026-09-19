# Contributing

Please start with a small, reproducible issue or a focused pull request.
There is no promise of a particular response time.

1. Run the unittest command in the README before and after your change.
   CI runs the same complete discovery in four concurrent,
   module-preserving shards.
2. Add a regression test for a bug fix. Use generated data and mocked exchange
   responses; never include a real account, transaction history, or API secret.
3. Explain what changed, why, and which interpreter/OS you tested.
4. Keep strategy changes separate from persistence or execution changes.
5. Discuss database schema changes before implementation. Do not silently
   discard old data or relax consistency checks merely to pass a test.

No contribution requires running real trades. Do not post `.env`, databases,
logs, screenshots containing account details, or remote access credentials.
Reduce a problem to a synthetic fixture instead.

AI-assisted work is welcome. Disclose material assistance in the PR description,
review the resulting code, and report only tests you actually ran. Never send
other people's confidential information to an AI tool.

By submitting a contribution, you represent that you have the right to do so and
agree that the contribution may be distributed under this project's MIT license.
Retain applicable third-party notices and identify imported code and its source.

The initial maintenance focus is reproducible setup, regression coverage,
documented limitations, and correctness fixes, not a promised trading return.
