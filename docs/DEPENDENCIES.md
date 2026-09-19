# Third-party dependencies

Dependencies are installed from their own distributions; their source is not
vendored into this repository. Their licenses are not replaced by our MIT license.

The clean Python 3.13 preparation environment resolved the following versions.
This is a validation snapshot, not a dependency lock or an assertion that future
versions have been tested.

| Package | Version observed | License from installed package metadata |
| --- | --- | --- |
| requests (direct) | 2.34.2 | Apache-2.0 |
| urllib3 | 2.8.0 | MIT |
| idna | 3.20 | BSD-3-Clause |
| certifi | 2026.7.22 | MPL-2.0 |
| charset-normalizer | 3.5.1 | MIT |

Python, SQLite, Git, and the operating system retain their respective licenses.
When redistributing packaged dependencies, review and include the licenses and
notices required by those distributions. The simple source release does not
bundle those runtimes or dependencies.
