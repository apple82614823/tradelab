# Changelog

## Unreleased — initial public candidate

- Exported a privately developed codebase without its private Git history.
- Preserved strategy evaluation and trade execution implementations without
  behavioral changes.
- Replaced a private one-record legacy maintenance identity with a fixed
  synthetic example in schema checks, validators, and two tests. Public schemas
  are deliberately incompatible with the original private database; no migration
  between these editions is provided.
- Replaced deployment identifiers and historical maintenance example values
  with generic placeholders in the engineering reference.
- Updated two documentation tests to read the relocated engineering reference.
- Aligned inherited tests with existing behavior: retired two-win qualification,
  no new paper entries, zero-row signal publication, and the previously adopted
  0.5% structured-fill tolerance. Added explicit tolerance endpoint checks;
  trading implementations were not changed to satisfy these tests.
- Added English/Chinese overview, MIT license, contribution guidance, security
  reporting guidance, and a proposed CI workflow.

The original project was developed privately with ChatGPT and Codex assistance.
This candidate is not a claim of public adoption, a completed GitHub release,
verified trading performance, or an approved funding application.
