# Repository Context

This repository has two top-level layers:

- `durable/` contains the reusable Rust durable-storage library.
- `origin/` is reserved for the Origin-like application to be built next.

When changing the durable substrate, follow `durable/AGENT.md` and keep the
library free of Git, Origin, or vector-domain semantics.

When building Origin, use `origin/README.md` and `origin/docs/context.md` first.
Origin should consume durable APIs rather than reimplementing object storage,
root CAS, journaling, queueing, caching, or retention protocols.
