# Implementation Notes

- Immutable object IDs are `SHA-256(domain_separator(format) || bytes)`.
- `DurableObjectRef` carries a private scope identity and can only be created by
  successful writes or trusted backend reads.
- Root revisions are logical UUIDs stored in root envelopes. S3/RustFS ETags are
  private conditional-write tokens and are never exposed as `Revision`.
- The journal uses immutable page objects and a CAS root head. Group commit is an
  in-process broker with bounded batch size and delay.
- Queue state is notification state, not an authoritative application log.
- Cache entries include scope identity, object ID, representation, extent, and
  integrity in their keys. Disk entries have local checksums and are disposable.
- Retention iteration 1 implements reader epochs and keeps physical deletion
  disabled.
- RustFS integration is split into backend conformance and full-stack e2e:
  conformance proves the S3/RustFS adapter contract, while e2e drives substrate,
  cache, journal, queue, worker, and retention together over RustFS using fresh
  clients instead of backend restarts.
- RustFS container restart is kept as an optional local persistence smoke test;
  it is not treated as an S3 bucket semantics test.

## Deviations

- RustFS is pinned by image digest rather than a semantic version tag because a
  stable semantic tag was not confirmed locally.
- The worker runtime exposes a small `run_once` API instead of a
  long-running service framework. It is enough to validate handler fencing,
  retry, and at-least-once contracts without adding broad runtime machinery.
