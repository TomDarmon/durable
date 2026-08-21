# Limitations

- RustFS is a local validation backend, not proof of production S3 semantics.
- There is no cross-region linearizability.
- There is no production failover authority.
- Safe GC publication permits are not implemented; physical deletion remains
  disabled.
- There is no exactly-once guarantee for external side effects.
- There are no Git, Origin, or vector-domain semantics in core.
- The cache is discardable and never authoritative.
- The queue is not a durable application log.
- Journal retry receipts are bounded by the retained head metadata.
- Distributed cache coherence is intentionally not implemented.
