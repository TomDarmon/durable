# Experimental Project Policy

`durable` is fully experimental. There are no backward-compatibility guarantees.
APIs, file formats, configuration, crate boundaries, and internals may be changed
or removed whenever doing so improves correctness or readability.

- Prefer substantial refactors over preserving a confusing design.
- Remove dead code instead of keeping compatibility layers, deprecated paths, or
  speculative extension points.
- Keep code explicit, conventional, and human-readable. Avoid broad frameworks,
  unnecessary abstractions, generated-looking boilerplate, and AI-style filler.
- Optimize for fast Rust iteration while preserving correctness and strict quality
  gates.

## Testing

- Prefer one focused, fast, real end-to-end test for every happy-path behavior.
- Add unit tests only for meaningful edge cases, failure classification, or logic
  that cannot be exercised clearly end to end.
- End-to-end tests should use real component boundaries and real local services
  where appropriate, including RustFS for storage integration.
- Use mocks only where a real dependency is impractical or nondeterministic, or to
  model a narrowly scoped fault condition.
- Keep tests deterministic and fast. Use controlled barriers and fault injection;
  do not use sleep-based race tests.
