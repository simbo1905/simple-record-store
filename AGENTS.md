# Repository Guidelines

## Project Structure & Module Organization
- Core store code lives in `src/main/java/com/github/trex_paxos/srs`; keep new storage primitives beside `FileRecordStore` for discoverability.
- Shared helpers (`ByteSequence`, `RandomAccessFileInterface`) sit in the same package so tests can exercise them without extra wiring.
- Place unit and property tests in `src/test/java`, mirroring package names; keep generated Maven artefacts inside `target/` and out of version control.

## Build, Test, and Development Commands
- `mvn compile` — validates Lombok usage and Java 21 compatibility.
- `mvn test` — runs the JUnit 4 suite, including crash-replay harnesses; execute before every push.
- `mvn package` — builds the distributable JAR plus sources/javadoc for release validation.
- `mvn clean deploy` — publishes snapshots to OSSRH; requires configured credentials and GPG keys.

## Coding Style & Naming Conventions
- Use four-space indentation and brace placement as shown in `FileRecordStore.java`.
- Keep packages lowercase with underscores, classes in UpperCamelCase, methods/fields in lowerCamelCase.
- Lean on Lombok for boilerplate; avoid duplicating generated accessors or synchronization.
- Encode keys deterministically; prefer `ByteSequence.stringToUtf8` over `String#getBytes` to prevent charset drift.

## Testing Guidelines
- New scenarios belong in `SimpleRecordStoreTest` or a sibling test class; name files `*Test` so Surefire picks them up.
- Crash-safety coverage uses the replay harness built into `SimpleRecordStoreTest`:
  - `RecordsFileSimulatesDiskFailures` swaps the production `RandomAccessFile` for an `InterceptedRandomAccessFile` so every I/O call flows through a `WriteCallback`.
  - `verifyWorkWithIOExceptions` first runs the scenario with a `StackCollectingWriteCallback` to capture the full sequence of file operations (stack traces trimmed once the call exits `com.github.simbo1905`).
  - It then replays the exact same scenario once per recorded call, using `CrashAtWriteCallback` to throw an `IOException` at that call index. Each run mimics a crash right after the intercepted disk operation.
  - After the induced failure, the test closes the write handle, reopens the file with a plain `FileRecordStore`, enumerates `keys()`, and calls `readRecordData` on every key. This forces the `CRC32` payload check mentioned in `README.md` and asserts `getNumRecords()` matches what is readable, flagging any divergence as corruption.
  - The pattern is exercised across inserts, updates, deletes, compaction scenarios, and both narrow/padded payloads (see `string1k`) to brute-force every write ordering.
- Any change that affects write ordering, fsync boundaries, or persistence metadata must be implemented in a crash-safe order and accompanied by updated replay tests.
- Capture `java.util.logging` at `FINE`/`FINEST` when diagnosing failures; `TracePlayback` can rebuild a store from log traces for reproduction.

## Commit & Pull Request Guidelines
- Write present-tense, 72-character subjects (`Update pom.xml to require Java 21`); add context or links in the body when needed.
- Reference related issues and describe crash-safety implications or new coverage in PR descriptions.
- Include evidence of `mvn test` (and any targeted replay logs) before requesting review.
- Rebase onto the latest mainline so CI reflects the final diff; attach screenshots only when they clarify tooling output.

## Configuration & Logging Tips
- Tune key length and padding with system properties such as `-Dcom.github.simbo1905.srs.BaseRecordStore.MAX_KEY_LENGTH=128` to mirror production limits.
- Enable `Level.FINEST` logging when investigating disk corruption; redact sensitive keys before sharing traces.
