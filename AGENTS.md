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
- New scenarios belong in `FileRecordStoreExceptionHandlingTest` or a sibling test class; name files `*Test` so Surefire picks them up.
- **Note**: `SimpleRecordStoreTest` has been deleted - all exception handling and persistence testing is now covered by `FileRecordStoreExceptionHandlingTest` which includes:
  - Comprehensive persistence verification after exceptions
  - verifyStoreIntegrity() helper for data validation
  - Enhanced scenario testing with persistence checks
  - Behavior-based verification instead of internal state inspection
- Crash-safety coverage uses the replay harness built into `FileRecordStoreExceptionHandlingTest`:
  - `RecordsFileSimulatesDiskFailures` swaps the production `RandomAccessFile` for an `InterceptedRandomAccessFile` so every I/O call flows through a `WriteCallback`.
  - `verifyWorkWithIOExceptions` first runs the scenario with a `StackCollectingWriteCallback` to capture the full sequence of file operations (stack traces trimmed once the call exits `com.github.simbo1905`).
  - It then replays the exact same scenario once per recorded call, using `CrashAtWriteCallback` to throw an `IOException` at that call index. Each run mimics a crash right after the intercepted disk operation.
  - After the induced failure, the test closes the write handle, reopens the file with a plain `FileRecordStore`, enumerates `keys()`, and calls `readRecordData` on every key. This forces the `CRC32` payload check mentioned in `README.md` and asserts `getNumRecords()` matches what is readable, flagging any divergence as corruption.
  - The pattern is exercised across inserts, updates, deletes, compaction scenarios, and both narrow/padded payloads (see `string1k`) to brute-force every write ordering.
- Any change that affects write ordering, fsync boundaries, or persistence metadata must be implemented in a crash-safe order and accompanied by updated replay tests.
- Capture `java.util.logging` at `FINE`/`FINEST` when diagnosing failures; `TracePlayback` can rebuild a store from log traces for reproduction.
- Run tests with custom log levels using: `-ea -Dcom.github.trex_paxos.srs.testLogLevel=FINEST` (or FINE, FINER, etc.)

## Commit & Pull Request Guidelines
- Write present-tense, 72-character subjects (`Update pom.xml to require Java 21`); add context or links in the body when needed.
- Reference related issues and describe crash-safety implications or new coverage in PR descriptions.
- Include evidence of `mvn test` (and any targeted replay logs) before requesting review.
- Rebase onto the latest mainline so CI reflects the final diff; attach screenshots only when they clarify tooling output.

## Configuration & Logging Tips
- Tune key length and padding with system properties such as `-Dcom.github.simbo1905.srs.BaseRecordStore.MAX_KEY_LENGTH=128` to mirror production limits.
- Enable `Level.FINEST` logging when investigating disk corruption; redact sensitive keys before sharing traces.

## Documentation Standards
- Use JEP 467 documentation format with `///` triple-slash comments instead of traditional `/**` javadoc style
- Place documentation on the line immediately above the element being documented
- Example: `/// Returns the file path of this record store.` followed by `public Path getFilePath()`

## Logging Standards
- **CRITICAL**: Never add temporary INFO level logging for debugging purposes
- Always use appropriate log levels: FINE for normal debugging, FINEST for detailed tracing
- Use `JulLoggingConfig` system properties to control logging levels: `-Djava.util.logging.config.file=logging.properties`
- Logging should be permanent and controlled via configuration, not added/removed from code
- Adding temporary logging and then removing it is considered fraudulent practice

## Critical API Behavior

### maxKeyLength Enforcement
The `maxKeyLength` parameter is **fundamental and enforced**:
- Files store their `maxKeyLength` in the header permanently
- When opening existing files, you **must** use the same `maxKeyLength` that was used to create the file
- Different `maxKeyLength` values will throw `IllegalArgumentException`
- This prevents data corruption and maintains file format integrity
- If you need different key lengths, create a new database and migrate data

## Build Output Analysis
- Always redirect compile output to a file, then use `tail` and `rg` to analyze errors systematically
- Never filter expected errors - analyze the complete output to understand all issues
- Use this pattern for systematic error analysis:
```shell
mvn test-compile > compile.log 2>&1; tail -50 compile.log; echo "=== FULL ERRORS ==="; rg "ERROR|error:|cannot find symbol" compile.log
```
- Overwrite a single temp file (`compile.log`) rather than creating multiple log files

## API Design Patterns
- Follow MVStore (H2 Database) builder pattern design for create vs open auto-detection
- Builder should automatically handle file existence validation and appropriate constructor selection
- Provide fluent API that eliminates user confusion about create vs open semantics
- Credit MVStore inspiration in javadoc: `/// Builder for creating FileRecordStore instances with a fluent API inspired by H2 MVStore.`

## Use Modern CLI Tools When Available

Check for `rg` and `perl` and other power tools and use them if that makes sense. For example to do mechanical refactors
across many sites perfer things like:

```shell
#  □ Replace Lombok `val` usages with `final var` in tests and sources.
perl -pi -e 's/\bval\b/final var/g' $(rg -l '\bval\b' --glob '*.java')
perl -pi -e 's/^import lombok\.final var;\n//' $(rg -l 'import lombok\.final var;' --glob '*.java')
```

## Logging Policy - CRITICAL

**NEVER DELETE LOGGING LINES** - Once logging is added at the appropriate level (FINE/FINEST), it becomes permanent infrastructure:

- Logging lines are not "temporary debug code" - they are permanent observability features
- Removing logging lines destroys debugging capability for future issues
- If a log level feels wrong, adjust the level, but never remove the line
- All logging must use appropriate levels: FINE for normal debugging, FINEST for detailed tracing
- System properties control visibility - never remove logging to "clean up" output
- Deleting logging lines is considered a destructive act that harms future debugging
