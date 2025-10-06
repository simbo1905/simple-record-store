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
- All exception handling and persistence testing is covered by `FileRecordStoreExceptionHandlingTest` which includes:
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
- Must be between 1 and 32763 bytes (Short.MAX_VALUE - 4 for CRC32 overhead)
- Files store their `maxKeyLength` in the header with magic number validation permanently
- File format: 4-byte magic number (0xBEEBBEEB) followed by 2-byte key length (upgraded from 1-byte)
- When opening existing files, you **must** use the same `maxKeyLength` that was used to create the file
- Different `maxKeyLength` values will throw `IllegalArgumentException`
- Invalid magic number throws `IllegalStateException` indicating corrupted or incompatible file
- This prevents data corruption and maintains file format integrity
- **Constructor Requirement**: `maxKeyLength` is now required - no default value provided
- If you need different key lengths, create a new database and migrate data

### Constructor Requirements (Post PR #86)
The FileRecordStore constructor now requires explicit sizing parameters:
- **preferredExpansionSize**: Expansion size in bytes for header region growth
- **preferredBlockSize**: Block size in bytes for data alignment (must be power of 2)
- **initialHeaderRegionSize**: Initial header region size in bytes
- **maxKeyLength**: Required parameter - no default value

These parameters replace the previous default-based approach with explicit sizing hints that the builder computes based on user-friendly KiB/MiB inputs.

## Build Output Analysis
- Always redirect compile output to a file in `/tmp`, then use `tail` and `rg` to analyze errors systematically
- Never filter expected errors - analyze the complete output to understand all issues
- Use this pattern for systematic error analysis:
```shell
mvn test-compile > /tmp/compile.log 2>&1; tail -50 /tmp/compile.log; echo "=== FULL ERRORS ==="; rg "ERROR|error:|cannot find symbol" /tmp/compile.log
```
- **CRITICAL**: Use `2>&1` NOT `2>>1` - the latter is junk syntax that won't capture stderr properly
- Overwrite a single temp file (`/tmp/compile.log`) rather than creating multiple log files in PWD

## Code Formatting Requirements (Spotless)
- **CRITICAL**: All code changes must comply with Google Java Format enforced by Spotless
- **Before any commit**: Always run `mvn spotless:apply` to auto-format code to Google standards
- **For large edits**: Run `mvn spotless:check` to verify formatting compliance before testing
- **CI/CD**: Build will fail if code is not properly formatted - Spotless check runs automatically
- **IDE Integration**: Configure your IDE to use Google Java Format to avoid manual fixes
- **Never disable**: Do not disable or bypass Spotless formatting checks

## API Design Patterns
- Follow MVStore (H2 Database) builder pattern design for create vs open auto-detection
- Builder should automatically handle file existence validation and appropriate constructor selection
- Provide fluent API that eliminates user confusion about create vs open semantics
- Credit MVStore inspiration in javadoc: `/// Builder for creating FileRecordStore instances with a fluent API inspired by H2 MVStore.`

### Builder Sizing Hints (Post PR #86)
The builder provides user-friendly sizing hints that are converted to constructor parameters:
- **hintInitialKeyCount(int)**: Number of initial keys (converted to initial header region size)
- **hintPreferredBlockSize(int)**: Block size in KiB (must be power of 2, converted to bytes)
- **hintPreferredExpandSize(int)**: Expansion size in MiB (converted to bytes)
- **maxKeyLength(int)**: Required parameter - no default, must be 1-32763 bytes

The builder performs these conversions and alignments:
1. Converts KiB/MiB inputs to bytes for constructor
2. Rounds key length + 4-byte CRC up to 8-byte boundaries for alignment
3. Computes optimal initial header region size based on key count and aligned key length
4. Validates block size is power of 2

Implementation ensures SSD-optimized defaults while giving users control over memory layout.

## Use Modern CLI Tools When Available

Check for `rg` and `perl` and other power tools and use them if that makes sense. For example to do mechanical refactors
across many sites perfer things like:

```shell
#  □ Replace Lombok `val` usages with `final var` in tests and sources.
perl -pi -e 's/\bval\b/final var/g' $(rg -l '\bval\b' --glob '*.java')
perl -pi -e 's/^import lombok\.final var;\n//' $(rg -l 'import lombok\.final var;' --glob '*.java')
```

## File Format Specification and Validation

### File Format Structure (v2 - Short Key Length)

| Offset | Size (bytes) | Field | Description | Validation |
|--------|-------------|--------|-------------|------------|
| 0 | 4 | Magic Number | `0xBEEBBEEB` - File format identifier | Must equal `0xBEEBBEEB` or throw `IllegalStateException` |
| 4 | 2 | Key Length | Maximum key length (1-32763) | Range validated, must match constructor parameter |
| 6 | 4 | Record Count | Number of records in store | Must be non-negative, validated against file size |
| 10 | 8 | Data Start Ptr | File offset to start of data region | Must be ≥ header size, validated against file size |
| 18 | - | Index Region | Record headers (key + envelope) | Size = `recordCount * (keyLength + ENVELOPE_SIZE + 5)` |
| Data Start Ptr | - | Data Region | Record data with length prefixes | Each record: 4-byte length + data + optional CRC32 |

**Terminology Clarification:**
- **Record Header**: The complete on-disk structure containing both the key and its envelope (metadata)
- **Envelope**: The fixed 20-byte metadata structure that follows each key in the index, containing data pointer, capacities, count, and CRC32

**Note**: Upgraded from 1-byte to 2-byte key length field to support SHA256/SHA512 hashes (32-64 bytes) and larger keys up to 32KB.

### SSD Optimized Defaults

The store is optimized for modern SSD performance characteristics:

- **Default Key Length**: 128 bytes (supports SHA256/SHA512 hashes)
- **Memory Mapping**: Enabled by default (reduces write amplification)
- **Pre-allocation**: Increased default preallocated records for better sequential performance
- **CRC32**: Enabled by default (SSDs handle checksums efficiently)

### Validation Checks

1. **Magic Number Check**: First 4 bytes must be `0xBEEBBEEB`
   - **Failure**: `IllegalStateException` - "Invalid file format: File does not contain required magic number"
2. **Key Length Validation**: Must be between 1-32763 and match constructor parameter
   - **Failure**: `IllegalArgumentException` - "File has key length X but builder specified Y"
3. **File Size Validation**: File must be large enough for claimed record count
   - **Failure**: `IOException` - "File too small for X records"
4. **Header CRC Validation**: Each key and record header includes CRC32 checksum
   - **Failure**: `IllegalStateException` - "invalid key CRC32" or "invalid header CRC32"
5. **Data CRC Validation**: Optional record payload CRC32 (when enabled)
   - **Failure**: `IllegalStateException` - "CRC32 check failed"

## Logging Policy - CRITICAL

**NEVER DELETE LOGGING LINES** - Once logging is added at the appropriate level (FINE/FINEST), it becomes permanent infrastructure:

- Logging lines are not "temporary debug code" - they are permanent observability features
- Removing logging lines destroys debugging capability for future issues
- If a log level feels wrong, adjust the level, but never remove the line
- All logging must use appropriate levels: FINE for normal debugging, FINEST for detailed tracing
- System properties control visibility - never remove logging to "clean up" output
- Deleting logging lines is considered a destructive act that harms future debugging

## Documentation Is Critical And Is A Stop The World Requirement

If I want a GitHub issue, or an update to docs, or anything else of a "write that down" then you MUST do that IMMEDIATELY. You MUST NOT say "let me finish x" as I may be telling you as the laptop we are on is about to shutdown or that you are about to enter compaction and forget. We do Spec Driven Development and Readme Driven Development and we document first and code second. 
