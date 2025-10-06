# Repository Guidelines

## Coding Style & Naming Conventions
- Use `mvn spotless:check` and `apply` for formatting of files. 

## Commit & Pull Request Guidelines
- Write present-tense, 72-character subjects (`Update pom.xml to require Java 21`); add context or links in the body when needed.
- Reference related issues and describe crash-safety implications or new coverage in PR descriptions.
- Rebase onto the latest mainline so CI reflects the final diff; attach screenshots only when they clarify tooling output.

## Documentation Standards
- Use JEP 467 documentation format with `///` triple-slash comments instead of traditional `/**` javadoc style
- Place documentation on the line immediately above the element being documented
- Example: `/// Returns the file path of this record store.` followed by `public Path getFilePath()`

### Unified Logging Workflow
- Always run the test suite with `-Dcom.github.trex_paxos.srs.testLogLevel=FINEST` and redirect output to a single `/tmp` log file (overwrite the same file each run).
- Inspect the log with `rg 'INFO|ERROR' /tmp/<logfile>` to verify every test emits the `@Before` INFO banner (see `src/test/java/com/github/simbo1905/nfp/srs/JulLoggingConfig.java`) and to spot failing tests and line numbers quickly.
- Once failing tests are identified, reuse the same log file filtered with `rg 'INFO|ERROR|FINE'` to follow the execution path; `FINE` entries document method-level flow decisions.
- Apply additional focused filters such as `rg 'INFO|ERROR|FINE|FINER'` or `rg 'INFO|ERROR|FINE|FINER|FINEST'` on that same file to zoom into deeper state transitions without rerunning the suite.
- This single-run workflow lets you debug multiple tests at progressively finer granularity without regenerating logs.

### Comprehensive Debug Logging Guidelines

**More logging is better** - detailed logging is essential for debugging complex state issues:

1. **Always include current record state**: Every debug log line must include the current RecordHeader state
2. **Use consistent format**: Use `toDebugString(RecordHeader, data, showData)` for standardized output
3. **Log level checking**: Always check `logger.isLoggable(Level.FINEST)` before expensive debug operations
4. **State change logging**: Use `logStateChange()` to track all header/index map updates
5. **Consistency checks**: Use `logConsistencyCheck()` to verify memory vs disk state matches
6. **Method entry/exit**: Log method entry with current state and exit with final state
7. **Path identification**: Clearly identify which code path is being taken (INPLACE, EXTEND, ALLOCATE, MOVE)

**Logging Level Hierarchy**:
- **FINE**: Method entry/exit with parameters and return values - shows major method flow
- **FINER**: Branch decisions within methods - shows which code paths are taken
- **FINEST**: Detailed state changes and internal operations - shows exactly what happened

**Example debug format**:
```
FINE  updateRecord: ENTER key=[0x01...0x20], value.length=512
FINER updateRecordInternal: INPLACE current=RecordHeader[dp=160,dl=26,dc=38,ip=0,crc=73275e82]
FINEST updateRecordInternal: writeRecordData fp=160, len=520, data=[0x00...0xFF]
STATE updateRecordInternal: key=[0x01...0x20], old=RecordHeader[dp=160,dl=26,dc=38,ip=0,crc=73275e82], new=RecordHeader[dp=928,dl=512,dc=524,ip=0,crc=e5eeac5b]
```

8. **Debug helper methods**: Use `final` debug methods to encourage JIT inlining and reduce overhead when disabled
9. **Never delete debug logging**: Once added, debug logging becomes permanent infrastructure for future debugging

## Build Output Analysis
- Always redirect compile output to a file in `/tmp`, then use `tail` and `rg` to analyze errors systematically
- Never filter expected errors - analyze the complete output to understand all issues
- **CRITICAL**: Use `2>&1` NOT `2>>1` - the latter is junk syntax that won't capture stderr properly
- Overwrite a single temp file (`/tmp/compile.log`) rather than creating multiple log files in PWD

## Use Modern CLI Tools When Available

Check for `rg` and `perl` and other power tools and use them if that makes sense. For example to do mechanical refactors
across many sites prefer things like:

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
