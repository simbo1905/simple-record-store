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
- Always run the **FULL** test suite with `-Dcom.github.trex_paxos.srs.testLogLevel=FINEST` and redirect output to a single `/tmp` log file (overwrite the same file each run).
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
- Always redirect compile output to a file in `/tmp`, then use `tail` and `rg` to analyse errors systematically
- Never filter expected errors - analyse the complete output to understand all issues
- **CRITICAL**: Use `2>&1` NOT `2>>1` - the latter is junk syntax that won't capture stderr properly
- Overwrite a single temp file (`/tmp/compile.log`) rather than creating multiple log files in PWD

## Use Modern CLI Tools When Available

Check for `rg` and `perl` and other power tools, and use them if that makes sense. For example, to do mechanical refactors
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
2. **Key Length Validation**: Must be between 1-32763 and match the constructor parameter
   - **Failure**: `IllegalArgumentException` - "File has key length X but builder specified Y"
3. **File Size Validation**: File must be large enough for the claimed record count
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

## Documentation Is Critical And Is A Stop-The-World Requirement

If I want a GitHub issue, or an update to the docs, or anything else of a "write that down" then you MUST do that IMMEDIATELY. You MUST NOT say "let me finish x" as I may be telling you, as the laptop we are on is about to shut down or that you are about to enter compaction and forget. We do Spec Driven Development and Readme Driven Development, and we document first and code second.

### Real Crash vs In-Process Corruption vs Test Simulation Of Crashes

**Real Crash (Power Loss, JVM Kill)**:
```
┌─────────────────────┐
│ JVM Running         │
│ - In-memory state   │ ← Complex, may be mid-update when logic throws
│ - Disk state        │ ← Consistent (when JVM+OS does not reorder disk writes)
└─────────────────────┘
         ↓ CRASH (JVM terminates)
┌─────────────────────┐
│ Disk state only     │ ← All in-memory state is LOST or is UNKNOWN 
│ - Committed data OK │
│ - Uncommitted gone  │
└─────────────────────┘
         ↓ Recovery
┌─────────────────────┐
│ New JVM opens file  │
│ - Reads disk only   │ ← Fresh, consistent state
│ - No stale memory   │
└─────────────────────┘
```

**In-Process Corruption Detection**:
```
┌─────────────────────┐
│ JVM Running         │
│ - State check fails │ ← memIndex.size != positionIndex.size
│ - Corruption in RAM │
└─────────────────────┘
         ↓ PANIC (state = UNKNOWN)
┌─────────────────────┐
│ Instance poisoned   │
│ - All ops fail      │ ← Prevent propagation to disk
│ - Must close        │
└─────────────────────┘
         ↓ Recovery
┌─────────────────────┐
│ Reopen file         │
│ - Fresh instance    │ ← Reads good state (if JVM+OS did not reorder disk writes)
│ - Clean state       │
└─────────────────────┘
```

### Key Distinction for Testing

**❌ INCORRECT Test Design**:
```java
// BAD: Expects zombie instance to be operational
store.insertRecord(key1, data);
haltOperations(); // Partial state update
// BUG: Inspecting zombie instance
assertEquals(OPEN, store.getState()); // WRONG - may be UNKNOWN
assertArrayEquals(data, store.readRecordData(key1)); // WRONG - may throw
```

**✅ CORRECT Test Design**:
```java
// GOOD: Models JVM termination
try{
   store.insertRecord(key1, data);
} catch (Exception simulatedIOException ){
   store.terminate(); // This method is marked @TestOnly
   store = null; // Allow object to be GCed and object unreachable
}

// CRASH RECOVERY: Fresh instance, typically a new JVM after a power outage. 
FileRecordStore fresh = new Builder().path(file).open();

// Validate disk consistency only
if (fresh.recordExists(key1)) {
    assertArrayEquals(data, fresh.readRecordData(key1));
}
// Normal termination that flushes all state to disk. 
fresh.close();
```

### State Consistency Checks

**What they protect against**:
1. Programming bugs in state update logic
2. Memory corruption (hardware failures, bit flips)
3. Race conditions (though store uses locks)
4. Exception-induced partial updates

**What they DO NOT validate**:
- ❌ NOT crash recovery (disk will be consistent if JVM+OS does not reorder disk writes)
- ❌ NOT disk corruption (CRC checks handle that separately)
- ❌ NOT JVM and OS disk write ordering (you may need to configure your filesystem, OS and JVM to prevent this)

**State checks verify**: `memIndex.size() == positionIndex.size()`

These two maps are not synchronised as it's the mapping to disk (not each other) that counts:
- `memIndex`: KeyWrapper → RecordHeader (lookup by key)
- `positionIndex`: dataPointer → RecordHeader (lookup by position)
- `memIndex` -> Disk (invalidated upon any IOException or JVM Error)
- `positionIndex` -> Disk (invalidated upon any IOException or JVM Error)

There is no point in using locks to keep two maps in sync when the only way they can get out of sync is if we fail to get a successful confirmation of a write to disk. It is **not** the case that any Exception or Error is a positive confirmation that the disk was not updated and that it is safe to retry. There are dozens of JVM vendors, with many Java versions, running on potentially hundreds of OSes, on potentially tens of thousands of different hardware configurations, on up to a billion devices. 

**If A Mismatch Is Detected**:

```java
if (memIndex.size() != positionIndex.size()) {
    parentStore.state = StoreState.UNKNOWN;
    throw new IllegalStateException(
        "State consistency error: memIndex.size=X != positionIndex.size=Y"
    );
}
```

### Testing Crash Safety: Required Patterns

**Rule 1**: Crash tests MUST model JVM termination, not a gradual shutdown with a successful. 

**Rule 2**: NEVER inspect zombie instances - they may be in UNKNOWN state (expected)

**Rule 3**: Validate ONLY:
- Fresh instance can open successfully
- Committed data is intact
- Uncommitted data is absent

**Rule 4**: Expected SEVERE logs during crash tests:
- "State consistency error" logs are **EXPECTED** in zombie instances
- They prove the fail-fast mechanism works correctly
- Test passes if fresh reopen succeeds

**Example Pattern**:
```java
// Systematic crash simulation
// Create file once at test start
Files.deleteIfExists(file);
Files.createFile(file);

for (int haltPoint = 1; haltPoint <= maxOps; haltPoint++) {
    // File persists across iterations - tests recovery from partial writes
    
    FileRecordStore zombie = createWithHalt(haltPoint);
    try {
        zombie.insertRecord(key, data); // May halt mid-op
    } catch (Exception e) {
        // Expected: halt or state corruption detected
    }
    
    // Wipe zombie to model JVM termination (DO NOT use close())
    try { zombie.wipe(); } catch (Exception ignored) {}
    
    // CRASH RECOVERY: Fresh instance
    FileRecordStore fresh = new Builder().path(file).open();
    
    // Validate disk consistency
    if (fresh.recordExists(key)) {
        assertArrayEquals(data, fresh.readRecordData(key));
    }
    
    fresh.close();
}
```

### Write Ordering Guarantees Crash Safety

Crash safety relies on **write ordering + CRC validation + idempotent recovery**.

This library depends on the JVM and OS to maintain write ordering as directed by the code.
If the JVM or OS reorders writes without the library knowing, crash safety cannot be guaranteed.
Historically, some filesystems had bugs with write reordering that caused corruption under power loss.
Such issues affect all software on those systems and must be fixed at the OS/filesystem level.

We cannot predict or work around OS-level bugs - we can only eliminate logical bugs in our code.
The implementation assumes writes reach disk in the order requested and that failures are reported correctly.

**Insert/Update commit points**:
1. Data written to new location (uncommitted)
2. Record header written to index (uncommitted - may be torn on crash)
3. `numRecords` incremented (COMMIT POINT - makes record visible)

**Update in-place semantics**:
1. New data written over old data (may be torn on crash)
2. Record header CRC recomputed (idempotent - can repeat safely)
3. Recovery validates CRC on every read - torn writes rejected

**Expansion commit points**:
1. File extended via `setLength()` (filesystem operation)
2. Record data copied to new location (uncommitted)
3. Record header updated in index (uncommitted - may be torn)
4. `dataStartPtr` updated (COMMIT POINT - makes new layout visible)

**Crash safety guarantees**:
- **Torn writes are safe**: Uncommitted headers have invalid CRC → ignored on recovery
- **Partial data is safe**: Data without valid header in `numRecords` → invisible on recovery
- **CRC validation**: Catches torn writes and corruption on every read
- **Idempotent recovery**: Multiple crashes during same operation → same final state

**Write ordering requirements**:
- We rely on JVM file I/O APIs maintaining write order
- `sync()` flushes buffered writes to OS (durability, not atomicity)
- `setLength()` updates file metadata (filesystem operation)
- Sector writes (512B/4KB) may be atomic, but not guaranteed beyond sector boundaries
- Write reordering without notification would break crash safety assumptions

### Source Code References

**State transition to UNKNOWN** (fail-fast on corruption):
- `FileRecordStore.State` inner class methods
- Pattern: detect mismatch → `parentStore.state = UNKNOWN` → throw exception

**Commit point operations**:
- `FileRecordStore.writeNumRecordsHeader()` - Insert/delete commit
- `FileRecordStore.writeDataStartPtrHeader()` - Expansion commit

**Write ordering enforcement**:
- `FileRecordStore.addEntryToIndex()` - Insert: data → header → numRecords increment
- `FileRecordStore.allocateRecordAfterCompaction()` - Expansion: extend file → copy data → update dataStartPtr

**CRC validation**:
- `RecordHeader.computeCrc32()` - Header CRC computation
- `RecordHeader.readFrom()` - Header CRC validation on read
- `FileRecordStore.readRecordData()` - Payload CRC validation

**Test-only crash simulation**:
- `FileRecordStore.wipe()` - Package-private method with `@TestOnly` annotation
- Models JVM termination: sync → close → clear/null state → CLOSED
- `HeaderExpansionCrashTest` - Uses `wipe()` not `close()` for crash testing

**Payload CRC configuration**:
- `FileRecordStoreBuilder.disablePayloadCrc32()` - Disable for performance (NOT recommended for production)
- Default: CRC enabled for all record payloads
- Header CRC: ALWAYS enabled (cannot be disabled)

### Fail-Fast Prevents Corruption Propagation

**Without fail-fast**:
```
Corruption detected → Continue operation → Write to disk → DISK CORRUPTED
```

**With fail-fast**:
```
Corruption detected → state = UNKNOWN → All ops fail → Disk protected
```

**Recovery path**:
```
Reopen file → Fresh read from disk → Disk was fine → Success
          OR → Disk actually corrupt → CRC failures → Report true error
```

This design ensures:
- In-memory corruption NEVER reaches disk
- Disk corruption detected via CRC, not state checks
- Client forced to get fresh state from authoritative source (disk)
 
