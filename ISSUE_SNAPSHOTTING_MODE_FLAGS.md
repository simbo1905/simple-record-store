# Snapshotting Mode: Sequential Scan Optimization with Concurrent Operations

## Overview

This issue builds upon issue #70 ("use read/write lock to allow snapshot parallel copy/compact") and the newly implemented in-place update toggle to provide a comprehensive snapshotting mode that enables fast, safe sequential scanning while allowing concurrent appends and reads.

## Problem Statement

Current snapshotting approaches face several challenges:

1. **Header Region Expansion**: During snapshotting, the key/header region can expand, moving records from the area being scanned
2. **Record Movement**: In-place updates can write to pages elsewhere in the memory-mapped file while snapshotting is trying to sequentially walk the memory space
3. **Concurrent Safety**: Need to allow concurrent appends and reads while preventing modifications that would corrupt the sequential scan

## Proposed Solution: Snapshotting Mode with Dual Flags

### Flag 1: In-Place Update Control (Already Implemented)
- **Purpose**: Prevent record movement during snapshotting
- **Implementation**: `setAllowInPlaceUpdates(boolean)` - already implemented in previous commit
- **Usage**: Disable during snapshotting to force append-only behavior for updates

### Flag 2: Header Region Expansion Control (NEW)
- **Purpose**: Prevent header region expansion during snapshotting
- **Implementation**: New flag to disable index/header expansion
- **Usage**: Disable during snapshotting to maintain stable memory layout

## Technical Requirements

### Sequential Scan Safety
For safe sequential snapshotting, we need to prevent:
1. **Header expansion** that would move records being scanned
2. **In-place updates** that could write to pages outside the scan area
3. **Record movement** that would invalidate the sequential walk

### Concurrent Operations
During snapshotting, we want to allow:
1. **Concurrent appends** - new records can be added
2. **Concurrent reads** - existing records can be read
3. **Memory-mapped safety** - prevent writes to pages being scanned

### SSD Optimization
- Use larger blocks (16KB+) that match physical SSD sizes
- UUID keys (16 bytes) with 16KB header space = fast operations
- Large header pre-allocation reduces expansion needs during snapshotting

## Implementation Plan

### Phase 1: Header Region Expansion Control

Add new field and methods:
```java
/// Whether to allow header region expansion during operations
private volatile boolean allowHeaderExpansion = true;

/// Sets whether to allow header region expansion
public void setAllowHeaderExpansion(boolean allow) {
    ensureOpen();
    ensureNotReadOnly();
    this.allowHeaderExpansion = allow;
}

/// Returns whether header region expansion is allowed
public boolean isAllowHeaderExpansion() {
    return allowHeaderExpansion;
}
```

### Phase 2: Modify Header Expansion Logic

Update `ensureIndexSpace()` method to respect the flag:
```java
private void ensureIndexSpace(int requiredNumRecords) throws IOException {
    if (!allowHeaderExpansion) {
        // Check if we have enough pre-allocated space
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if (endIndexPtr > dataStartPtr) {
            throw new IllegalStateException(
                "Header expansion disabled and insufficient pre-allocated space. " +
                "Required: " + requiredNumRecords + " records, but only " + 
                getNumRecords() + " slots available.");
        }
        return;
    }
    // Existing expansion logic...
}
```

### Phase 3: Builder Integration

Add builder option:
```java
public Builder allowHeaderExpansion(boolean allow) {
    this.allowHeaderExpansion = allow;
    return this;
}
```

## Usage Pattern for Snapshotting

```java
// Enter snapshotting mode
store.setAllowInPlaceUpdates(false);        // Prevent record movement
store.setAllowHeaderExpansion(false);       // Prevent header expansion

// Perform sequential snapshot
for (int i = 0; i < store.size(); i++) {
    KeyWrapper key = store.readKeyFromIndex(i);
    byte[] data = store.readRecordData(key.bytes());
    // Process data sequentially...
}

// Exit snapshotting mode
store.setAllowInPlaceUpdates(true);         // Re-enable in-place updates
store.setAllowHeaderExpansion(true);        // Re-enable header expansion
```

## Benefits

1. **Sequential Safety**: Prevents record movement during sequential scans
2. **Concurrent Operations**: Allows appends and reads during snapshotting
3. **Memory-Mapped Safety**: Prevents writes to pages being scanned
4. **Performance**: Optimized for SSD block sizes and sequential access
5. **Flexibility**: Can be toggled on/off for different operational modes

## Integration with Issue #70

This solution directly supports issue #70 ("use read/write lock to allow snapshot parallel copy/compact") by providing:

1. **Read/Write Lock Compatibility**: Operations can proceed concurrently with proper locking
2. **Parallel Copy Safety**: Prevents modifications that would corrupt parallel copying
3. **Compact Operation Support**: Enables safe compaction by controlling when moves occur

## SSD Optimization Strategy

### Block Size Alignment
- Use 16KB blocks to match common SSD page sizes
- Align header and data regions to block boundaries
- Minimize write amplification through proper alignment

### Pre-allocation Strategy
```java
// Pre-allocate sufficient header space for snapshotting
FileRecordStore store = new FileRecordStore.Builder()
    .path(databasePath)
    .preallocatedRecords(10000)  // Large header space
    .maxKeyLength(16)            // UUID keys = fast headers
    .allowHeaderExpansion(false) // Start in snapshot-ready mode
    .open();
```

## Error Handling

When snapshotting mode is active and space runs out:
- **Header expansion disabled**: Throw `IllegalStateException` with clear message
- **In-place updates disabled**: Force dual-write pattern for smaller records
- **Pre-allocation insufficient**: Guide user to increase pre-allocation or enable expansion

## Backward Compatibility

Both flags default to `true` (current behavior), ensuring:
- Existing code continues to work without modification
- New behavior is opt-in for specific use cases
- Clear documentation of trade-offs and use cases

## Future Enhancements

1. **Automatic Mode Detection**: Detect snapshotting patterns and auto-enable flags
2. **Metrics**: Track snapshotting performance and space utilization
3. **Adaptive Pre-allocation**: Dynamically adjust pre-allocation based on usage patterns
4. **Block Size Configuration**: Allow custom block size alignment for different storage media

## Conclusion

This dual-flag approach provides a robust solution for snapshotting that:
- Enables safe sequential scanning
- Maintains concurrent operation support
- Optimizes for modern SSD storage
- Integrates seamlessly with existing issue #70
- Provides clear operational modes for different use cases

The implementation gives users fine-grained control over storage behavior while maintaining the crash-safety guarantees that make FileRecordStore reliable.

## Architectural Context

This implementation introduces the first runtime-mutable operational mode toggles in FileRecordStore, creating a distinction between:

- **Configuration**: Immutable characteristics set at construction (maxKeyLength, keyType, etc.)
- **Operational Mode**: Runtime behavioral switches for specific operations (in-place updates, header expansion)

The volatile fields `allowInPlaceUpdates` and `allowHeaderExpansion` enable dynamic switching between operational modes without store reconstruction. This architectural pattern supports advanced use cases like snapshotting while maintaining the existing immutable configuration model for permanent store characteristics.

These toggles represent a departure from the constructor-time certainty pattern, but are justified as operational mode switches rather than permanent configuration choices."}   