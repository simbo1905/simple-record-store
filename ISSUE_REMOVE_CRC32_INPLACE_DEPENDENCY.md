# Remove CRC32 Dependency for In-Place Updates

## Problem Statement

Currently, the FileRecordStore prevents in-place updates for smaller records when CRC32 checking is disabled. This creates an artificial dependency between data integrity checking and storage efficiency that doesn't align with modern storage realities.

### Current Behavior

- **Same-size records**: Can always be updated in-place (regardless of CRC32 setting)
- **Smaller records**: Can only be updated in-place if CRC32 is enabled
- **Larger records**: Never updated in-place (always use dual-write pattern)

The current logic in `updateRecord()` methods (lines 1241, 741):
```java
final var recordIsSmallerAndCrcEnabled = !disableCrc32 && value.length < capacity;
if (recordIsSameSize || recordIsSmallerAndCrcEnabled) {
    // IN-PLACE UPDATE PATH
} else {
    // DUAL-WRITE/MOVE PATH
}
```

## Rationale for Change

### Modern Storage Realities
1. **SSD Block Sizes**: Modern SSDs use large block sizes (16KB+) making partial writes extremely unlikely
2. **Memory-Mapped Files**: OS guarantees about memory-mapped file consistency provide additional safety
3. **Low Corruption Risk**: The combination of SSD reliability and OS guarantees makes CRC32-less in-place updates safe

### User Choice for Integrity
1. **Higher-Level Integrity**: Users with large data can implement compression (which includes integrity checks)
2. **Application-Level Validation**: Applications can implement their own integrity mechanisms
3. **Performance vs Safety Trade-off**: Users should be able to choose performance over mandatory CRC32 validation

### Use Case: Append-Only Mode for Snapshots
This change supports the planned feature for snapshotting (referenced in issue #70: "use read/write lock to allow snapshot parallel copy/compact"):
- **Snapshot Mode**: Temporarily disable in-place updates during snapshotting
- **Memory-Mapped Safety**: Prevent record movement while walking memory space
- **SSD Alignment**: Use 16KB+ aligned blocks that match physical SSD sizes

## Proposed Solution

### 1. Add Public Toggle for In-Place Updates
Add a new builder option and public setter:
```java
public Builder allowInPlaceUpdates(boolean allow) {
    this.allowInPlaceUpdates = allow;
    return this;
}

public void setAllowInPlaceUpdates(boolean allow) {
    ensureNotReadOnly();
    this.allowInPlaceUpdates = allow;
}
```

### 2. Modify Update Logic
Change the in-place update decision logic:
```java
final var recordIsSameSize = value.length == capacity;
final var recordIsSmaller = value.length < capacity;

// Allow in-place updates based on setting, not CRC32 requirement
if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
    // IN-PLACE UPDATE PATH
} else {
    // DUAL-WRITE/MOVE PATH
}
```

### 3. Maintain Backward Compatibility
- Default `allowInPlaceUpdates = true` (new behavior)
- Preserve existing behavior for users who explicitly want CRC32-dependent updates
- Document the trade-offs clearly

### 4. Support Snapshot Use Case
For the snapshotting feature mentioned in issue #70:
```java
// During snapshotting
tempStore.setAllowInPlaceUpdates(false);
// ... perform snapshot operations ...
tempStore.setAllowInPlaceUpdates(true);
```

## Benefits

1. **Performance**: Eliminates unnecessary record movement for smaller updates
2. **User Choice**: Allows users to trade integrity checking for performance
3. **Modern Alignment**: Better aligns with SSD block sizes and memory-mapped file behavior
4. **Snapshot Support**: Enables append-only mode for safe snapshotting
5. **Simplified Logic**: Removes artificial coupling between integrity and storage strategy

## Implementation Considerations

### Alternative Length Validation
When CRC32 is disabled and in-place updates are allowed for smaller records:
1. **Trust Length Field**: Rely on the length field without additional validation
2. **Read-Time Validation**: Implement length bounds checking during reads
3. **Padding Strategy**: Use clear padding patterns for unused space

### Documentation Updates
- Document the new behavior and trade-offs
- Explain when to use/avoid in-place updates without CRC32
- Reference the snapshotting use case
- Provide guidance on SSD-aligned block sizes

## Backward Compatibility

This change maintains backward compatibility:
- Existing code continues to work without modification
- New behavior (allowing in-place updates) becomes the default
- Users can opt into the old behavior if needed for specific use cases

## Related Issues

- Issue #70: "use read/write lock to allow snapshot parallel copy/compact" - This change supports the snapshotting feature by enabling append-only mode
- Future enhancement: Add SSD-aligned block size configuration for optimal performance

## Architectural Context

This implementation represents the first introduction of runtime-mutable operational mode toggles in FileRecordStore, creating a distinction between:

- **Configuration**: Immutable characteristics set at construction (maxKeyLength, keyType, etc.)
- **Operational Mode**: Runtime behavioral switches for specific operations (in-place updates, header expansion)

The `setAllowInPlaceUpdates(boolean)` method is a volatile field that enables dynamic switching between storage strategies without store reconstruction. This architectural pattern supports advanced use cases like snapshotting while maintaining the existing immutable configuration model for permanent store characteristics.

## Conclusion

Removing the CRC32 dependency for in-place updates modernizes the storage engine, provides better performance options, and enables advanced features like snapshotting while maintaining user choice for integrity validation.