# FIFO Wrapper Design for SRS Library

## Overview

This document outlines the design for a FIFO (First-In-First-Out) wrapper around the Simple Record Store (SRS) library. The wrapper provides queue semantics with high/low water markers, timestamps, and batching capabilities for single-threaded actor/isolate environments.

## Architecture

### Core Components

1. **FifoRecordStore** - Main wrapper class providing FIFO semantics
2. **Genesis Record** - Special metadata record tracking queue state
3. **Key Structure** - 128-bit keys combining counter and timestamp
4. **Buffering System** - Batching mechanism for performance optimization

## Key Structure Design

### Primary Key Format (128-bit)
```
|-- 64 bits --|-- 64 bits --|
| Counter      | Timestamp   |
```

- **Counter (64-bit)**: Monotonic sequence number ensuring FIFO ordering
- **Timestamp (64-bit)**: Milliseconds since epoch for delay analysis
- **Total**: 16 bytes (128 bits) - optimal for SRS UUID mode

### Enhanced "Fat Key" Format (Alternative)
```
|-- 64 bits --|-- 64 bits --|-- 32 bits --|-- 32 bits --|
| Counter      | Timestamp   | Total Put   | Total Take  |
```

- Embeds statistics directly in key for crash recovery
- 24 bytes total - requires BYTE_ARRAY mode
- Enables stateless recovery by scanning all keys

## Genesis Record Design

### Sentinel Key
- **Key**: All zeros (16 bytes for UUID mode, or special sentinel pattern)
- **Purpose**: Store queue metadata and statistics

### Genesis Record Payload (JSON)
```json
{
  "version": 1,
  "highWaterMark": 1000,
  "lowWaterMark": 100,
  "lastPutTime": 1698765432000,
  "lastTakeTime": 1698765431000,
  "totalPutCount": 5000,
  "totalTakeCount": 4900,
  "nextCounter": 5001,
  "queueSize": 100
}
```

### Metadata Fields
- **highWaterMark**: Maximum queue size reached
- **lowWaterMark**: Minimum queue size (for monitoring)
- **lastPutTime**: Timestamp of last enqueue operation
- **lastTakeTime**: Timestamp of last dequeue operation
- **totalPutCount**: Total items ever enqueued
- **totalTakeCount**: Total items ever dequeued
- **nextCounter**: Next counter value to use
- **queueSize**: Current number of items in queue

## FIFO Wrapper API

### Core Operations

```java
public class FifoRecordStore implements AutoCloseable {
    
    // Constructor
    public FifoRecordStore(Path filePath, int maxKeyLength) throws IOException
    
    // FIFO Operations
    public void put(byte[] data) throws IOException
    public byte[] take() throws IOException
    public byte[] peek() throws IOException
    public boolean isEmpty() throws IOException
    public int size() throws IOException
    
    // Monitoring
    public FifoStats getStats() throws IOException
    public long getHighWaterMark() throws IOException
    public long getLowWaterMark() throws IOException
    public long getAverageDelay() throws IOException
    
    // Batching
    public void putBatch(List<byte[]> items) throws IOException
    public List<byte[]> takeBatch(int maxItems) throws IOException
    public void flush() throws IOException
    
    // Configuration
    public void setBufferSize(int size)
    public void setFlushInterval(Duration interval)
    public void enableAutoFlush(boolean enable)
}
```

### Statistics Class

```java
public class FifoStats {
    private final long highWaterMark;
    private final long lowWaterMark;
    private final long lastPutTime;
    private final long lastTakeTime;
    private final long totalPutCount;
    private final long totalTakeCount;
    private final long currentSize;
    private final long averageDelay;
    
    // Getters and utility methods
}
```

## Internal Implementation

### Key Generation Strategy

```java
private byte[] generateKey() {
    long counter = getNextCounter();
    long timestamp = System.currentTimeMillis();
    
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(counter);
    buffer.putLong(timestamp);
    return buffer.array();
}
```

### Counter Management
- Monotonic 64-bit counter ensures strict FIFO ordering
- Counter persisted in genesis record
- Atomic increment on each put operation
- Handles counter overflow (wraps to 0 after Long.MAX_VALUE)

### Queue Operations

#### Put Operation Flow
1. Generate new key (counter + timestamp)
2. Buffer the item if batching enabled
3. Write item record to store
4. Update genesis record with new statistics (commit point)
5. Update in-memory statistics

#### Take Operation Flow
1. Find smallest counter key in store
2. Read and return the data
3. Delete the record from store
4. Update genesis record with new statistics
5. Update in-memory statistics

### Crash Recovery

#### Stateless Recovery (Fat Key Approach)
```java
private void recoverState() throws IOException {
    // Scan all keys to rebuild state
    Set<byte[]> allKeys = new HashSet<>();
    for (byte[] key : store.keysBytes()) {
        if (!isGenesisKey(key)) {
            allKeys.add(key);
        }
    }
    
    // Rebuild statistics from keys
    long maxCounter = 0;
    long minTimestamp = Long.MAX_VALUE;
    long maxTimestamp = 0;
    
    for (byte[] key : allKeys) {
        FifoKey fifoKey = FifoKey.fromBytes(key);
        maxCounter = Math.max(maxCounter, fifoKey.counter);
        minTimestamp = Math.min(minTimestamp, fifoKey.timestamp);
        maxTimestamp = Math.max(maxTimestamp, fifoKey.timestamp);
    }
    
    // Update genesis record with recovered state
    updateGenesisRecord(maxCounter + 1, allKeys.size(), maxTimestamp);
}
```

#### Genesis-Based Recovery (Simple Key Approach)
```java
private void recoverState() throws IOException {
    try {
        // Try to read genesis record
        byte[] genesisData = store.readRecordData(GENESIS_KEY);
        this.stats = parseGenesisRecord(genesisData);
    } catch (IllegalArgumentException e) {
        // Genesis record doesn't exist - initialize new queue
        initializeNewQueue();
    }
}
```

## Buffering and Batching System

### Buffer Management

```java
public class FifoBuffer {
    private final List<BufferedItem> putBuffer = new ArrayList<>();
    private final List<byte[]> takeBuffer = new ArrayList<>();
    private final int maxBufferSize;
    private final Duration flushInterval;
    private volatile boolean autoFlush = true;
    
    private static class BufferedItem {
        final byte[] data;
        final long timestamp;
        
        BufferedItem(byte[] data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
```

### Batching Strategies

#### Time-Based Batching
- Flush buffer after specified time interval
- Background timer thread for automatic flushing
- Configurable flush interval (default: 100ms)

#### Size-Based Batching
- Flush when buffer reaches maximum size
- Configurable buffer size (default: 100 items)
- Immediate flush on buffer overflow

#### Explicit Batching
- Manual flush() calls from application
- Synchronous flush command from UI isolate
- Useful for transaction boundaries

### Batch Write Implementation

```java
public void flushPutBuffer() throws IOException {
    if (putBuffer.isEmpty()) return;
    
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    
    // Generate keys and prepare batch
    for (BufferedItem item : putBuffer) {
        byte[] key = generateKey();
        keys.add(key);
        values.add(item.data);
    }
    
    // Write all items first
    for (int i = 0; i < keys.size(); i++) {
        store.insertRecord(keys.get(i), values.get(i));
    }
    
    // Update genesis record AFTER all items are written successfully
    // This ensures atomicity: if crash occurs during item writes,
    // genesis record still reflects the old state
    updateGenesisRecordBatch(keys.size());
    
    // Clear buffer
    putBuffer.clear();
}
```

## Performance Considerations

### SRS Configuration
- Use UUID key mode for 16-byte keys (optimal performance)
- Enable memory mapping for better I/O performance
- Configure appropriate block size (4KB-8KB for SSDs)
- Pre-allocate sufficient header space

### Memory Management
- Bounded buffer sizes to prevent OOM
- Efficient key generation without allocations
- Reuse ByteBuffer instances where possible

### Concurrency Model
- Single-threaded design for actor/isolate environments
- No internal locking required
- Thread-safe statistics access via volatile fields

## Error Handling and Reliability

### Crash Safety
- **Write Ordering**: Items are written to SRS BEFORE updating genesis record
  - If crash occurs during item writes, genesis record remains consistent (unchanged)
  - If crash occurs after all items written but before genesis update, items are orphaned but system is still consistent
  - Genesis record acts as the commit point for batch operations
- **Atomicity Guarantee**: Genesis record update is atomic (single SRS write operation)
- **SRS Integration**: Leverages SRS crash-safe guarantees for individual record writes
- **Recovery**: Rebuilds consistent state from disk using genesis record as source of truth
- **Orphan Detection**: Recovery can optionally scan for and reclaim orphaned records (keys not referenced by genesis counter range)

### Error Recovery
```java
public void handleCorruption() throws IOException {
    try {
        // Attempt normal recovery
        recoverState();
    } catch (Exception e) {
        // Fallback to full scan recovery
        logger.warn("Genesis record corrupted, performing full recovery", e);
        recoverStateFromFullScan();
    }
}
```

### Monitoring and Diagnostics
- Comprehensive statistics tracking
- Delay analysis for performance monitoring
- Queue depth monitoring for capacity planning
- Error counters for reliability metrics

## Configuration Options

### Builder Pattern
```java
FifoRecordStore fifo = new FifoRecordStoreBuilder()
    .path("/path/to/queue.db")
    .maxKeyLength(16)  // UUID mode
    .bufferSize(1000)
    .flushInterval(Duration.ofMillis(100))
    .enableAutoFlush(true)
    .enableStatistics(true)
    .build();
```

### Runtime Configuration
- Dynamic buffer size adjustment
- Flush interval modification
- Statistics collection toggle
- Auto-flush enable/disable

## Testing Strategy

### Unit Tests
- Key generation and ordering
- Genesis record serialization
- Buffer management
- Statistics calculation

### Integration Tests
- End-to-end FIFO operations
- Crash recovery scenarios
- Performance benchmarks
- Concurrent access patterns

### Crash Tests
- Simulate power loss during operations
- Verify recovery consistency
- Test genesis record corruption handling
- Validate queue ordering after recovery

## Migration and Compatibility

### Version Management
- Genesis record includes version field
- Forward/backward compatibility handling
- Migration utilities for format changes

### Upgrade Path
- In-place upgrades where possible
- Export/import for major format changes
- Validation tools for data integrity

## Future Enhancements

### Potential Features
- Priority queues with multiple priority levels
- Queue compaction for long-running queues
- Distributed queue coordination
- Queue mirroring and replication
- Advanced monitoring and metrics

### Performance Optimizations
- Bulk operations for large batches
- Asynchronous I/O integration
- Memory-mapped queue headers
- Custom serialization formats

## Implementation Phases

### Phase 1: Basic FIFO
- Core put/take operations
- Simple key structure
- Genesis record management
- Basic statistics

### Phase 2: Buffering
- Time and size-based batching
- Flush operations
- Buffer management
- Performance optimization

### Phase 3: Advanced Features
- Crash recovery
- Statistics and monitoring
- Configuration management
- Comprehensive testing

### Phase 4: Production Hardening
- Error handling
- Diagnostics and logging
- Performance tuning
- Documentation and examples

## Conclusion

This design provides a robust, crash-safe FIFO wrapper around the SRS library with the following key benefits:

1. **Strict FIFO Ordering**: Counter-based keys ensure proper ordering
2. **Crash Safety**: Leverages SRS crash-safe guarantees
3. **Performance**: Batching and buffering for high throughput
4. **Monitoring**: Comprehensive statistics and delay analysis
5. **Simplicity**: Clean API suitable for actor/isolate environments
6. **Reliability**: Robust error handling and recovery mechanisms

The design balances simplicity with functionality, providing a production-ready queue implementation that can handle the demands of modern distributed systems while maintaining the reliability guarantees of the underlying SRS library.