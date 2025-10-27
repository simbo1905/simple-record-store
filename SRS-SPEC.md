# Simple Record Store (SRS) Specification

A language-agnostic specification for a crash-safe, random-access record store with multiple storage backends.

## Overview

The Simple Record Store (SRS) is designed to provide crash-safe storage of key-value records with the following characteristics:

- **Crash Safety**: Survives power loss and system crashes
- **Random Access**: O(1) key-based record retrieval
- **Efficient Storage**: Minimal overhead with CRC32 validation
- **Platform Agnostic**: Works across Java, Dart, C++, and other languages
- **Scalable**: Supports files up to 2^63 bytes

## File Format Specification

### Header Structure (Fixed 18 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0xBEEBBEEB` - File format identifier |
| 4 | 2 | Key Length | Maximum key length (1-32763 bytes) |
| 6 | 4 | Record Count | Number of active records |
| 10 | 8 | Data Start Ptr | File offset to start of data region |

### Index Region Structure

Each record in the index consists of:
- **Key**: Variable length (1 to Key Length bytes)
- **Envelope**: Fixed 20 bytes containing metadata

#### Envelope Structure (20 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Data Pointer | File offset to record data |
| 8 | 4 | Data Length | Length of record data |
| 12 | 4 | Data Capacity | Allocated space for record data |
| 16 | 4 | CRC32 | CRC32 checksum of key + envelope |

### Data Region Structure

Each record in the data region consists of:
- **Length Prefix**: 4 bytes (little-endian)
- **Data**: Variable length record payload
- **CRC32**: 4 bytes (optional, configurable)

## Crash Safety Guarantees

### Write Ordering Requirements

The implementation must ensure the following write order for crash safety:

1. **Record Insert**:
   - Write data to data region
   - Write key + envelope to index region
   - Increment record count (COMMIT POINT)

2. **Record Update**:
   - Write new data (in-place or new location)
   - Update envelope in index region
   - CRC32 validation on read

3. **Record Delete**:
   - Mark record as deleted in envelope
   - Decrement record count (COMMIT POINT)

### CRC32 Validation

- **Header CRC**: Always enabled, validates key + envelope integrity
- **Data CRC**: Optional, configurable per implementation
- **Recovery**: Invalid CRC indicates torn write, record is ignored

## Sequence Diagrams

### Record Insert Operation

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant Index as Index Region
    participant Data as Data Region
    participant FS as File System

    App->>SRS: insertRecord(key, data)
    SRS->>Data: allocateSpace(data.length)
    Data-->>SRS: dataPointer
    SRS->>Data: writeData(dataPointer, data)
    Note over Data: Data written to disk
    SRS->>Index: writeKey(key)
    SRS->>Index: writeEnvelope(dataPointer, length, capacity, crc32)
    Note over Index: Key + envelope written
    SRS->>SRS: incrementRecordCount()
    Note over SRS: COMMIT POINT - Record visible
    SRS-->>App: success
```

### Record Update Operation

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant Index as Index Region
    participant Data as Data Region
    participant FS as File System

    App->>SRS: updateRecord(key, newData)
    SRS->>Index: findRecord(key)
    Index-->>SRS: envelope(dataPointer, length, capacity)
    
    alt Data fits in existing space
        SRS->>Data: writeData(dataPointer, newData)
        Note over Data: In-place update
    else Data needs more space
        SRS->>Data: allocateSpace(newData.length)
        Data-->>SRS: newDataPointer
        SRS->>Data: writeData(newDataPointer, newData)
        SRS->>Data: freeSpace(dataPointer)
    end
    
    SRS->>Index: updateEnvelope(newDataPointer, newLength, newCapacity, newCrc32)
    Note over Index: Envelope updated
    SRS-->>App: success
```

### Record Delete Operation

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant Index as Index Region
    participant Data as Data Region
    participant FS as File System

    App->>SRS: deleteRecord(key)
    SRS->>Index: findRecord(key)
    Index-->>SRS: envelope(dataPointer, length, capacity)
    SRS->>Data: freeSpace(dataPointer)
    Note over Data: Space marked as free
    SRS->>Index: markDeleted(key)
    Note over Index: Record marked as deleted
    SRS->>SRS: decrementRecordCount()
    Note over SRS: COMMIT POINT - Record removed
    SRS-->>App: success
```

### Data Area Expansion

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant Index as Index Region
    participant Data as Data Region
    participant FS as File System

    App->>SRS: insertRecord(key, largeData)
    SRS->>Data: checkAvailableSpace(largeData.length)
    Data-->>SRS: insufficient space
    
    SRS->>FS: extendFile(newSize)
    Note over FS: File extended via setLength()
    SRS->>Data: updateDataStartPtr(newDataStart)
    Note over Data: Data region expanded
    SRS->>Data: allocateSpace(largeData.length)
    Data-->>SRS: dataPointer
    SRS->>Data: writeData(dataPointer, largeData)
    SRS->>Index: writeKey(key)
    SRS->>Index: writeEnvelope(dataPointer, length, capacity, crc32)
    SRS->>SRS: incrementRecordCount()
    SRS-->>App: success
```

### Header Space Expansion

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant Index as Index Region
    participant Data as Data Region
    participant FS as File System

    App->>SRS: insertRecord(longKey, data)
    SRS->>Index: checkKeyLength(longKey.length)
    Index-->>SRS: key too long for current header space
    
    SRS->>FS: extendFile(newSize)
    Note over FS: File extended
    SRS->>Data: moveDataRegion(newDataStart)
    Note over Data: Data region moved to make room
    SRS->>Index: expandIndexRegion(newIndexSize)
    Note over Index: Index region expanded
    SRS->>Index: writeKey(longKey)
    SRS->>Index: writeEnvelope(dataPointer, length, capacity, crc32)
    SRS->>SRS: incrementRecordCount()
    SRS-->>App: success
```

## Synchronization and Durability

### File Sync Requirements

The SRS specification requires applications to call `sync()` for crash durability:

```mermaid
sequenceDiagram
    participant App as Application
    participant SRS as SRS Store
    participant OS as Operating System
    participant Disk as Storage Device

    App->>SRS: insertRecord(key1, data1)
    App->>SRS: insertRecord(key2, data2)
    App->>SRS: insertRecord(key3, data3)
    Note over SRS: Records buffered in memory
    
    App->>SRS: sync()
    SRS->>OS: fsync(fileDescriptor)
    OS->>Disk: flushBuffers()
    Disk-->>OS: data written
    OS-->>SRS: sync complete
    SRS-->>App: sync complete
    
    Note over App, Disk: All records now crash-safe
```

### Single Writer Pattern

SRS is designed for single-writer scenarios where the application controls when data becomes durable:

- **Batch Operations**: Multiple inserts/updates before sync
- **Transaction-like**: Group related operations
- **Performance**: Reduce sync overhead
- **Crash Safety**: All-or-nothing durability

## FIFO Sync Service Pattern

### Server Synchronization Architecture

The SRS can be used to implement a robust server synchronization service:

```mermaid
sequenceDiagram
    participant Device as Mobile Device
    participant LocalSRS as Local SRS Store
    participant SyncSRS as Sync SRS Store
    participant Server as Server API
    participant ServerSRS as Server SRS Store

    Note over Device, ServerSRS: Initial State: Both stores empty

    Device->>LocalSRS: insertRecord("user:123", userData)
    Device->>LocalSRS: insertRecord("photo:456", photoData)
    Device->>LocalSRS: sync()
    Note over LocalSRS: Data committed locally

    Device->>SyncSRS: insertRecord("sync:outgoing", syncRecord)
    Note over SyncSRS: Queue outgoing changes
    Device->>SyncSRS: sync()

    Device->>Server: POST /sync (syncRecord)
    Server->>ServerSRS: insertRecord("device:abc", deviceData)
    Server-->>Device: 200 OK

    Device->>SyncSRS: deleteRecord("sync:outgoing")
    Device->>SyncSRS: sync()
    Note over SyncSRS: Outgoing queue cleared

    Server->>Device: POST /sync (serverChanges)
    Device->>SyncSRS: insertRecord("sync:incoming", serverChanges)
    Device->>LocalSRS: applyChanges(serverChanges)
    Device->>SyncSRS: deleteRecord("sync:incoming")
    Device->>SyncSRS: sync()
    Device->>LocalSRS: sync()
```

### Bidirectional Sync Implementation

```mermaid
sequenceDiagram
    participant App as Application
    participant OutSRS as Outgoing SRS
    participant InSRS as Incoming SRS
    participant LocalSRS as Local Data SRS
    participant Server as Server

    Note over App, Server: Bidirectional Sync Service

    App->>LocalSRS: insertRecord(key, data)
    App->>OutSRS: queueChange("INSERT", key, data)
    App->>OutSRS: sync()

    App->>Server: syncOutgoing()
    Server->>App: serverChanges[]
    App->>InSRS: queueChanges(serverChanges)
    App->>InSRS: sync()

    App->>InSRS: processIncoming()
    InSRS->>LocalSRS: applyChanges()
    InSRS->>InSRS: clearProcessed()
    InSRS->>InSRS: sync()

    App->>OutSRS: clearSent()
    App->>OutSRS: sync()
    App->>LocalSRS: sync()
```

### Sync Service Benefits

1. **Crash Safety**: Both local and sync operations survive crashes
2. **Resumable**: Sync can resume from any point
3. **Ordered**: FIFO queue ensures proper operation ordering
4. **Efficient**: Batch operations reduce network overhead
5. **Reliable**: CRC32 validation prevents corruption

## Implementation Requirements

### Language Agnostic Requirements

All implementations must:

1. **Follow the file format exactly** - Byte-for-byte compatibility
2. **Implement CRC32 validation** - Header CRC mandatory, data CRC optional
3. **Maintain write ordering** - Critical for crash safety
4. **Support file expansion** - Both data and header regions
5. **Provide sync() method** - For durability control
6. **Handle torn writes** - Invalid CRC indicates corruption

### Platform-Specific Optimizations

- **Java**: Memory-mapped files for performance
- **Dart**: Random access files with buffering
- **Mobile**: Platform-specific storage APIs
- **Embedded**: Minimal memory footprint

### Testing Requirements

- **Crash simulation** - Power loss during operations
- **CRC validation** - Corrupt data detection
- **File format** - Cross-platform compatibility
- **Performance** - Platform-specific benchmarks

## File Format Validation

### Magic Number Check
```c
if (readUInt32(0) != 0xBEEBBEEB) {
    throw InvalidFormatException("Invalid magic number");
}
```

### Key Length Validation
```c
uint16_t keyLength = readUInt16(4);
if (keyLength < 1 || keyLength > 32763) {
    throw InvalidFormatException("Invalid key length");
}
```

### CRC32 Validation
```c
uint32_t computedCrc = crc32(key, keyLength);
computedCrc = crc32(computedCrc, envelope, 20);
if (computedCrc != readUInt32(envelopeOffset + 16)) {
    throw CorruptDataException("Invalid CRC32");
}
```

## Conclusion

The SRS specification provides a robust foundation for crash-safe record storage across multiple platforms. The language-agnostic design ensures interoperability while allowing platform-specific optimizations. The sync service pattern enables reliable server synchronization for mobile and distributed applications.