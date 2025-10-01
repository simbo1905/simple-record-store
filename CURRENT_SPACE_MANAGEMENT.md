# Current Space Management Analysis

## Overview

The current implementation uses an in-place update strategy with a free space tracking system. The file structure consists of:

1. **File Headers** (13 bytes):
   - Byte 0: Max key length
   - Bytes 1-4: Number of records (int)
   - Bytes 5-12: Data start pointer (long)

2. **Index Region**: Contains record headers for all keys
   - Each entry: key (with length prefix + CRC32) + RecordHeader (20 bytes)
   - Can expand by moving data records

3. **Data Region**: Contains actual record data
   - May have gaps from deletions/updates

## Data Structures

### In-Memory Structures
- `HashMap<ByteSequence, RecordHeader> memIndex` - O(1) key lookup
- `TreeMap<Long, RecordHeader> positionIndex` - Records indexed by file position
- `ConcurrentSkipListMap<RecordHeader, Integer> freeMap` - Free space sorted by size (ascending)

### RecordHeader
```
long dataPointer;      // 8 bytes - file position
int dataCapacity;      // 4 bytes - allocated space
int dataCount;         // 4 bytes - actual data length
int crc32;            // 4 bytes - header checksum
int indexPosition;     // position in index (not written to file)
```

## Operation Sequence Diagrams and Write Counts

### INSERT Operation

```
Client -> FileRecordStore: insertRecord(key, value)
  |
  +-> Check if key exists
  |
  +-> ensureIndexSpace(numRecords + 1)
  |     |
  |     +-> Calculate: endIndexPtr = FILE_HEADERS + (numRecords * indexEntryLength)
  |     |
  |     +-> WHILE endIndexPtr > dataStartPtr:
  |           |
  |           +-> Read first record at dataStartPtr            [READ]
  |           +-> Move to end of file:
  |               +-> Read record data                         [READ]
  |               +-> Update dataPointer to EOF
  |               +-> Expand file
  |               +-> Write record data at new position        [WRITE #1: data]
  |               +-> Write record header to index             [WRITE #2: header in index]
  |               +-> Update dataStartPtr
  |               +-> Write dataStartPtr to file header        [WRITE #3: data start header]
  |
  +-> allocateRecord(dataLength)
  |     |
  |     +-> Calculate padded length
  |     |
  |     +-> Check gap between index and data start:
  |     |   IF enough space:
  |     |     +-> Allocate in gap
  |     |     +-> Write dataStartPtr header                    [WRITE #1: data start header]
  |     |     +-> Return new RecordHeader
  |     |
  |     +-> ELSE search freeMap (sorted by size):
  |     |   FOR EACH free space:
  |     |     IF space large enough:
  |     |       +-> Split existing record
  |     |       +-> Write updated previous record header       [WRITE #1: previous header]
  |     |       +-> Return new RecordHeader
  |     |
  |     +-> ELSE append to end:
  |         +-> Expand file
  |         +-> Return new RecordHeader
  |
  +-> writeRecordData(header, value)
  |     +-> Seek to dataPointer
  |     +-> Write length + data + CRC32                        [WRITE #N: record data]
  |
  +-> addEntryToIndex(key, header, numRecords)
        +-> writeKeyToIndex()
        |   +-> Seek to index position
        |   +-> Write key length + key + CRC32                 [WRITE #N+1: key]
        |
        +-> Write RecordHeader to index                        [WRITE #N+2: record header]
        +-> Write numRecords header                            [WRITE #N+3: num records]
        +-> Update in-memory maps
```

**INSERT Write Count (Best Case - no index expansion, gap available):**
- 1 write: data start pointer header
- 1 write: record data
- 1 write: key to index
- 1 write: record header to index
- 1 write: num records header
- **Total: 5 writes**

**INSERT Write Count (Worst Case - with index expansion moving N records):**
- For each moved record (3 writes × N records):
  - Write moved data
  - Write updated header in index
  - Write data start pointer header
- Plus 5 writes for the actual insert
- **Total: 3N + 5 writes**

### UPDATE Operation (In-Place)

```
Client -> FileRecordStore: updateRecord(key, value)
  |
  +-> keyToRecordHeader(key)                                   [Lookup in memIndex]
  |
  +-> IF same size OR (smaller AND CRC enabled):
        |
        +-> Write existing header (for crash safety)           [WRITE #1: header backup]
        +-> Update dataCount
        +-> writeRecordData(header, value)
        |     +-> Seek to dataPointer
        |     +-> Write length + data + CRC32                  [WRITE #2: record data]
        |
        +-> Write header again (with new CRC)                  [WRITE #3: header final]
```

**UPDATE Write Count (In-Place - Same Size or Smaller with CRC):**
- 1 write: record header (backup)
- 1 write: record data
- 1 write: record header (final)
- **Total: 3 writes**

### UPDATE Operation (Last Record - Expand/Contract File)

```
Client -> FileRecordStore: updateRecord(key, value)
  |
  +-> keyToRecordHeader(key)
  |
  +-> IF endOfRecord == fileLength:
        |
        +-> Update dataCount and dataCapacity
        +-> Expand or contract file
        +-> writeRecordData(header, value)
        |     +-> Write length + data + CRC32                  [WRITE #1: record data]
        |
        +-> Write header to index                              [WRITE #2: record header]
```

**UPDATE Write Count (Last Record):**
- 1 write: record data
- 1 write: record header
- **Total: 2 writes**

### UPDATE Operation (Move Required - Data Grows)

```
Client -> FileRecordStore: updateRecord(key, value)
  |
  +-> keyToRecordHeader(key) -> oldHeader
  |
  +-> IF value.length > oldHeader.dataCapacity:
        |
        +-> allocateRecord(value.length)
        |     +-> [See allocateRecord flow above]              [WRITE #1-2: varies]
        |
        +-> writeRecordData(newHeader, value)                  [WRITE #3: new data]
        +-> Write new header to index                          [WRITE #4: new header]
        +-> Update in-memory maps
        |
        +-> Handle old space:
            |
            +-> IF previous record exists:
            |     +-> Increment previous record's capacity
            |     +-> Write previous header                    [WRITE #5: previous header]
            |
            +-> ELSE:
                  +-> Write dataStartPtr header                [WRITE #5: data start header]
```

**UPDATE Write Count (Move - Record Grows):**
- Best case (append to end): 5 writes
- Worst case (reuse free space): 6 writes

### DELETE Operation

```
Client -> FileRecordStore: deleteRecord(key)
  |
  +-> keyToRecordHeader(key) -> delRec
  |
  +-> deleteEntryFromIndex(delRec, numRecords)
  |     |
  |     +-> IF not last in index:
  |     |     +-> Read last key from index                     [READ]
  |     |     +-> Get last header
  |     |     +-> Update last header's indexPosition
  |     |     +-> Write last key to deleted position           [WRITE #1: key overwrite]
  |     |     +-> Write last header to deleted position        [WRITE #2: header overwrite]
  |     |
  |     +-> Write numRecords - 1                               [WRITE #3: num records]
  |
  +-> Remove from in-memory maps
  |
  +-> IF delRec is at end of file:
  |     +-> Shrink file
  |
  +-> ELSE IF previous record exists:
  |     +-> Increment previous capacity
  |     +-> Write previous header                              [WRITE #4: previous header]
  |
  +-> ELSE:
        +-> Write dataStartPtr + delRec.capacity               [WRITE #4: data start header]
```

**DELETE Write Count (Not Last Record):**
- If not last in index: 2 writes (key + header overwrite)
- 1 write: num records header
- 1 write: update adjacent space (previous header or data start)
- **Total: 4 writes**

**DELETE Write Count (Last Record in File):**
- Similar index updates (0-2 writes)
- 1 write: num records header
- File shrink (no write)
- **Total: 1-3 writes**

## Free Space Management

### Current Strategy
1. **FreeMap Structure**: `ConcurrentSkipListMap` sorted by free space size (ascending)
2. **Space Allocation**: Linear scan from smallest to find first fit
3. **Space Tracking**: Each RecordHeader tracks its own free space via `dataCapacity - (dataCount + overhead)`
4. **Coalescing**: Only happens with adjacent previous record on delete/move

### Space Allocation Priority
1. Gap between index and data start (if available)
2. First-fit search in freeMap (scans from smallest)
3. Append to end of file

### Issues with Current Approach
1. **Write Amplification**: Multiple seeks and writes per operation
2. **Fragmentation**: Free space scattered throughout file
3. **Linear Scan**: Must iterate freeMap to find suitable space
4. **In-Place Updates**: Requires seeking to old position to overwrite
5. **Index Expansion**: May require moving multiple records (3N writes)

## Summary: Total Writes Per Operation

| Operation | Best Case | Worst Case | Notes |
|-----------|-----------|------------|-------|
| INSERT (no index expand) | 5 | 6 | Depends on free space reuse |
| INSERT (with index expand) | 5 | 3N + 6 | N = records to move |
| UPDATE (in-place) | 3 | 3 | Same/smaller size |
| UPDATE (move) | 5 | 6 | Record grows |
| DELETE | 1 | 4 | Last record vs middle |

## Key Observations

1. **Multiple Writes Per Operation**: Each operation involves 3-6 writes minimum
2. **Scattered Writes**: Writes happen at different file locations (header, index, data)
3. **Synchronous Operations**: All writes must complete before operation finishes
4. **Index Maintenance Overhead**: Moving headers between index positions
5. **Free Space Bookkeeping**: Requires updating headers of adjacent records
6. **No Batching**: Each operation commits immediately

## Current Micro-Optimizations

1. **PAD_DATA_TO_KEY_LENGTH**: Pads records to avoid frequent index expansions
2. **CRC32 for Crash Safety**: Dual writes of headers for atomic updates
3. **Free Space Sorting**: ConcurrentSkipListMap for fast iteration
4. **Index Preallocation**: Initial size hint to reduce early expansions

## Requirements for SSD-Optimized Approach

Based on the analysis above, an SSD-optimized approach should:

### 1. Append-Only Writes
- **Eliminate in-place updates**: Never seek backwards to overwrite data
- **Always append**: All writes go to the end of the current active segment
- **No free space reuse**: Don't search for gaps in the file
- **Simplified allocation**: Just increment end-of-file pointer

### 2. Minimal Bookkeeping
- **Track total free space**: Single counter instead of sorted map
- **No per-record free space**: Remove freeMap entirely
- **Simpler data structures**: Eliminate NavigableMap overhead

### 3. Large Block Allocation
- **Big slab preallocation**: Start with large file (e.g., 100MB)
- **Big slab expansion**: Grow by large chunks (e.g., 50MB)
- **Always pad records**: Don't optimize for space - optimize for simplicity

### 4. Copy Compaction Strategy
- **Two-file approach**: Active file and shadow file
- **Version counter**: Track which file is current
- **Background compaction**: Copy live records when free space exceeds threshold
- **Atomic switch**: Update version counter to make new file active

### 5. Crash Safety
- **Write-ahead version**: Version counter ensures recovery
- **Monotonic operations**: Only append, never modify in place
- **Simple recovery**: On restart, use file with highest version

### 6. Operation Simplification

**INSERT (Append-Only)**:
- Calculate space needed
- Append data to end of file
- Append index entry
- Update in-memory maps
- **Target: 2 writes** (data + index entry)

**UPDATE (Append-Only)**:
- Append new data to end of file
- Update index entry in place (or append if append-only index)
- Mark old space as free (increment free space counter)
- **Target: 2-3 writes**

**DELETE (Append-Only)**:
- Mark index entry as deleted (tombstone) or remove
- Increment free space counter
- **Target: 1 write**

**COMPACTION (Background)**:
- When free space > threshold (e.g., 30%):
  - Expand file to (current size - free space) × 1.5
  - Copy all live records to new area
  - Update version counter
  - Truncate old area
- **Periodic operation, not per-request**

### 7. Trade-offs
- **Space for Speed**: Accept more disk space usage for fewer writes
- **Periodic Compaction**: Amortize space reclamation across many operations
- **Larger Files**: Pre-allocate to avoid frequent expansions
- **Simpler Code**: Remove complex free space management logic

### 8. Write Reduction Analysis

Current average writes per operation: **3-5 writes**
Target with append-only: **2 writes** (data + index update)
**Reduction: 40-60% fewer writes**

Additionally:
- Fewer seeks (always append)
- Better SSD wear leveling
- Simpler recovery logic
- More predictable performance
