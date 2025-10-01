# Copy Compaction: SSD-Optimized Append-Only Design

## Core Principle

**Never write to the same location twice.** Always append. Periodically compact via copy collection.

## Design Overview

### File Structure

```
[Version Counter: 8 bytes]
[File Headers: same as current]
[Index Region: grows via append]
[Data Region: append-only]
```

The version counter determines which file is active in a two-file system.

### Two-File Approach

- `store.db.0` - Primary file
- `store.db.1` - Secondary file
- On startup: open both, use the one with higher version counter
- During compaction: copy to inactive file, increment its version, swap

### Simplified Data Structures

**Remove:**
- `ConcurrentSkipListMap<RecordHeader, Integer> freeMap` - No longer needed
- Free space sorting logic
- Space allocation search

**Keep:**
- `HashMap<ByteSequence, RecordHeader> memIndex` - Fast key lookup
- `TreeMap<Long, RecordHeader> positionIndex` - Track record positions

**Add:**
- `long totalFreeSpace` - Single counter for free space
- `long currentVersion` - File version number
- `String activeFile` - Which file is currently active

## Operations

### INSERT (Append-Only)

```
insertRecord(key, value):
  1. Check if key exists → throw if exists
  2. Calculate space needed (with padding)
  3. Get EOF position → dataPointer = getFileLength()
  4. Write data at EOF → [WRITE #1: data]
  5. Append index entry → [WRITE #2: index entry]
  6. Update memIndex and positionIndex
  
  Total: 2 writes, both at EOF (sequential)
```

**No more:**
- ensureIndexSpace moving records
- Searching freeMap
- Checking gap between index and data
- Writing dataStartPtr header

### UPDATE (Append-Only)

```
updateRecord(key, value):
  1. Get old header from memIndex
  2. Calculate new space needed
  3. Append new data at EOF → [WRITE #1: data]
  4. Update index entry → [WRITE #2: index update]
  5. Update memIndex (point to new location)
  6. Add old space to totalFreeSpace
  7. Check if compaction needed
  
  Total: 2 writes, both at EOF
```

**No more:**
- In-place updates
- Dual header writes for crash safety
- Expanding last record
- Moving records

### DELETE (Append-Only Option 1: Tombstone)

```
deleteRecord(key):
  1. Get header from memIndex
  2. Write tombstone marker in index → [WRITE #1: tombstone]
  3. Add space to totalFreeSpace
  4. Remove from memIndex
  5. Check if compaction needed
  
  Total: 1 write
```

### DELETE (Append-Only Option 2: No Write)

```
deleteRecord(key):
  1. Get header from memIndex
  2. Add space to totalFreeSpace
  3. Remove from memIndex and positionIndex
  4. Mark space as free (no disk write)
  5. Check if compaction needed
  
  Total: 0 writes (compaction will skip deleted records)
```

**Trade-off:** Option 2 requires tracking deleted space in memory, but eliminates delete writes entirely.

## Compaction Strategy

### When to Compact

```
shouldCompact():
  threshold = 0.30  // 30% free space triggers compaction
  return totalFreeSpace / getFileLength() > threshold
```

Check after each delete or update that increases free space.

### Compaction Process

```
compact():
  // Determine target file
  inactiveFile = (activeFile == "store.db.0") ? "store.db.1" : "store.db.0"
  
  // Calculate new size
  liveDataSize = getFileLength() - totalFreeSpace
  newSize = liveDataSize * 1.5  // 50% headroom
  
  // Create new file with higher version
  newVersion = currentVersion + 1
  
  1. Open/create inactiveFile
  2. Write newVersion to version counter → [WRITE #1: version]
  3. Write file headers → [WRITE #2: headers]
  4. For each record in memIndex:
       a. Read data from active file
       b. Write data to new file → [WRITE #N: data]
       c. Write index entry → [WRITE #N+1: index]
       d. Update positionIndex with new location
  5. Fsync new file
  6. Update activeFile pointer
  7. Close and delete old file (or keep for backup)
  8. Reset totalFreeSpace = 0
```

**Background thread:** Run compaction asynchronously while serving reads/writes from active file.

**Synchronization:** 
- Read operations: use current memIndex (points to old file during compaction)
- Write operations during compaction: append to old file, will be copied if compaction not finished
- After compaction: atomically swap memIndex pointers and activeFile

## Crash Recovery

### On Startup

```
openStore(basePath):
  file0 = basePath + ".db.0"
  file1 = basePath + ".db.1"
  
  version0 = readVersionFrom(file0)  // -1 if not exists
  version1 = readVersionFrom(file1)  // -1 if not exists
  
  if version0 > version1:
    activeFile = file0
    currentVersion = version0
  else:
    activeFile = file1
    currentVersion = version1
  
  loadIndex(activeFile)
  calculateFreeSpace()  // Scan index to compute totalFreeSpace
```

**Recovery is simple:** The file with the highest version counter is always the correct one.

### Mid-Compaction Crash

If crash occurs during compaction:
- New file has either lower version or partially written
- On restart, use old file (higher version or more complete)
- New partial file is discarded or overwritten on next compaction

## Free Space Tracking

### In Memory

```
long totalFreeSpace = 0
```

### On Update/Delete

```
onRecordObsolete(oldSize):
  totalFreeSpace += oldSize
```

### After Compaction

```
totalFreeSpace = 0  // All space is compacted
```

### Recovery After Crash

```
calculateFreeSpace():
  totalFileSize = getFileLength()
  usedSpace = 0
  for each record in memIndex:
    usedSpace += record.dataCapacity
  totalFreeSpace = totalFileSize - usedSpace - indexSize
```

## File Layout Changes

### Current Layout
```
[KeyLength: 1][NumRecords: 4][DataStart: 8]
[Index Region: variable]
[Data Region: variable with gaps]
```

### New Layout
```
[Version: 8]
[KeyLength: 1][NumRecords: 4][DataStart: 8] (keep for compatibility)
[Index Region: append-only, grows from front]
[Data Region: append-only, grows from end]
```

**Simplification:** Remove DataStart pointer (no longer needed, index grows forward, data grows backward or both forward).

**Alternative - Both grow forward:**
```
[Version: 8]
[KeyLength: 1][NumRecords: 4]
[Index Entries: grows via append]
[Data Records: grows via append]
```

Index entries are fixed size, just append each new entry. Data records append to end.

## Implementation Changes

### Remove These Methods
- `allocateRecord()` - No longer search for space
- `updateFreeSpaceIndex()` - No freeMap to update
- `ensureIndexSpace()` - No moving records for index expansion
- `getRecordAt()` - No coalescing adjacent free space
- `split()` in RecordHeader - No splitting free space

### Simplify These Methods
- `insertRecord()` - Just append data + index entry
- `updateRecord()` - Just append new data, update index
- `deleteRecord()` - Mark free space, optionally write tombstone

### Add These Methods
- `compact()` - Background compaction
- `shouldCompact()` - Check threshold
- `getActiveFile()` - Determine which file to use
- `readVersion()` - Read version counter
- `writeVersion()` - Update version counter

### Modify These Classes
- Remove `freeMap` field from `FileRecordStore`
- Add `totalFreeSpace` field
- Add `currentVersion` field
- Add `activeFile` field
- Remove `compareRecordHeaderByFreeSpace` comparator
- Simplify `RecordHeader` (remove split() method)

## Write Count Comparison

### Current System
| Operation | Writes |
|-----------|--------|
| INSERT (best) | 5 |
| INSERT (worst) | 3N + 6 |
| UPDATE (in-place) | 3 |
| UPDATE (move) | 5-6 |
| DELETE | 1-4 |

### Append-Only System
| Operation | Writes |
|-----------|--------|
| INSERT | 2 |
| UPDATE | 2 |
| DELETE | 0-1 |
| COMPACT (periodic) | 2N (N = live records) |

**Amortized Cost:**
If compaction runs when 30% space is free, and we delete/update 30% of records before compacting:
- Operations: 0.7N × 2 writes = 1.4N writes
- Compaction: 0.7N × 2 writes = 1.4N writes  
- Total: 2.8N writes for N operations
- **Amortized: 2.8 writes per operation** (vs 3-5 currently)

## Configuration Parameters

### New Properties
- `COMPACT_THRESHOLD` - Default 0.30 (compact at 30% free)
- `COMPACT_HEADROOM` - Default 1.5 (expand by 50% during compact)
- `INITIAL_SIZE` - Default 100MB (large SSD-friendly slab)
- `EXPANSION_SIZE` - Default 50MB (large slab increments)

### Removed Properties
- `PAD_DATA_TO_KEY_LENGTH` - Always pad (no micro-optimization)

## Benefits

1. **Fewer Writes**: 2 writes per operation vs 3-5
2. **Sequential Writes**: All writes append (SSD friendly)
3. **Simpler Code**: Remove ~200 lines of free space management
4. **Better SSD Wear**: No overwrite in place
5. **Predictable Performance**: No worst-case 3N writes
6. **Simpler Recovery**: Version counter makes recovery trivial

## Trade-offs

1. **More Disk Space**: Keep 2 files, pre-allocate large slabs
2. **Periodic Pause**: Compaction must run (but can be async)
3. **Memory During Compaction**: Two sets of file handles
4. **Wasted Space**: Free space not reclaimed until compaction

## Migration Path

1. Add version counter to existing files (default 0)
2. Keep current behavior with version 0
3. New files start with version 1 and append-only mode
4. Provide tool to migrate old files to new format

## Summary

The append-only approach eliminates micro-optimizations around space reuse in favor of:
- Large slab allocation (100MB chunks)
- Always append (never search for space)
- Always pad (no space optimization)
- Track total free space (single counter)
- Periodic copy compaction (background process)
- Two-file versioning (simple crash recovery)

**Result:** Fewer writes, simpler code, better SSD performance.
