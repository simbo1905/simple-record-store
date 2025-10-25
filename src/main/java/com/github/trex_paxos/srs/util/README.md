# Expand Key Size Utility

## Overview

The `expand_key_size` utility expands the maximum key size of a Simple Record Store database file. It creates a new file with the larger key size and copies all data in an efficient single-pass forward scan.

## Building

Build the utility JAR using the Maven profile:

```bash
mvn clean package -Putility-jar
```

This creates `target/simple-record-store-*-utility.jar` with the main class configured.

## Usage

```bash
java -jar target/simple-record-store-*-utility.jar expand_key_size <new_key_size> <old_file> <new_file>
```

### Parameters

- `new_key_size`: The new maximum key length (must be larger than the current key length)
- `old_file`: Path to the existing database file
- `new_file`: Path for the new database file (must not exist)

### Example

```bash
# Expand a database from 32-byte keys to 128-byte keys
java -jar target/simple-record-store-1.0.0-RC7-SNAPSHOT-utility.jar \
  expand_key_size 128 mydata.db mydata_expanded.db
```

## Algorithm

The utility performs an efficient single-pass forward scan:

1. **Read headers**: Reads the old file's max key length, number of records, and data start pointer
2. **Calculate offsets**: Computes the new index size and data region offset
3. **Copy index**: Reads each index entry (key + header) from old file, adjusts data pointers, writes to new file
4. **Copy data**: Uses stored header positions to copy all data records in a forward pass

## Validation

The utility validates:
- Source file exists
- Destination file does not exist
- New key size is within valid range (1-252)
- New key size is larger than current key size
- Header CRC32 checksums are valid

## Testing

Run the comprehensive test suite:

```bash
mvn test -Dtest=ExpandKeySizeTest
```

Tests cover:
- Empty databases
- Large data records (10KB+)
- Many records (100+)
- Maximum key length (252)
- Data integrity preservation
- Updated and deleted records
- All error conditions

## Implementation

- **Package**: `com.github.trex_paxos.srs.util`
- **Main class**: `ExpandKeySize`
- **Dependencies**: Zero (pure JDK)
- **Test coverage**: 15 comprehensive tests
