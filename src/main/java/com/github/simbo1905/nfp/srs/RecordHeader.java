package com.github.simbo1905.nfp.srs;

/// Immutable record header representing a record's metadata in the file store.
/// Uses static factory methods for atomic updates instead of Lombok @With.
record RecordHeader(
    /// File pointer to the first byte of record data (8 bytes).
    long dataPointer,
    /// Actual number of bytes of data held in this record (4 bytes).
    int dataCount,
    /// Number of bytes of data that this record can hold (4 bytes).
    int dataCapacity,
    /// This header's position in the file index.
    int indexPosition,
    /// CRC32 checksum of the header data (dataPointer + dataCapacity + dataCount).
    long crc32) {

  /// Creates a new RecordHeader with the specified data pointer and capacity.
  /// Computes CRC32 for the header data. Used when creating new records.
  RecordHeader(long dataPointer, int dataCapacity) {
    this(dataPointer, -1, dataCapacity, 0, computeHeaderCrc(dataPointer, -1, dataCapacity));
  }

  /// Creates a new RecordHeader with the specified fields, computing CRC32 from the header data.
  /// This constructor is used when writing headers to ensure CRC consistency.
  RecordHeader(long dataPointer, int dataCount, int dataCapacity, int indexPosition) {
    this(
        dataPointer,
        dataCount,
        dataCapacity,
        indexPosition,
        computeHeaderCrc(dataPointer, dataCount, dataCapacity));
  }

  /// Computes CRC32 for header data (dataPointer + dataCapacity + dataCount).
  /// Matches the original mutable implementation exactly.
  private static long computeHeaderCrc(long dataPointer, int dataCount, int dataCapacity) {
    // Use ByteBuffer to match original implementation exactly
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(16); // 8 + 4 + 4 bytes
    buffer.putLong(dataPointer); // 8 bytes
    buffer.putInt(dataCapacity); // 4 bytes
    buffer.putInt(dataCount); // 4 bytes

    final var array = buffer.array();
    java.util.zip.CRC32 crc = new java.util.zip.CRC32();
    crc.update(array, 0, 16); // Update all 16 bytes
    return crc.getValue();
  }

  /// Creates a new RecordHeader by copying from an existing one with updated fields.
  /// Used for atomic updates in map operations. Recomputes CRC if data fields change.
  private static RecordHeader withFields(
      RecordHeader original, long dataPointer, int dataCount, int dataCapacity, int indexPosition) {
    // Determine final field values
    long finalDataPointer = dataPointer != -1 ? dataPointer : original.dataPointer;
    int finalDataCount = dataCount != -1 ? dataCount : original.dataCount;
    int finalDataCapacity = dataCapacity != -1 ? dataCapacity : original.dataCapacity;
    int finalIndexPosition = indexPosition != -1 ? indexPosition : original.indexPosition;

    // Recompute CRC only if data fields changed (indexPosition doesn't affect CRC)
    long finalCrc32;
    if (dataPointer != -1 || dataCount != -1 || dataCapacity != -1) {
      finalCrc32 = computeHeaderCrc(finalDataPointer, finalDataCount, finalDataCapacity);
    } else {
      finalCrc32 = original.crc32;
    }

    return new RecordHeader(
        finalDataPointer, finalDataCount, finalDataCapacity, finalIndexPosition, finalCrc32);
  }

  /// Creates a new RecordHeader with updated data pointer and data capacity.
  /// Used for atomic updates in map operations.
  static RecordHeader move(RecordHeader original, long newDataPointer, int newDataCapacity) {
    return withFields(original, newDataPointer, -1, newDataCapacity, -1);
  }

  /// Creates a new RecordHeader with updated data count.
  /// Used for atomic updates after writing record data.
  static RecordHeader withDataCount(RecordHeader original, int newDataCount) {
    return withFields(original, -1, newDataCount, -1, -1);
  }

  /// Creates a new RecordHeader with updated index position.
  /// Used when moving records in the index.
  static RecordHeader withIndexPosition(RecordHeader original, int newIndexPosition) {
    return withFields(original, -1, -1, -1, newIndexPosition);
  }

  /// Returns the free space available in this record.
  int getFreeSpace(boolean disableCrc32) {
    int len = dataCount + 4; // for length prefix
    if (!disableCrc32) {
      len += 8;
    }
    return dataCapacity - len;
  }

  /// Returns a new record header which occupies the free space of this record.
  /// Shrinks this record size by the size of its free space.
  RecordHeader split(boolean disableCrc32, int padding) {
    long newFp = dataPointer + dataCount + padding;
    RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace(disableCrc32));
    // Return updated version of this record with reduced capacity
    return withFields(this, -1, -1, dataCount, -1);
  }

  /// Returns a new RecordHeader with incremented data capacity.
  RecordHeader incrementDataCapacity(int increment) {
    return withFields(this, -1, -1, this.dataCapacity + increment, -1);
  }
}
