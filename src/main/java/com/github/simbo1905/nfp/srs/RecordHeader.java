package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/// Immutable record header representing a record's metadata in the file store.
/// Uses static factory methods for atomic updates instead of Lombok @With.
record RecordHeader(
    long dataPointer,
    int dataLength,
    int dataCapacity,
    int indexPosition,
    long crc32) {

  /// The fixed-size metadata envelope that describes a record header.
  /// This envelope includes *all* fixed-width metadata fields for the record:
  /// - keyLength (short): 2 bytes – the length of the key.
  /// - dataPointer (long): 8 bytes – pointer to the data payload.
  /// - dataCapacity (int): 4 bytes – allocated space for the payload.
  /// - dataLength (int): 4 bytes – actual used size within the payload.
  /// - crc32 (int): 4 bytes – checksum covering the header contents.
  ///
  /// Total: 2 + 8 + 4 + 4 + 4 = 22 bytes.
  ///
  /// Including keyLength here ensures the record header is self-describing.
  /// The key itself remains variable-length and follows this envelope.
  /// Without this, someone might forget to read or write the short,
  /// breaking index consistency and corrupting the store.
  static final int ENVELOPE_SIZE =
      Short.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES;

  /// Creates a new RecordHeader with the specified data pointer, length, and capacity.
  /// Computes CRC32 for the header data. Used when creating new records.
  RecordHeader(long dataPointer, int dataLength, int dataCapacity) {
    this(dataPointer, dataLength, dataCapacity, 0, computeHeaderCrc(dataLength, dataCapacity));
    // Debug logging to trace corruption
    if (dataLength != dataCapacity) {
      System.err.printf(
          "RecordHeader CONSTRUCTOR: dataPointer=%d, dataLength=%d, dataCapacity=%d%n",
          dataPointer, dataLength, dataCapacity);
    }
  }

  /// Creates a new RecordHeader with the specified fields, computing CRC32 from the header data.
  /// This constructor is used when writing headers to ensure CRC consistency.
  RecordHeader(long dataPointer, int dataCount, int dataCapacity, int indexPosition) {
    this(
        dataPointer,
        dataCount,
        dataCapacity,
        indexPosition,
        computeHeaderCrc(dataCount, dataCapacity));
  }

  /// Computes CRC32 for header data (dataLength + dataCapacity + keyLength placeholder).
  /// Does NOT include file position (dataPointer) as that causes unnecessary churn when records
  /// move. File position is transient metadata - including it would require recomputing CRCs on every
  /// move. Includes keyLength placeholder for consistent envelope structure with key CRC.
  private static long computeHeaderCrc(int dataLength, int dataCapacity) {
    // CRC covers the actual header data fields, not file position
    final int HEADER_DATA_SIZE =
        Integer.BYTES + Integer.BYTES + Short.BYTES; // 4 + 4 + 2 = 10 bytes
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(HEADER_DATA_SIZE);
    buffer.putInt(dataLength); // 4 bytes
    buffer.putInt(dataCapacity); // 4 bytes
    buffer.putShort(
        (short) 0); // 2 bytes placeholder for keyLength salt (actual key length added separately)

    final var array = buffer.array();
    java.util.zip.CRC32 crc = new java.util.zip.CRC32();
    crc.update(array, 0, HEADER_DATA_SIZE); // Update all header data bytes
    return crc.getValue();
  }

  /// Creates a new RecordHeader with updated data pointer and data capacity.
  /// Used for atomic updates in map operations.
  static RecordHeader move(RecordHeader original, long newDataPointer, int newDataCapacity) {
    return withDataPointerAndCapacity(original, newDataPointer, newDataCapacity);
  }

  /// Creates a new RecordHeader with updated data pointer and data capacity.
  /// Used for atomic updates when moving records to new file positions.
  /// Recomputes CRC due to capacity changes.
  static RecordHeader withDataPointerAndCapacity(
      RecordHeader original, long newDataPointer, int newDataCapacity) {
    Objects.requireNonNull(original, "original RecordHeader cannot be null");
    return new RecordHeader(
        newDataPointer,
        original.dataLength,
        newDataCapacity,
        original.indexPosition,
        computeHeaderCrc(original.dataLength, newDataCapacity));
  }

  /// Creates a new RecordHeader with updated data capacity, keeping other fields unchanged.
  /// Used for atomic updates when expanding or shrinking record space.
  /// Recomputes CRC due to capacity change.
  static RecordHeader withDataCapacity(RecordHeader original, int newDataCapacity) {
    Objects.requireNonNull(original, "original RecordHeader cannot be null");
    return new RecordHeader(
        original.dataPointer,
        original.dataLength,
        newDataCapacity,
        original.indexPosition,
        computeHeaderCrc(original.dataLength, newDataCapacity));
  }

  /// Creates a new RecordHeader with updated data count.
  /// Used for atomic updates after writing record data.
  static RecordHeader withDataCount(RecordHeader original, int newDataCount) {
    Objects.requireNonNull(original, "original RecordHeader cannot be null");
    return new RecordHeader(
        original.dataPointer,
        newDataCount,
        original.dataCapacity,
        original.indexPosition,
        computeHeaderCrc(newDataCount, original.dataCapacity));
  }

  /// Creates a new RecordHeader with updated index position.
  /// Used when moving records in the index.
  static RecordHeader withIndexPosition(RecordHeader original, int newIndexPosition) {
    Objects.requireNonNull(original, "original RecordHeader cannot be null");
    return new RecordHeader(
        original.dataPointer,
        original.dataLength,
        original.dataCapacity,
        newIndexPosition,
        original.crc32);
  }

  /// Returns the free space available in this record.
  int getFreeSpace(boolean disableCrc32) {
    int len = dataLength + 4; // for length prefix
    if (!disableCrc32) {
      len += 8;
    }
    return dataCapacity - len;
  }

  /// Splits this record into used portion, free portion for file fragmentation management.
  /// Creates a new RecordHeader that occupies the free space following this record's used data.
  /// This record's capacity is conceptually reduced to match its used data length.
  /// Returns the new free-space RecordHeader that can be reused for future writes.
  ///
  /// Usage pattern in FileRecordStore:
  /// 1. Call split() to create a free-space header from unused capacity
  /// 2. Update original record's capacity to dataLength (removing free space)
  /// 3. Track the new free-space header in freeMap for future allocations
  ///
  /// This enables in-place record splitting during:
  /// - Delete operations: reclaim trailing space as free records
  /// - Compaction: split large records to fit smaller inserts
  /// - Space management: maintain free-space list for efficient reuse
  ///
  /// Inverse operation: incrementDataCapacity() merges adjacent free space back into record.
  RecordHeader split(boolean disableCrc32, int padding) {
    int freeSpace = getFreeSpace(disableCrc32);
    if (freeSpace <= 0) {
      throw new IllegalStateException("No free space available for split operation");
    }

    // Create new record at the position after this record's used data + padding
    long newFp = dataPointer + dataLength + padding;

    // Return the new free-space record header
    // Caller will update this record's capacity to dataLength to complete the split
    return new RecordHeader(newFp, freeSpace, freeSpace);
  }

  /// Returns a new RecordHeader with incremented data capacity for file compaction management.
  /// Expands this record's capacity by absorbing adjacent free space.
  /// Used to merge free regions back into records during compaction or growth operations.
  ///
  /// Inverse operation: split() divides records to create free-space regions.
  /// Both methods work together to manage in-file fragmentation:
  /// - split(): divide record + free → record + free_header
  /// - incrementDataCapacity(): merge record + free → larger_record
  @SuppressWarnings("UnusedReturnValue")
  RecordHeader incrementDataCapacity(int increment) {
    return withDataCapacity(this, dataCapacity + increment);
  }

  /// Writes a RecordHeader to the file operations.
  /// Handles envelope serialization without logging (logging handled by caller).
  /// Note: keyLength is written separately before this envelope, but is included
  /// in ENVELOPE_SIZE for documentation completeness.
  static void writeTo(FileOperations out, RecordHeader header) throws IOException {
    // Create ByteBuffer for envelope serialization (without keyLength)
    final int ACTUAL_ENVELOPE_SIZE = ENVELOPE_SIZE - Short.BYTES; // 20 bytes (without keyLength)
    ByteBuffer buffer = ByteBuffer.allocate(ACTUAL_ENVELOPE_SIZE);
    buffer.putLong(header.dataPointer());
    buffer.putInt(header.dataCapacity());
    buffer.putInt(header.dataLength());
    buffer.putInt((int) (header.crc32() & 0xFFFFFFFFL));

    out.write(buffer.array(), 0, ACTUAL_ENVELOPE_SIZE);
  }

  /// Reads a RecordHeader from the file operations with CRC validation.
  /// Returns a new RecordHeader with the specified index position.
  /// Logging is handled by the caller.
  /// Note: keyLength is read separately before this envelope, but is included
  /// in ENVELOPE_SIZE for documentation completeness.
  static RecordHeader readFrom(FileOperations in, int indexPosition) throws IOException {
    final int ACTUAL_ENVELOPE_SIZE = ENVELOPE_SIZE - Short.BYTES; // 20 bytes (without keyLength)
    byte[] header = new byte[ACTUAL_ENVELOPE_SIZE];
    in.readFully(header);

    ByteBuffer buffer = ByteBuffer.allocate(ACTUAL_ENVELOPE_SIZE);
    buffer.put(header);
    buffer.flip();

    long dataPointer = buffer.getLong();
    int dataCapacity = buffer.getInt();
    int dataCount = buffer.getInt();
    long crc32 = buffer.getInt() & 0xFFFFFFFFL;

    // Validate CRC
    long expectedCrc = computeHeaderCrc(dataCount, dataCapacity);
    if (crc32 != expectedCrc) {
      throw new IllegalStateException(
          String.format("invalid header CRC32 expected %d but got %d", expectedCrc, crc32));
    }

    return new RecordHeader(dataPointer, dataCount, dataCapacity, indexPosition, crc32);
  }

  /// Computes CRC32 for arbitrary byte data with consistent implementation.
  static long computeCrc32(byte[] data, int length) {
    java.util.zip.CRC32 crc = new java.util.zip.CRC32();
    crc.update(data, 0, length);
    return crc.getValue();
  }

  /// Formats a RecordHeader for logging with optional data preview.
  @SuppressWarnings("SameParameterValue")
  static String formatForLog(RecordHeader header, byte[] preview) {
    return String.format(
        "RecordHeader[dataPointer=%d, dataLength=%d, dataCapacity=%d, crc32=%08x, preview=%s]",
        header.dataPointer(),
        header.dataLength(),
        header.dataCapacity(),
        header.crc32(),
        preview != null ? FileRecordStore.print(preview) : "null");
  }

  /// Computes CRC32 for key data (key length + key bytes).
  /// Used for key validation in the index.
  /// Includes key length as a salt to improve collision resistance for small keys.
  static long computeKeyCrc(byte[] key) {
    ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + key.length);
    buffer.putShort((short) key.length); // key length as salt
    buffer.put(key);
    return computeCrc32(buffer.array(), buffer.array().length);
  }
}
