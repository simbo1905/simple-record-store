package com.github.simbo1905.nfp.srs;

import static com.github.simbo1905.nfp.srs.FileRecordStoreBuilder.*;
import static java.util.Optional.of;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Synchronized;
import org.jetbrains.annotations.TestOnly;

/// A persistent record store that maps keys to values with crash-safe guarantees.
/// Provides ACID properties with durable writes and supports both direct I/O and memory-mapped
/// access modes.
public class FileRecordStore implements AutoCloseable {

  private static final Logger logger = Logger.getLogger(FileRecordStore.class.getName());

  // this is an unsigned 32 int
  static final int CRC32_LENGTH = Integer.BYTES;

  /// The maximum key length this store was configured with. Immutable after creation.
  public final int maxKeyLength;

  /// The total length of one complete index entry containing both key and header metadata.
  /// Each index entry consists of:
  /// - keyLength (short): 2 bytes - length of the key that follows
  /// - keyData (byte[]): maxKeyLength bytes - the actual key data (padded to fixed width)
  /// - keyCrc32 (int): 4 bytes - CRC32 of keyLength + keyData for corruption detection
  /// - recordHeader (RecordHeader.ENVELOPE_SIZE): 22 bytes - fixed metadata envelope containing:
  ///   * keyLength placeholder (short): 2 bytes - included for documentation consistency
  ///   * dataPointer (long): 8 bytes - file position of record data
  ///   * dataLength (int): 4 bytes - actual bytes used in data region
  ///   * dataCapacity (int): 4 bytes - total allocated space for data
  ///   * headerCrc32 (int): 4 bytes - CRC of header data fields (excluding file position)
  ///
  /// Total: 2 + maxKeyLength + 4 + 22 = maxKeyLength + 28 bytes
  /// The key and header are stored separately but both are included in this fixed-width entry
  /// to enable O(1) index lookups by position while maintaining crash-safe write ordering.
  private final int indexEntryLength;

  @Getter private final Path filePath;

  /// Flag indicating if this store was opened as read-only
  private final boolean readOnly;

  private final Comparator<RecordHeader> compareRecordHeaderByFreeSpace =
      Comparator.comparingInt(o -> o.getFreeSpace(true));

  ///  FileOperations abstracts over memory mapped files and traditional IO random access file.
  /*default*/ FileOperations fileOperations;

  /// Thread-safe state management for record headers
  private final State headerState;

  /// Key type for optimized handling - enables JIT branch elimination since this is final after
  // construction
  private final KeyType keyType;

  /// Whether to use defensive copying for byte array keys for example is not needed when the actual
  // keys ware UUID or Strings
  private final boolean defensiveCopy;

  /// Returns whether in-place updates are allowed for smaller records. We will toggle this to do
  // online backups.
  @Getter private volatile boolean allowInPlaceUpdates = true;

  /// Returns whether header region expansion is allowed. We will toggle this to do online backups.
  ///
  /// Whether to allow header region expansion during operations
  @Getter private volatile boolean allowHeaderExpansion = true;

  /// Expansion size in bytes for extending the file. These days on cloud hosts with SSDs 2 MiB is
  // sensible default.
  final int preferredExpansionSize;

  /// Block size in bytes for data alignment (must be power of 2). Typical with SSD a 4 KiB, 8 KiB
  // or even higher may be sensible.
  final int preferredBlockSize;

  /// Initial header region size in bytes. If you know you have small keys and will only have a
  // hundred thousand we can just preallocate the space to avoid moving records to ever expand the
  // header region.
  final int initialHeaderRegionSize;

  /// Store state tracking for proper lifecycle management
  /// <ul>
  ///   <li><b>NEW</b> - Initial state - store created but not yet validated/opened</li>
  ///   <li><b>OPEN</b> - Store successfully opened and operational</li>
  ///   <li><b>CLOSED</b> - Store cleanly closed via close() method</li>
  ///   <li><b>UNKNOWN</b> - Store encountered an exception, state is uncertain</li>
  /// </ul>
  enum StoreState {
    NEW,
    OPEN,
    CLOSED,
    UNKNOWN
  }

  /// Current state of the store
  private volatile StoreState state = StoreState.NEW;

  /// Reference to parent FileRecordStore for state management during errors
  private final FileRecordStore parentStore = this;
  /// TreeMap of headers by file index - now managed by State class
  // private TreeMap<Long, RecordHeader> positionIndex; // Replaced by State class
  /// ConcurrentSkipListMap makes scanning by ascending values fast and is sorted by smallest free
  /// space first
  private ConcurrentNavigableMap<RecordHeader, Integer> freeMap =
      new ConcurrentSkipListMap<>(compareRecordHeaderByFreeSpace);
  // Current file pointer to the start of the record data.
  private long dataStartPtr;
  // only change this when debugging in unit tests
  private boolean disableCrc32;

  /// Creates a new database file with pre-allocated index space and optional memory-mapped I/O.
  ///
  /// @param file              the database file handle
  /// @param preallocatedRecords number of index entries to pre-allocate; inserts beyond this force
  /// record movement to expand the index region.
  ///                            Set to 0 for testing to force movement on every insert.
  /// @param maxKeyLength        maximum key length in bytes
  /// @param preferredExpansionSize expansion size in bytes for header region growth
  /// @param preferredBlockSize  block size in bytes for data alignment (must be power of 2)
  /// @param initialHeaderRegionSize initial header region size in bytes
  public FileRecordStore(
      File file,
      int preallocatedRecords,
      int maxKeyLength,
      boolean disablePayloadCrc32,
      boolean useMemoryMapping,
      String accessMode,
      KeyType keyType,
      boolean defensiveCopy,
      int preferredExpansionSize,
      int preferredBlockSize,
      int initialHeaderRegionSize)
      throws IOException {
    try {
      // Validate and store sizing parameters
      if (preferredExpansionSize <= 0) {
        throw new IllegalArgumentException(
            "preferredExpansionSize must be positive, got " + preferredExpansionSize);
      }
      if (preferredBlockSize <= 0 || (preferredBlockSize & (preferredBlockSize - 1)) != 0) {
        throw new IllegalArgumentException(
            "preferredBlockSize must be positive and power of 2, got " + preferredBlockSize);
      }
      if (initialHeaderRegionSize <= 0) {
        throw new IllegalArgumentException(
            "initialHeaderRegionSize must be positive, got " + initialHeaderRegionSize);
      }
      // Validate maxKeyLength early - must be greater than zero
      if (maxKeyLength < 1 || maxKeyLength > FileRecordStoreBuilder.MAX_KEY_LENGTH) {
        throw new IllegalArgumentException(
            String.format(
                "maxKeyLength must be greater than zero and no more than %d (8-byte aligned maximum), got %d",
                FileRecordStoreBuilder.MAX_KEY_LENGTH, maxKeyLength));
      }

      java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, accessMode);

      try {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "create file=%s preallocatedRecords=%d maxKeyLength=%d disablePayloadCrc32=%b useMemoryMapping=%b",
                    file.toPath(),
                    preallocatedRecords,
                    maxKeyLength,
                    disablePayloadCrc32,
                    useMemoryMapping));

        this.disableCrc32 = disablePayloadCrc32;
        this.maxKeyLength = maxKeyLength;
        this.indexEntryLength =
            Short.BYTES + maxKeyLength + CRC32_LENGTH + (RecordHeader.ENVELOPE_SIZE - Short.BYTES);
        this.readOnly = !"rw".equals(accessMode);
        this.keyType = keyType;
        this.defensiveCopy = defensiveCopy;
        this.preferredExpansionSize = preferredExpansionSize;
        this.preferredBlockSize = preferredBlockSize;
        this.initialHeaderRegionSize = initialHeaderRegionSize;

        // Validate UUID mode constraints
        if (keyType == KeyType.UUID && maxKeyLength != 16) {
          throw new IllegalArgumentException(
              "UUID key type requires maxKeyLength=16, got " + maxKeyLength);
        }

        // Check if file was empty when we opened it (before we modify it)
        boolean wasEmpty = raf.length() == 0;

        // Only set length for new files - don't overwrite existing data
        if (wasEmpty) {
          raf.setLength(
              FileRecordStoreBuilder.FILE_HEADERS_REGION_LENGTH
                  + (preallocatedRecords * indexEntryLength * 2L));
        }
        this.fileOperations =
            useMemoryMapping ? new MemoryMappedFile(raf) : new RandomAccessFile(raf);
        this.filePath = file.toPath();

        dataStartPtr =
            FileRecordStoreBuilder.FILE_HEADERS_REGION_LENGTH
                + ((long) preallocatedRecords * indexEntryLength);

        headerState = new State();

        // Only initialize headers for new files - existing files should already have headers
        if (wasEmpty) {
          writeMagicNumberHeader();
          writeKeyLengthHeader();
          writeNumRecordsHeader(0);
          writeDataStartPtrHeader(dataStartPtr);
        } else {
          // Existing file - validate headers before loading data
          // First check if this is an old format file (without magic number)
          fileOperations.seek(0);
          int firstFourBytes = fileOperations.readInt();

          int existingKeyLength;
          int existingRecords;

          if (firstFourBytes == MAGIC_NUMBER) {
            // New format with magic number
            existingKeyLength = readKeyLengthHeader();
            existingRecords = readNumRecordsHeader();
          } else {
            // Old format - reject with exception instead of warning
            throw new IllegalStateException(
                "Invalid file format: File does not contain required magic number 0xBEEBBEEB. "
                    + "This appears to be an old format file or corrupted data. "
                    + "Only files created with FileRecordStore.Builder are supported.");
          }

          // Validate key length matches before proceeding
          if (existingKeyLength != maxKeyLength) {
            throw new IllegalArgumentException(
                String.format(
                    "File has key length %d but builder specified %d",
                    existingKeyLength, maxKeyLength));
          }

          // Read dataStartPtr for new format
          dataStartPtr = readDataStartHeader();

          // Validate file has minimum required size for existing records
          long requiredFileSize =
              FileRecordStoreBuilder.FILE_HEADERS_REGION_LENGTH
                  + ((long) existingRecords * indexEntryLength);
          if (fileOperations.length() < requiredFileSize) {
            throw new IOException(
                String.format(
                    "File too small for %d records. Required: %d bytes, Actual: %d bytes",
                    existingRecords, requiredFileSize, fileOperations.length()));
          }

          // Load existing index into memory - this may throw if data is corrupted
          loadExistingIndex(existingRecords);
        }
      } catch (Exception e) {
        // Close the RandomAccessFile if constructor fails to prevent resource leak
        try {
          raf.close();
        } catch (IOException closeException) {
          // Log but don't mask the original exception
          logger.log(
              Level.WARNING,
              "Failed to close RandomAccessFile during constructor failure",
              closeException);
        }
        throw e;
      }

      // Only transition to OPEN after successful initialization
      state = StoreState.OPEN;
    } catch (Exception e) {
      // Construction failed - transition to UNKNOWN state
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Writes the magic number header to the beginning of the fileOperations.
  private void writeMagicNumberHeader() throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.MAGIC_NUMBER_HEADER_LOCATION);
    fileOperations.writeInt(MAGIC_NUMBER);
    logger.log(
        Level.FINEST, () -> String.format("Writing magic number header: 0x%08X", MAGIC_NUMBER));
  }

  /// Writes the max key length to the fileOperations (after magic number).
  private void writeKeyLengthHeader() throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.KEY_LENGTH_HEADER_LOCATION);
    final var keyLength = (short) maxKeyLength;
    logger.log(
        Level.FINEST,
        "Writing key length header: " + keyLength + " (from maxKeyLength=" + maxKeyLength + ")");
    fileOperations.writeShort(keyLength);
  }

  /// Reads the max key length from the fileOperations (after magic number).
  private int readKeyLengthHeader() throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.KEY_LENGTH_HEADER_LOCATION);
    int keyLength = fileOperations.readShort() & 0xFFFF;
    logger.log(Level.FINEST, "Reading key length header: " + keyLength);
    return keyLength;
  }

  /// Writes the number of records header to the fileOperations.
  private void writeNumRecordsHeader(int numRecords) throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.NUM_RECORDS_HEADER_LOCATION);
    fileOperations.writeInt(numRecords);
  }

  /// Reads the number of records header from the fileOperations.
  private int readNumRecordsHeader() throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.NUM_RECORDS_HEADER_LOCATION);
    return fileOperations.readInt();
  }

  /// Writes the data start pointer header to the fileOperations.
  private void writeDataStartPtrHeader(long dataStartPtr) throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.DATA_START_HEADER_LOCATION);
    fileOperations.writeLong(dataStartPtr);
  }

  /// Reads the data start pointer header from the fileOperations.
  private long readDataStartHeader() throws IOException {
    fileOperations.seek(FileRecordStoreBuilder.DATA_START_HEADER_LOCATION);
    return fileOperations.readLong();
  }

  /// Loads existing index entries from file into memory.
  private void loadExistingIndex(int numRecords) throws IOException {
    for (int i = 0; i < numRecords; i++) {
      KeyWrapper key = readKeyFromIndex(i);
      RecordHeader header = readRecordHeaderFromIndex(i);

      headerState.update(
          key,
          null,
          header.dataPointer(),
          header.dataLength(),
          header.dataCapacity(),
          header.indexPosition());
      updateFreeSpaceIndex(header);
    }
  }

  /// Reads the ith key from the index.
  private KeyWrapper readKeyFromIndex(int position) throws IOException {
    final var fp = indexPositionToKeyFp(position);
    fileOperations.seek(fp);

    final var keyLengthShort = fileOperations.readShort();
    final int len = keyLengthShort & 0xFFFF; // interpret as unsigned short
    final int entrySize = Short.BYTES + maxKeyLength + CRC32_LENGTH;

    // FINER logging: Key and envelope details coming from disk //// TODO make it aggressively as
    // small as possible and only make larger if it passes until it is back to failing
    FileRecordStore.logger.log(
        Level.FINER,
        () ->
            String.format(
                "key and envelope details: position=%d, width=%d, fp=%d, writeLen=%d",
                position, len, fp, entrySize));

    assert len <= maxKeyLength : String.format("%d > %d", len, maxKeyLength);

    byte[] paddedKey = new byte[maxKeyLength];
    fileOperations.readFully(paddedKey);
    byte[] key = Arrays.copyOf(paddedKey, len);

    byte[] crcBytes = new byte[CRC32_LENGTH];
    fileOperations.readFully(crcBytes);
    ByteBuffer buffer = ByteBuffer.allocate(CRC32_LENGTH);
    buffer.put(crcBytes);
    buffer.flip();
    long crc32expected =
        buffer.getInt() & 0xffffffffL; // https://stackoverflow.com/a/22938125/329496

    // Compute CRC over key length (short) + key bytes using RecordHeader static method
    final var crc32actual = RecordHeader.computeKeyCrc(key);

    // FINEST logging: Key and envelope details with first and last 128 bytes //// TODO make it
    // aggressively as small as possible and only make larger if it passes until it is back to
    // failing
    FileRecordStore.logger.log(
        Level.FINEST,
        () ->
            String.format(
                "key and envelope details: position=%d, width=%d, fp=%d, writeLen=%d, expectedCrc32=%d, actualCrc32=%d, firstLast128Bytes=%s",
                position, len, fp, entrySize, crc32expected, crc32actual, printFirstLast128(key)));

    if (crc32actual != crc32expected) {
      throw new IllegalStateException(
          String.format(
              "key and envelope details: position=%d, width=%d, fp=%d, expectedCrc32=%d, actualCrc32=%d, firstLast128Bytes=%s",
              position, len, fp, crc32expected, crc32actual, printFirstLast128(key)));
    }

    return KeyWrapper.of(key, defensiveCopy);
  }

  /// Reads the ith record header from the index.
  private RecordHeader readRecordHeaderFromIndex(int index) throws IOException {
    fileOperations.seek(indexPositionToRecordHeaderFp(index));
    return read(index, fileOperations);
  }

  /// Updates a map of record headers to free space values.
  ///
  /// @param rh Record that has new free space.
  private void updateFreeSpaceIndex(RecordHeader rh) {
    int free = rh.getFreeSpace(disableCrc32);
    if (free > 0) {
      freeMap.put(rh, free);
    } else {
      freeMap.remove(rh);
    }
  }

  /// Returns a file pointer in the index pointing to the first byte in the key
  /// located at the given index position.
  private long indexPositionToKeyFp(int pos) {
    return FileRecordStoreBuilder.FILE_HEADERS_REGION_LENGTH + ((long) indexEntryLength * pos);
  }

  static String print(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (byte b : bytes) {
      sb.append(String.format("0x%02X ", b));
    }
    sb.append("]");
    return sb.toString();
  }

  /// Formats byte array showing first and last 128 bytes with ellipsis in between for large arrays.
  /// Used for logging key data to show both beginning and end of potentially large keys.
  static String printFirstLast128(byte[] bytes) {
    if (bytes.length <= 256) {
      return print(bytes);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("[ ");

    // First 128 bytes
    for (int i = 0; i < 128; i++) {
      sb.append(String.format("0x%02X ", bytes[i]));
    }

    sb.append("... ");

    // Last 128 bytes
    for (int i = bytes.length - 128; i < bytes.length; i++) {
      sb.append(String.format("0x%02X ", bytes[i]));
    }

    sb.append("]");
    return sb.toString();
  }

  /// Comprehensive debug string for RecordHeader with optional data preview
  static String toDebugString(RecordHeader header) {
    if (header == null) return "RecordHeader[null]";
    return "RecordHeader["
        + "dp="
        + header.dataPointer()
        + ",dl="
        + header.dataLength()
        + ",dc="
        + header.dataCapacity()
        + ",ip="
        + header.indexPosition()
        + ",crc="
        + String.format("%08x", header.crc32())
        + "]";
  }

  /// Debug logging helper with consistent format - handles log level checking internally
  private void logDebug(String method, String message, Object... args) {
    // Fast path: check log level once to avoid lambda creation overhead
    if (logger.isLoggable(Level.FINEST)) {
      logger.log(
          Level.FINEST, () -> String.format("DEBUG %s: %s", method, String.format(message, args)));
    }
  }

  /// Debug logging for state changes - only logs if FINEST is enabled
  @SuppressWarnings("SameParameterValue")
  private void logStateChange(
      String operation, KeyWrapper key, RecordHeader oldHeader, RecordHeader newHeader) {
    if (logger.isLoggable(Level.FINEST)) {
      if (oldHeader == null && newHeader == null) {
        logger.log(
            Level.FINEST,
            () -> String.format("STATE %s: key=%s, both null", operation, print(key.bytes())));
      } else if (oldHeader == null) {
        logger.log(
            Level.FINEST,
            () ->
                String.format(
                    "STATE %s: key=%s, old=null, new=%s",
                    operation, print(key.bytes()), toDebugString(newHeader)));
      } else if (newHeader == null) {
        logger.log(
            Level.FINEST,
            () ->
                String.format(
                    "STATE %s: key=%s, old=%s, new=null",
                    operation, print(key.bytes()), toDebugString(oldHeader)));
      } else {
        logger.log(
            Level.FINEST,
            () ->
                String.format(
                    "STATE %s: key=%s, old=%s, new=%s",
                    operation,
                    print(key.bytes()),
                    toDebugString(oldHeader),
                    toDebugString(newHeader)));
      }
    }
  }

  /// Returns a file pointer in the index pointing to the first byte in the
  /// record pointer located at the given index position.
  private long indexPositionToRecordHeaderFp(int pos) {
    return indexPositionToKeyFp(pos) + Short.BYTES + maxKeyLength + CRC32_LENGTH;
  }

  private static RecordHeader read(int index, FileOperations in) throws IOException {
    final var fp = in.getFilePointer();

    // Use RecordHeader static method for consistent deserialization with CRC validation
    RecordHeader rh = RecordHeader.readFrom(in, index);

    final int ACTUAL_ENVELOPE_SIZE = RecordHeader.ENVELOPE_SIZE - Short.BYTES;
    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "<h fp:%d idx:%d len:%d %s",
                fp, index, ACTUAL_ENVELOPE_SIZE, RecordHeader.formatForLog(rh, null)));

    return rh;
  }

  static int getMaxKeyLengthOrDefault() {
    final String key = String.format("%s.%s", FileRecordStore.class.getName(), "MAX_KEY_LENGTH");
    String keyLength =
        System.getenv(key) == null
            ? Integer.valueOf(DEFAULT_MAX_KEY_LENGTH).toString()
            : System.getenv(key);
    keyLength = System.getProperty(key, keyLength);
    return Integer.parseInt(keyLength);
  }

  @SuppressWarnings("unused")
  @SneakyThrows
  static String print(File f) {
    try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(f.getAbsolutePath(), "r")) {
      final var len = file.length();
      assert len < Integer.MAX_VALUE;
      final var bytes = new byte[(int) len];
      file.readFully(bytes);
      return print(bytes);
    }
  }

  /// Command-line utility to dump the contents of a record store file.
  ///
  /// @param args command line arguments: filename
  /// @throws Exception if an error occurs during file processing
  public static void main(String[] args) throws Exception {
    if (args.length == 0 || Arrays.asList(args).contains("--help")) {
      printDumpUsage();
      return;
    }

    final String filename = args[0];
    DumpCommand command = DumpCommand.DUMP;
    Level level = Level.INFO;
    boolean disableCrc32 = false;
    boolean validateStructure = false;
    boolean validatePayloads = false;

    for (int i = 1; i < args.length; i++) {
      final String arg = args[i];
      if ("dump".equalsIgnoreCase(arg)) {
        command = DumpCommand.DUMP;
      } else if ("keys".equalsIgnoreCase(arg)) {
        command = DumpCommand.KEYS;
      } else if ("summary".equalsIgnoreCase(arg)) {
        command = DumpCommand.SUMMARY;
      } else if (arg.startsWith("--level=")) {
        final String value = arg.substring("--level=".length()).toUpperCase(Locale.ROOT);
        try {
          level = Level.parse(value);
        } catch (IllegalArgumentException ex) {
          System.err.println("Unknown level '" + value + "'");
          printDumpUsage();
          return;
        }
      } else if ("--disable-crc".equalsIgnoreCase(arg)) {
        disableCrc32 = true;
      } else if ("--validate-structure".equalsIgnoreCase(arg)) {
        validateStructure = true;
      } else if ("--validate-payloads".equalsIgnoreCase(arg)) {
        validateStructure = true;
        validatePayloads = true;
      } else {
        System.err.println("Unknown argument: " + arg);
        printDumpUsage();
        return;
      }
    }

    final DumpCommand dumpCommand = command;
    logger.log(
        Level.INFO, () -> String.format("Dumping %s using command %s", filename, dumpCommand));
    dumpFile(filename, dumpCommand, level, disableCrc32, validateStructure, validatePayloads);
  }

  private static void printDumpUsage() {
    System.out.println(
        "Usage: java "
            + FileRecordStore.class.getName()
            + " <file> [summary|keys|dump] [--level=<LEVEL>] [--disable-crc]\n"
            + "  summary   - file level information only\n"
            + "  keys      - summary plus index/key details\n"
            + "  dump      - keys plus record payload previews (default)\n"
            + "  --level   - JUL logging level (INFO, FINE, FINER, FINEST, ...)\n"
            + "  --disable-crc - open store with CRC disabled (for corrupted payload inspection)\n"
            + "  --validate-structure - scan on-disk headers for overlap/corruption\n"
            + "  --validate-payloads - scan structure and verify payload CRCs\n");
  }

  enum DumpCommand {
    SUMMARY(false, false),
    KEYS(true, false),
    DUMP(true, true);

    final boolean includeKeys;
    final boolean includeData;

    DumpCommand(boolean includeKeys, boolean includeData) {
      this.includeKeys = includeKeys;
      this.includeData = includeData;
    }
  }

  static void dumpFile(
      String filename,
      DumpCommand command,
      Level level,
      boolean disableCrc,
      boolean validateStructure,
      boolean validatePayloads)
      throws IOException {
    try (FileRecordStore recordFile =
        FileRecordStore.Builder()
            .path(filename)
            .accessMode(AccessMode.READ_ONLY)
            .disablePayloadCrc32(disableCrc)
            .open()) {

      final StoreState storeState = recordFile.state;
      final int recordCount = recordFile.getNumRecords();
      final long fileLength = recordFile.getFileLength();
      final long dataStartPtr = recordFile.dataStartPtr;
      final int preferredExpansion = recordFile.preferredExpansionSize;
      final int preferredBlock = recordFile.preferredBlockSize;
      final boolean allowHeaderExpansion = recordFile.isAllowHeaderExpansion();
      final boolean allowInPlaceUpdates = recordFile.isAllowInPlaceUpdates();

      emitDump(
          level,
          () ->
              String.format(
                  Locale.ROOT,
                  "summary: state=%s, records=%d, fileLength=%d, dataStartPtr=%d, preferredExpansionSize=%d, preferredBlockSize=%d, allowHeaderExpansion=%s, allowInPlaceUpdates=%s",
                  storeState,
                  recordCount,
                  fileLength,
                  dataStartPtr,
                  preferredExpansion,
                  preferredBlock,
                  allowHeaderExpansion,
                  allowInPlaceUpdates));

      List<IndexEntry> entries = new ArrayList<>(recordCount);

      for (int index = 0; index < recordFile.getNumRecords(); index++) {
        final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
        final KeyWrapper keyWrapper = recordFile.readKeyFromIndex(index);
        entries.add(new IndexEntry(index, keyWrapper, header));

        if (command.includeKeys) {
          final int finalIndex = index;
          final String headerLog = RecordHeader.formatForLog(header, null);
          final String keyLog = formatKeyForLog(keyWrapper);
          emitDump(
              level,
              () ->
                  String.format(
                      Locale.ROOT, "record[%d]: header=%s key=%s", finalIndex, headerLog, keyLog));
        }

        if (command.includeData) {
          final int finalIndex = index;
          final byte[] keyBytes = keyWrapper.bytes();
          final byte[] data = recordFile.readRecordData(keyBytes);
          final String dataPreview = printFirstLast128(data);
          emitDump(
              level,
              () ->
                  String.format(
                      Locale.ROOT,
                      "record[%d]: dataLength=%d dataPreview=%s",
                      finalIndex,
                      data.length,
                      dataPreview));
        }
      }

      if (validateStructure || validatePayloads) {
        validateOnDiskStructure(recordFile, entries, validatePayloads, level);
      }
    }
  }

  private static void emitDump(Level level, Supplier<String> messageSupplier) {
    final String message = messageSupplier.get();
    logger.log(level, message);
    System.out.println(message);
  }

  private static void emitIssue(Level level, String message) {
    emitDump(level, () -> message);
  }

  private static String formatKeyForLog(KeyWrapper keyWrapper) {
    byte[] keyBytes = keyWrapper.bytes();
    String base64 = Base64.getEncoder().encodeToString(keyBytes);
    return String.format(
        Locale.ROOT,
        "len=%d ascii=\"%s\" base64=%s hexPreview=%s",
        keyBytes.length,
        asciiPreview(keyBytes),
        base64,
        printFirstLast128(keyBytes));
  }

  private static String asciiPreview(byte[] bytes) {
    int limit = Math.min(bytes.length, 64);
    StringBuilder ascii = new StringBuilder(limit + 3);
    for (int i = 0; i < limit; i++) {
      int ch = Byte.toUnsignedInt(bytes[i]);
      if (ch >= 0x20 && ch <= 0x7E) {
        ascii.append((char) ch);
      } else {
        ascii.append('.');
      }
    }
    if (bytes.length > limit) {
      ascii.append('…');
    }
    return ascii.toString();
  }

  private static void validateOnDiskStructure(
      FileRecordStore recordFile, List<IndexEntry> entries, boolean validatePayloads, Level level)
      throws IOException {

    emitDump(
        level, () -> String.format(Locale.ROOT, "validation: scanning %d records", entries.size()));

    if (validatePayloads && recordFile.disableCrc32) {
      emitIssue(
          Level.WARNING, "validation: payload CRC validation requested but store has CRC disabled");
    }

    long headerRegionEnd = recordFile.indexPositionToKeyFp(recordFile.getNumRecords());
    long fileLength = recordFile.getFileLength();
    long dataStartPtr = recordFile.dataStartPtr;

    int issueCount = 0;
    Set<Long> dataPointers = new HashSet<>();
    Set<Integer> indexPositions = new HashSet<>();

    for (IndexEntry entry : entries) {
      RecordHeader header = entry.header();

      if (!indexPositions.add(header.indexPosition())) {
        issueCount++;
        emitIssue(
            Level.WARNING,
            String.format(
                Locale.ROOT,
                "validation issue: duplicate index position %d",
                header.indexPosition()));
      }

      if (header.indexPosition() != entry.index()) {
        issueCount++;
        emitIssue(
            Level.WARNING,
            String.format(
                Locale.ROOT,
                "validation issue: header indexPosition=%d but located at logical index=%d",
                header.indexPosition(),
                entry.index()));
      }

      if (!dataPointers.add(header.dataPointer())) {
        issueCount++;
        emitIssue(
            Level.WARNING,
            String.format(
                Locale.ROOT, "validation issue: duplicate dataPointer=%d", header.dataPointer()));
      }

      if (header.dataPointer() < headerRegionEnd) {
        issueCount++;
        emitIssue(
            Level.WARNING,
            String.format(
                Locale.ROOT,
                "validation issue: dataPointer=%d overlaps header region end=%d",
                header.dataPointer(),
                headerRegionEnd));
      }

      long dataEnd = header.dataPointer() + header.dataCapacity();
      if (dataEnd > fileLength) {
        issueCount++;
        emitIssue(
            Level.SEVERE,
            String.format(
                Locale.ROOT,
                "validation issue: record at index %d exceeds file length (end=%d > fileLength=%d)",
                entry.index(),
                dataEnd,
                fileLength));
      }
    }

    List<IndexEntry> sorted = new ArrayList<>(entries);
    sorted.sort(Comparator.comparingLong(e -> e.header().dataPointer()));

    long previousEnd = Long.MIN_VALUE;
    long minPointer = Long.MAX_VALUE;

    for (IndexEntry entry : sorted) {
      RecordHeader header = entry.header();
      long start = header.dataPointer();
      long end = header.dataPointer() + header.dataCapacity();

      if (previousEnd > start) {
        issueCount++;
        emitIssue(
            Level.SEVERE,
            String.format(
                Locale.ROOT,
                "validation issue: data overlap between records (previousEnd=%d, start=%d, index=%d)",
                previousEnd,
                start,
                entry.index()));
      }

      previousEnd = Math.max(previousEnd, end);
      minPointer = Math.min(minPointer, start);
    }

    if (!sorted.isEmpty() && minPointer < dataStartPtr) {
      issueCount++;
      emitIssue(
          Level.WARNING,
          String.format(
              Locale.ROOT,
              "validation issue: smallest dataPointer=%d is below dataStartPtr=%d",
              minPointer,
              dataStartPtr));
    }

    if (validatePayloads && !recordFile.disableCrc32) {
      emitDump(level, () -> "validation: verifying payload CRCs using store configuration");
      for (IndexEntry entry : entries) {
        try {
          recordFile.readRecordData(entry.key().bytes());
        } catch (IOException ex) {
          issueCount++;
          emitIssue(
              Level.SEVERE,
              String.format(
                  Locale.ROOT,
                  "validation issue: payload read failed for index %d: %s",
                  entry.index(),
                  ex.getMessage()));
        }
      }
    }

    final int finalIssueCount = issueCount;
    emitDump(
        level,
        () ->
            String.format(Locale.ROOT, "validation: completed with %d issue(s)", finalIssueCount));
  }

  private record IndexEntry(int index, KeyWrapper key, RecordHeader header) {}

  long getFileLength() throws IOException {
    return fileOperations.length();
  }

  /// Returns the current number of records in the database.
  @Synchronized
  int getNumRecords() {
    return headerState.size();
  }

  /// Reads the data for the record with the specified key.
  ///
  /// @param key the key of the record to read
  /// @return the data stored for the specified key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  public byte[] readRecordData(byte[] key) throws IOException {
    ensureOpen();
    try {
      logger.log(Level.FINE, () -> String.format("readRecordData key:%s", print(key)));
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
      final var header = keyToRecordHeader(keyWrapper);
      return readRecordData(header);
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Reads the data for the record with the specified UUID key.
  /// Optimized for UUID keys when store is configured for UUID mode.
  ///
  /// @param key the UUID key of the record to read
  /// @return the data stored for the specified key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  /// @throws UnsupportedOperationException if store is not in UUID mode
  public byte[] readRecordData(UUID key) throws IOException {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    try {
      logger.log(Level.FINE, () -> String.format("readRecordData UUID key:%s", key));
      final var keyWrapper = KeyWrapper.of(key);
      final var header = keyToRecordHeader(keyWrapper);
      return readRecordData(header);
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Ensures the store is open, throwing IllegalStateException if not in OPEN state.
  private void ensureOpen() {
    if (state != StoreState.OPEN) {
      throw new IllegalStateException("Store is in state " + state + ", expected OPEN");
    }
  }

  /*
   * Maps a key to a record header by looking it up in the in-memory index.
   */
  private RecordHeader keyToRecordHeader(KeyWrapper key) {
    RecordHeader h = headerState.getByKey(key);
    if (h == null) {
      throw new IllegalArgumentException(String.format("Key not found %s", print(key.bytes())));
    }
    return h;
  }

  /// Reads the record data for the given record header.
  private byte[] readRecordData(RecordHeader header) throws IOException {
    // read the length
    fileOperations.seek(header.dataPointer());
    byte[] lenBytes = new byte[Integer.BYTES];
    fileOperations.readFully(lenBytes);
    int len = (new DataInputStream(new ByteArrayInputStream(lenBytes))).readInt();

    logger.log(
        Level.FINEST,
        () ->
            String.format("<d fp:%d len:%d bytes:%s ", header.dataPointer(), len, print(lenBytes)));

    assert header.dataPointer() + len < getFileLength()
        : String.format(
            "attempting to read up to %d beyond length of file %d",
            (header.dataLength() + len), getFileLength());

    // read the body
    byte[] buf = new byte[len];
    fileOperations.readFully(buf);

    if (!disableCrc32) {
      byte[] crcBytes = new byte[CRC32_LENGTH];
      fileOperations.readFully(crcBytes);
      final var expectedCrc =
          (new DataInputStream(new ByteArrayInputStream(crcBytes))).readInt() & 0xffffffffL;
      long actualCrc = RecordHeader.computeCrc32(buf, buf.length);

      logger.log(
          Level.FINEST,
          () ->
              String.format(
                  "<d fp:%d len:%d crc:%d bytes:%s",
                  header.dataPointer() + Integer.BYTES, len, actualCrc, print(buf)));

      if (actualCrc != expectedCrc) {
        throw new IllegalStateException(
            String.format(
                "CRC32 check failed expected %d got %d for data length %d with header %s",
                expectedCrc, actualCrc, buf.length, header));
      }
    }

    return buf;
  }

  /// Inserts a new record with a UUID key. Optimized for 16-byte UUID storage.
  ///
  /// @param key the UUID key for the new record
  /// @param value the data to store for the key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key already exists
  /// @throws UnsupportedOperationException if store is not in UUID mode
  @Synchronized
  public void insertRecord(UUID key, byte[] value) throws IOException {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    try {
      ensureNotReadOnly();
      logger.log(
          Level.FINE,
          () -> String.format("insertRecord UUID value.len:%d key:%s ", value.length, key));
      final var keyWrapper = KeyWrapper.of(key);
      if (recordExists(key)) {
        throw new IllegalArgumentException("Key exists: " + key);
      }
      ensureIndexSpace(getNumRecords() + 1);
      RecordHeader newRecord = allocateRecord(payloadLength(value.length));
      writeRecordData(newRecord, value);
      addEntryToIndex(keyWrapper, newRecord, getNumRecords());
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Common implementation for updating records with new data.
  /// Handles all the update logic including in-place updates, file expansion, and record moves.
  ///
  /// @param keyWrapper the wrapped key for the record to update
  /// @param updateMeHeader the current header of the record being updated
  /// @param value the new data to store
  /// @throws IOException if an I/O error occurs
  private void updateRecordInternal(
      KeyWrapper keyWrapper, RecordHeader updateMeHeader, byte[] value) throws IOException {
    logDebug(
        "updateRecordInternal",
        "ENTER: current=%s, newDataLen=%d",
        toDebugString(updateMeHeader),
        value.length);

    final var capacity = updateMeHeader.dataCapacity();
    final var recordIsSameSize = value.length == capacity;
    final var recordIsSmaller = value.length < capacity;

    logger.log(
        Level.FINER,
        () ->
            String.format(
                "updateRecordInternal: capacity=%d, sameSize=%b, smaller=%b, allowInPlace=%b",
                capacity, recordIsSameSize, recordIsSmaller, allowInPlaceUpdates));

    // can update in place if the record is same size no matter whether CRC32 is enabled.
    // for smaller records, allow in-place updates based on the allowInPlaceUpdates setting
    if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
      logger.log(Level.FINER, () -> "updateRecordInternal: taking INPLACE path");
      logDebug("updateRecordInternal", "INPLACE: current=%s", toDebugString(updateMeHeader));

      // write with the backup crc so one of the two CRCs will be valid after a crash
      writeRecordHeaderToIndex(updateMeHeader);

      final var updatedHeader = RecordHeader.withDataCount(updateMeHeader, value.length);
      logDebug("updateRecordInternal", "INPLACE: updated=%s", toDebugString(updatedHeader));
      updateFreeSpaceIndex(updatedHeader);

      // write the main data
      writeRecordData(updatedHeader, value);

      // write the header with the main CRC - this will update the state
      writeRecordHeaderToIndex(updatedHeader);

      logStateChange("updateRecordInternal", keyWrapper, updateMeHeader, updatedHeader);

    } else { // Handle cases where in-place update is not possible
      final var endOfRecord = updateMeHeader.dataPointer() + updateMeHeader.dataCapacity();
      final var fileLength = getFileLength();

      if (endOfRecord == fileLength) {
        logger.log(Level.FINER, () -> "updateRecordInternal: taking EXTEND path");
        logDebug(
            "updateRecordInternal",
            "EXTEND: current=%s end=%d fileLen=%d",
            toDebugString(updateMeHeader),
            endOfRecord,
            fileLength);

        long length = fileLength + (value.length - updateMeHeader.dataCapacity());
        final var newDataCapacity = payloadLength(value.length);
        final var movedHeader = RecordHeader.move(updateMeHeader, length, newDataCapacity);
        final var updatedHeader = RecordHeader.withDataCount(movedHeader, value.length);
        logDebug(
            "updateRecordInternal",
            "EXTEND: moved=%s, final=%s",
            toDebugString(movedHeader),
            toDebugString(updatedHeader));

        setFileLength(length);
        updateFreeSpaceIndex(updatedHeader);
        writeRecordData(updatedHeader, value);
        writeRecordHeaderToIndex(updatedHeader);

        // CRITICAL: Update both in-memory indexes to point to the new header
        headerState.update(keyWrapper, updateMeHeader, updatedHeader);

        logStateChange("updateRecordInternal", keyWrapper, updateMeHeader, updatedHeader);

      } else if (value.length > updateMeHeader.dataCapacity()) {
        logger.log(Level.FINER, () -> "updateRecordInternal: taking ALLOCATE path");
        logDebug(
            "updateRecordInternal",
            "ALLOCATE: current=%s newDataLen=%d value.length=%d",
            toDebugString(updateMeHeader),
            value.length,
            value.length);

        // allocate to next free space or expand the file
        final var dataLength = value.length;
        logDebug(
            "updateRecordInternal",
            "ALLOCATE: calling allocateRecord with dataLength=%d",
            dataLength);
        RecordHeader newRecord = allocateRecord(dataLength);
        logDebug("updateRecordInternal", "ALLOCATE: allocated=%s", toDebugString(newRecord));

        // new record is expanded old record - use it directly as allocateRecord already set correct
        // capacity
        writeRecordData(newRecord, value);

        writeRecordHeaderToIndex(newRecord);
        headerState.update(keyWrapper, updateMeHeader, newRecord);
        logStateChange("updateRecordInternal", keyWrapper, updateMeHeader, newRecord);
        updateFreeSpaceIndex(newRecord);

        // if there is a previous record add space to it
        final var previousIndex = updateMeHeader.dataPointer() - 1;
        final var previousOptional = getRecordAt(previousIndex);

        if (previousOptional.isPresent()) {
          RecordHeader previous = previousOptional.get();
          logDebug(
              "updateRecordInternal",
              "ALLOCATE: freeing space to previous=%s",
              toDebugString(previous));
          previous.incrementDataCapacity(updateMeHeader.dataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          logDebug(
              "updateRecordInternal",
              "ALLOCATE: freeing space at dataStartPtr=%d",
              updateMeHeader.dataPointer());
          writeDataStartPtrHeader(updateMeHeader.dataPointer());
        }

      } else {
        logger.log(Level.FINER, () -> "updateRecordInternal: taking MOVE path");
        logDebug(
            "updateRecordInternal",
            "MOVE: current=%s newDataLen=%d",
            toDebugString(updateMeHeader),
            value.length);

        // Not last record - need to move to new location
        // This handles both larger records and smaller records when in-place updates are disabled
        RecordHeader newRecord = allocateRecord(value.length);
        logDebug("updateRecordInternal", "MOVE: allocated=%s", toDebugString(newRecord));

        // new record is expanded/moved old record
        final var updatedNewRecord = RecordHeader.withDataCount(newRecord, value.length);
        logDebug("updateRecordInternal", "MOVE: updated=%s", toDebugString(updatedNewRecord));

        writeRecordData(updatedNewRecord, value);
        writeRecordHeaderToIndex(updatedNewRecord);
        headerState.update(keyWrapper, updateMeHeader, updatedNewRecord);
        logStateChange("updateRecordInternal", keyWrapper, updateMeHeader, updatedNewRecord);
        updateFreeSpaceIndex(updatedNewRecord);

        // if there is a previous record add space to it
        final var previousIndex = updateMeHeader.dataPointer() - 1;
        final var previousOptional = getRecordAt(previousIndex);

        if (previousOptional.isPresent()) {
          RecordHeader previous = previousOptional.get();
          logDebug(
              "updateRecordInternal",
              "MOVE: freeing space to previous=%s",
              toDebugString(previous));
          previous.incrementDataCapacity(updateMeHeader.dataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          logDebug(
              "updateRecordInternal",
              "MOVE: freeing space at dataStartPtr=%d",
              updateMeHeader.dataPointer());
          writeDataStartPtrHeader(updateMeHeader.dataPointer());
        }
      }
    }

    logDebug("updateRecordInternal", "EXIT: completed");
  }

  /// Updates an existing record with a UUID key.
  ///
  /// @param key the UUID key of the record to update
  /// @param value the new data to store for the key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  /// @throws UnsupportedOperationException if store is not in UUID mode
  @Synchronized
  public void updateRecord(UUID key, byte[] value) throws IOException {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    try {
      ensureNotReadOnly();
      logger.log(
          Level.FINE,
          () -> String.format("updateRecord UUID value.len:%d key:%s", value.length, key));
      final var keyWrapper = KeyWrapper.of(key);
      final var updateMeHeader = keyToRecordHeader(keyWrapper);
      final var capacity = updateMeHeader.dataCapacity();

      final var recordIsSameSize = value.length == capacity;
      final var recordIsSmaller = value.length < capacity;

      // can update in place if the record is same size no matter whether CRC32 is enabled.
      // for smaller records, allow in-place updates based on the allowInPlaceUpdates setting
      if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
        // write with the backup crc so one of the two CRCs will be valid after a crash
        writeRecordHeaderToIndex(updateMeHeader);
        final var updatedHeader = RecordHeader.withDataCount(updateMeHeader, value.length);
        updateFreeSpaceIndex(updatedHeader);
        // write the main data
        writeRecordData(updatedHeader, value);
        // write the header with the main CRC - this will update the state
        writeRecordHeaderToIndex(updatedHeader);
      } else { // Handle cases where in-place update is not possible
        final var endOfRecord = updateMeHeader.dataPointer() + updateMeHeader.dataCapacity();
        final var fileLength =
            getFileLength(); // perform a move. insert data to the end of the file then overwrite
        // header.
        if (endOfRecord == fileLength) {
          long length = fileLength + (value.length - updateMeHeader.dataCapacity());
          final var updatedHeader = RecordHeader.move(updateMeHeader, length, value.length);
          setFileLength(length);
          updateFreeSpaceIndex(updatedHeader);
          writeRecordData(updatedHeader, value);
          writeRecordHeaderToIndex(updatedHeader);
        } else if (value.length > updateMeHeader.dataCapacity()) {
          // allocate to next free space or expand the file
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded old record
          final var updatedNewRecord = RecordHeader.withDataCount(newRecord, value.length);
          writeRecordData(updatedNewRecord, value);
          writeRecordHeaderToIndex(updatedNewRecord);
          headerState.update(keyWrapper, updateMeHeader, updatedNewRecord);
          updateFreeSpaceIndex(updatedNewRecord);

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer() - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.dataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer());
          }
        } else {
          // Not last record - need to move to new location
          // This handles both larger records and smaller records when in-place updates are disabled
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded/moved old record
          final var updatedNewRecord = RecordHeader.withDataCount(newRecord, value.length);
          writeRecordData(updatedNewRecord, value);
          writeRecordHeaderToIndex(updatedNewRecord);
          headerState.update(keyWrapper, updateMeHeader, updatedNewRecord);
          updateFreeSpaceIndex(updatedNewRecord);

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer() - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.dataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer());
          }
        }
      }
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Deletes the record with the specified UUID key.
  ///
  /// @param key the UUID key of the record to delete
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  /// @throws UnsupportedOperationException if store is not in UUID mode
  @Synchronized
  public void deleteRecord(UUID key) throws IOException {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    try {
      ensureNotReadOnly();
      logger.log(Level.FINE, () -> String.format("deleteRecord UUID key:%s", key));
      final var keyWrapper = KeyWrapper.of(key);
      RecordHeader delRec = keyToRecordHeader(keyWrapper);
      int currentNumRecords = getNumRecords();
      // Remove from maps first before index manipulation
      headerState.remove(keyWrapper, delRec);
      freeMap.remove(delRec);
      // Now manipulate the index file
      deleteEntryFromIndex(delRec, currentNumRecords);

      if (getFileLength() == delRec.dataPointer() + delRec.dataCapacity()) {
        // shrink file since this is the last record in the file
        setFileLength(delRec.dataPointer());
      } else {
        final var previousOptional = getRecordAt(delRec.dataPointer() - 1);
        if (previousOptional.isPresent()) {
          // append space of deleted record onto previous record
          final var previous = previousOptional.get();
          previous.incrementDataCapacity(delRec.dataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          // make free space at the end of the index area
          writeDataStartPtrHeader(delRec.dataPointer() + delRec.dataCapacity());
        }
      }
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  private void setFileLength(long l) throws IOException {
    fileOperations.setLength(l);
  }

  /// Checks if the store has been closed due to an error.
  ///
  /// @return true if the store is closed, false if it's open
  public boolean isClosed() {
    return state != StoreState.OPEN;
  }

  /// Gets the current state of the store.
  StoreState getState() {
    return state;
  }

  /// Generates a defensive copy of all the keys in a thread safe manner.
  /// Returns byte arrays for BYTE_ARRAY mode, or UUIDs converted from 16-byte arrays for UUID mode.
  ///
  /// @return an iterable collection of all keys in the store
  public Iterable<byte[]> keysBytes() {
    ensureOpen();
    final var snapshot = headerState.keySet();
    if (keyType == KeyType.UUID) {
      return snapshot.stream()
          .map(KeyWrapper::toUUID)
          .map(
              uuid -> {
                byte[] bytes = new byte[16];
                ByteBuffer.wrap(bytes)
                    .putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits());
                return bytes;
              })
          .collect(Collectors.toSet());
    } else {
      return snapshot.stream().map(KeyWrapper::copyBytes).collect(Collectors.toSet());
    }
  }

  /// Returns all UUID keys when store is in UUID mode.
  ///
  /// @return an iterable collection of all UUID keys in the store
  /// @throws UnsupportedOperationException if store is not in UUID mode
  public Iterable<UUID> uuidKeys() {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    final var snapshot = headerState.keySet();
    return snapshot.stream().map(KeyWrapper::toUUID).collect(Collectors.toSet());
  }

  /// Checks if the store contains no records.
  ///
  /// @return true if the store is empty, false otherwise
  @Synchronized
  public boolean isEmpty() {
    ensureOpen();
    return headerState.isEmpty();
  }

  /// Checks if there is a record belonging to the given key.
  ///
  /// @param key the key to check
  /// @return true if a record exists for the key, false otherwise
  @Synchronized
  public boolean recordExists(byte[] key) {
    ensureOpen();
    final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
    return headerState.containsKey(keyWrapper);
  }

  /// Checks if there is a record belonging to the given UUID key.
  ///
  /// @param key the UUID key to check
  /// @return true if a record exists for the key, false otherwise
  /// @throws UnsupportedOperationException if store is not in UUID mode
  @Synchronized
  public boolean recordExists(UUID key) {
    ensureOpen();
    if (keyType != KeyType.UUID) {
      throw new UnsupportedOperationException(
          "UUID operations only supported when store is configured with uuidKeys()");
    }
    final var keyWrapper = KeyWrapper.of(key);
    return headerState.containsKey(keyWrapper);
  }

  /// Searches for free space without triggering expansion.
  /// Returns null if no free space is found.
  private RecordHeader findFreeRecord(int payloadLength) throws IOException {
    // FIFO deletes cause free space after the index.
    long dataStart = dataStartPtr;
    long endIndexPtr = indexPositionToKeyFp(getNumRecords());
    // we prefer speed overs space so we leave space for the header for this insert plus one for
    // future use
    long available = dataStart - endIndexPtr - (2L * indexEntryLength);

    if (payloadLength <= available) {
      RecordHeader newRecord =
          new RecordHeader(dataStart - payloadLength, payloadLength, payloadLength);
      final var finalNewRecord = newRecord;
      logger.log(
          Level.FINEST,
          () ->
              String.format(
                  "DEBUG findFreeRecord: using index space, newRecord=%s", finalNewRecord));
      dataStartPtr = dataStart - payloadLength;
      writeDataStartPtrHeader(dataStartPtr);
      return newRecord;
    }

    // search for empty space in free map
    for (RecordHeader next : this.freeMap.keySet()) {
      int free = next.getFreeSpace(disableCrc32);
      logger.log(
          Level.FINEST,
          () ->
              String.format(
                  "DEBUG findFreeRecord: checking free space, next=%s, free=%d", next, free));
      if (payloadLength <= free) {
        // Split the existing record to create space for our new data
        RecordHeader freeSpaceHeader = next.split(disableCrc32, payloadLength(0));
        logger.log(
            Level.FINEST,
            () ->
                String.format(
                    "DEBUG findFreeRecord: split result, freeSpaceHeader=%s", freeSpaceHeader));
        // Create a proper data record header with correct data length
        RecordHeader newRecord =
            new RecordHeader(
                freeSpaceHeader.dataPointer(), payloadLength, freeSpaceHeader.dataCapacity());
        final var finalNewRecord2 = newRecord;
        logger.log(
            Level.FINEST,
            () -> String.format("DEBUG findFreeRecord: created newRecord=%s", finalNewRecord2));

        // Reduce original record's capacity to complete the split
        KeyWrapper key = readKeyFromIndex(next.indexPosition());
        RecordHeader updatedNext = RecordHeader.withDataCapacity(next, next.dataLength());
        logger.log(
            Level.FINEST,
            () ->
                String.format(
                    "DEBUG findFreeRecord: reduced capacity, updatedNext=%s", updatedNext));

        freeMap.remove(next);
        headerState.update(key, next, updatedNext);
        updateFreeSpaceIndex(updatedNext);
        writeRecordHeaderToIndex(updatedNext);
        return newRecord;
      }
    }

    return null; // No free space found
  }

  /// This method searches the free map for free space and then returns a
  /// RecordHeader which uses the space.
  private RecordHeader allocateRecord(int dataLength) throws IOException {
    logger.log(
        Level.FINEST, () -> String.format("DEBUG allocateRecord: ENTER dataLength=%d", dataLength));

    // we needs space for the length int and the optional long crc32
    int payloadLength = payloadLength(dataLength);
    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "DEBUG allocateRecord: dataLength=%d, payloadLength=%d",
                dataLength, payloadLength));

    expandHeaderIfNeeded(payloadLength);
    RecordHeader newRecord = findFreeRecord(payloadLength);

    if (newRecord == null) {
      if (!allowHeaderExpansion) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "allocateRecord: Header expansion disabled and no space for payloadLength=%d",
                    payloadLength));
        throw new IllegalStateException(
            "Header expansion disabled - insufficient space for new record");
      }
      // 🔧 Fix: expand file instead of returning null
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "allocateRecord: No free record found for payloadLength=%d - expanding file",
                  payloadLength));
      expandFile(payloadLength);
      // Retry allocation after expansion
      newRecord = findFreeRecord(payloadLength);

      if (newRecord == null) {
        try {
          long currentFileLen = getFileLength();
          long currentDataStart = dataStartPtr;
          int currentRecords = getNumRecords();
          final var logMessage =
              String.format(
                  "allocateRecord: CRITICAL - Still no space after expansion. Current file length=%d, dataStartPtr=%d, numRecords=%d",
                  currentFileLen, currentDataStart, currentRecords);
          logger.log(Level.SEVERE, () -> logMessage);
        } catch (IOException e) {
          final var errorMessage =
              "allocateRecord: CRITICAL - Failed to read current file state: " + e.getMessage();
          logger.log(Level.SEVERE, () -> errorMessage);
        }
        throw new IllegalStateException(
            "allocateRecord failed even after file expansion: dataLength=" + dataLength);
      }
    }
    return newRecord;
  }

  /// Returns the record to which the target file pointer belongs - meaning the
  /// specified location in the file is part of the record data of the
  /// RecordHeader which is returned.
  private Optional<RecordHeader> getRecordAt(long targetFp) {
    final var floor = headerState.getFloorEntry(targetFp);
    Optional<Map.Entry<Long, RecordHeader>> before = (floor != null) ? of(floor) : Optional.empty();
    return before.map(
        entry -> {
          final var rh = entry.getValue();
          if (targetFp >= rh.dataPointer()
              && targetFp < rh.dataPointer() + (long) rh.dataCapacity()) {
            return rh;
          } else {
            return null;
          }
        });
  }

  /// Closes the database.
  @Synchronized
  public void close() throws IOException {
    logger.log(Level.FINE, () -> String.format("closed called on %s", this));
    try {
      try {
        if (fileOperations != null) fileOperations.sync();
        if (fileOperations != null) fileOperations.close();
      } finally {
        fileOperations = null;
      }
    } catch (Exception e) {
      // Exception during close - transition to UNKNOWN state
      state = StoreState.UNKNOWN;
      throw e;
    } finally {
      if (headerState != null) headerState.clear();
      if (freeMap != null) freeMap.clear();
      freeMap = null;
      // Always transition to CLOSED after cleanup, regardless of previous state
      // This ensures consistent state even if exception occurred during close
      this.state = StoreState.CLOSED;
    }
  }

  /// TEST-ONLY: Simulates JVM vanishing without any further disk activity.
  ///
  /// **This method must remain package-private** as it violates the invariants of the class.
  /// **Usage in crash tests**:
  ///
  /// ```java
  /// FileRecordStore zombie = createStoreWithHalt(haltPoint);
  /// try {
  ///   zombie.insertRecord(key, data); // May halt mid-operation
  /// } catch (Exception e) {
  ///   // Expected: halt may leave zombie in UNKNOWN state
  /// }
  /// zombie.terminate(); // Model JVM termination it is now in state UNKNOWN so unsable
  /// zombie = null; /// you should free it for GC.
  ///
  /// // Fresh instance models crash recovery
  /// FileRecordStore fresh = new Builder().path(file).open();
  /// ```
  ///
  /// **Exception handling**: Swallows sync/close exceptions because zombie instances
  /// in UNKNOWN state may fail these operations. Real JVM termination would not throw.
  @TestOnly
  void terminate() {
    // Transition to UNKNOWN (zombie instance no longer usable)
    this.state = StoreState.UNKNOWN;
    // Clear and null in-memory state (models JVM termination)
    if (headerState != null) {
      headerState.clear();
    }
    if (freeMap != null) {
      freeMap.clear();
      freeMap = null;
    }
    logger.log(
        Level.FINE,
        () -> String.format("terminate() succeeded on %s (simulating JVM termination)", this));
  }

  /// Adds the new record to the in-memory index and calls the super class add
  /// the index entry to the fileOperations.
  private void addEntryToIndex(KeyWrapper key, RecordHeader newRecord, int currentNumRecords)
      throws IOException {
    if (key.length() > maxKeyLength) {
      throw new IllegalArgumentException(
          String.format(
              "Key of len %d is larger than permitted max size of %d bytes. You can increase this to %d using env var or system property %s.MAX_KEY_LENGTH",
              key.length(),
              maxKeyLength,
              FileRecordStoreBuilder.MAX_KEY_LENGTH,
              FileRecordStore.class.getName()));
    }

    writeKeyToIndex(key, currentNumRecords);

    if (newRecord == null) {
      throw new IllegalStateException(
          "allocateRecord returned null - unable to allocate space for record");
    }

    // CRASH-SAFE ORDERING: Write header to disk FIRST, then update in-memory state
    final var updatedNewRecord = RecordHeader.withIndexPosition(newRecord, currentNumRecords);
    fileOperations.seek(indexPositionToRecordHeaderFp(currentNumRecords));
    final var writtenHeader = write(updatedNewRecord, fileOperations);

    // Only update the in-memory state AFTER the header is successfully written to disk
    final var duplicate = headerState.getByKey(key);
    headerState.update(
        key,
        duplicate,
        writtenHeader.dataPointer(),
        writtenHeader.dataLength(),
        writtenHeader.dataCapacity(),
        writtenHeader.indexPosition());

    updateFreeSpaceIndex(writtenHeader);

    // Finally update the record count - this is the atomic commit point
    writeNumRecordsHeader(currentNumRecords + 1);

    logger.log(
        Level.FINEST,
        () -> String.format("after state update: headerState.size=%d", headerState.size()));
  }

  private RecordHeader write(RecordHeader rh, FileOperations out) throws IOException {
    if (rh.dataLength() < 0) {
      throw new IllegalStateException("dataLength has not been initialized " + this);
    }
    final var fp = out.getFilePointer();

    logger.log(Level.FINEST, () -> String.format("DEBUG write(): input rh=%s", rh));
    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "DEBUG write(): rh.dataPointer()=%d, rh.dataLength()=%d, rh.dataCapacity()=%d, rh.indexPosition()=%d",
                rh.dataPointer(), rh.dataLength(), rh.dataCapacity(), rh.indexPosition()));

    // Create updated RecordHeader using constructor that computes CRC internally
    RecordHeader updatedRh =
        new RecordHeader(rh.dataPointer(), rh.dataLength(), rh.dataCapacity(), rh.indexPosition());

    logger.log(Level.FINEST, () -> String.format("DEBUG write(): created updatedRh=%s", updatedRh));
    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "DEBUG write(): updatedRh.dataPointer()=%d, updatedRh.dataLength()=%d, updatedRh.dataCapacity()=%d, updatedRh.indexPosition()=%d",
                updatedRh.dataPointer(),
                updatedRh.dataLength(),
                updatedRh.dataCapacity(),
                updatedRh.indexPosition()));

    // Use RecordHeader static method for consistent serialization
    RecordHeader.writeTo(out, updatedRh);

    // Log using FileRecordStore logger for consistent formatting
    final int ACTUAL_ENVELOPE_SIZE = RecordHeader.ENVELOPE_SIZE - Short.BYTES;
    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">h fp:%d idx:%d len:%d end:%d %s",
                fp,
                updatedRh.indexPosition(),
                ACTUAL_ENVELOPE_SIZE,
                fp + ACTUAL_ENVELOPE_SIZE,
                RecordHeader.formatForLog(updatedRh, null)));

    // Return the updated header so caller can use it
    return updatedRh;
  }

  /// Removes the record from the index. Replaces the target with the entry at
  /// the end of the index.
  private void deleteEntryFromIndex(RecordHeader header, int currentNumRecords) throws IOException {
    if (header.indexPosition() != currentNumRecords - 1) {
      final var lastKey = readKeyFromIndex(currentNumRecords - 1);
      RecordHeader last = keyToRecordHeader(lastKey);
      final var updatedLast = RecordHeader.withIndexPosition(last, header.indexPosition());

      writeKeyToIndex(lastKey, updatedLast.indexPosition());

      fileOperations.seek(this.indexPositionToRecordHeaderFp(updatedLast.indexPosition()));
      final var writtenHeader = write(updatedLast, fileOperations);

      // Update the state with the written header (which has correct CRC)
      headerState.update(
          lastKey,
          last,
          writtenHeader.dataPointer(),
          writtenHeader.dataLength(),
          writtenHeader.dataCapacity(),
          writtenHeader.indexPosition());
    }
    writeNumRecordsHeader(currentNumRecords - 1);
  }

  /// Forces all buffered writes to be written to disk.
  ///
  /// @throws IOException if an I/O error occurs during sync
  @Synchronized
  public void fsync() throws IOException {
    ensureOpen();
    try {
      logger.log(Level.FINE, () -> String.format("fsync called on %s", this));
      fileOperations.sync();
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  private void writeKeyToIndex(KeyWrapper key, int index) throws IOException {
    final short len = (short) key.length();
    final int entrySize = Short.BYTES + maxKeyLength + CRC32_LENGTH;

    // FINER logging: Key and envelope details going to disk //// TODO make it aggressively as small
    // as possible and only make larger if it passes until it is back to failing
    final long fpk = indexPositionToKeyFp(index);
    FileRecordStore.logger.log(
        Level.FINER,
        () ->
            String.format(
                "key and envelope details: position=%d, width=%d, fp=%d, writeLen=%d",
                index, len & 0xFFFF, fpk, entrySize));

    ByteBuffer buffer = ByteBuffer.allocate(entrySize);
    buffer.putShort(len);

    byte[] rawKey = key.bytes();
    buffer.put(rawKey, 0, rawKey.length);

    int padding = maxKeyLength - rawKey.length;
    if (padding > 0) {
      buffer.put(new byte[padding]);
    }

    int crc32 = (int) (RecordHeader.computeKeyCrc(rawKey) & 0xFFFFFFFFL);
    buffer.putInt(crc32);

    byte[] entryBytes = buffer.array();

    // FINEST logging: Key and envelope details with first and last 128 bytes //// TODO make it
    // aggressively as small as possible and only make larger if it passes until it is back to
    // failing
    FileRecordStore.logger.log(
        Level.FINEST,
        () ->
            String.format(
                "key and envelope details: position=%d, width=%d, fp=%d, writeLen=%d, crc32=%d, firstLast128Bytes=%s",
                index, len & 0xFFFF, fpk, entrySize, crc32, printFirstLast128(entryBytes)));

    fileOperations.seek(fpk);
    fileOperations.write(entryBytes, 0, entrySize);
  }

  /// Writes the ith record header to the index.
  /// This method needs to be refactored to receive the key parameter for proper state updates.
  /// For now, it uses the inefficient key lookup approach.
  private void writeRecordHeaderToIndex(RecordHeader header) throws IOException {
    logger.log(
        Level.FINEST,
        () -> String.format("writeRecordHeaderToIndex: ENTER input header=%s", header));
    fileOperations.seek(indexPositionToRecordHeaderFp(header.indexPosition()));
    final var writtenHeader = write(header, fileOperations);
    logger.log(
        Level.FINEST,
        () -> String.format("writeRecordHeaderToIndex: written header=%s", writtenHeader));
    // Update the headerState with the header that has the correct CRC
    // We need to find the key for this header to update properly
    // Since we don't have the key here, we'll update via the headerState path
    final var oldHeader = headerState.getByPointer(header.dataPointer());
    if (oldHeader != null) {
      // Find the key for this header - this is inefficient but necessary for now
      for (var entry : headerState.memIndex.entrySet()) {
        if (entry.getValue().equals(oldHeader)) {
          headerState.update(
              entry.getKey(),
              oldHeader,
              writtenHeader.dataPointer(),
              writtenHeader.dataLength(),
              writtenHeader.dataCapacity(),
              writtenHeader.indexPosition());
          break;
        }
      }
    }
  }

  /// Inserts a new record. It tries to insert into free space at the end of the index space, or
  /// free space between
  /// records, then finally extends the fileOperations. If the file has been set to a large initial
  /// file it will initially all
  /// be considered space at the end of the index space such that inserts will be prepended into the
  /// back of the
  /// fileOperations. When there is no more space in the index area the file will be expanded and
  /// record(s) will be into the new
  /// space to make space for headers.
  ///
  /// @param key the key for the new record
  /// @param value the data to store for the key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key already exists
  @Synchronized
  public void insertRecord(byte[] key, byte[] value) throws IOException {
    ensureOpen();
    try {
      ensureNotReadOnly();
      logger.log(
          Level.FINE,
          () -> String.format("insertRecord value.len:%d key:%s ", value.length, print(key)));
      if (recordExists(key)) {
        throw new IllegalArgumentException("Key exists: " + Arrays.toString(key));
      }
      ensureIndexSpace(getNumRecords() + 1);
      RecordHeader newRecord = allocateRecord(payloadLength(value.length));
      writeRecordData(newRecord, value);
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
      // For new inserts, we pass the original newRecord to addEntryToIndex
      // The state will be updated there after the index entry is written
      addEntryToIndex(keyWrapper, newRecord, getNumRecords());
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  private int payloadLength(int raw) {
    int len = raw + Integer.BYTES; // for length prefix
    if (!disableCrc32) {
      len += Long.BYTES; // for crc32 long
    }
    return len;
  }

  /// Updates an existing record with new data.
  ///
  /// @param key the key of the record to update
  /// @param value the new data to store for the key
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  @Synchronized
  public void updateRecord(byte[] key, byte[] value) throws IOException {
    logger.log(
        Level.FINE,
        () ->
            String.format("updateRecord: ENTER key=%s, value.length=%d", print(key), value.length));
    ensureOpen();
    try {
      ensureNotReadOnly();
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
      final var updateMeHeader = keyToRecordHeader(keyWrapper);
      final var capacity = updateMeHeader.dataCapacity();

      final var recordIsSameSize = value.length == capacity;
      final var recordIsSmaller = value.length < capacity;

      logger.log(
          Level.FINER,
          () ->
              String.format(
                  "updateRecord: capacity=%d, sameSize=%b, smaller=%b, allowInPlace=%b",
                  capacity, recordIsSameSize, recordIsSmaller, allowInPlaceUpdates));

      // can update in place if the record is same size no matter whether CRC32 is enabled.
      // for smaller records, allow in-place updates based on the allowInPlaceUpdates setting
      if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
        logger.log(Level.FINER, () -> "updateRecord: taking INPLACE path");
        updateRecordInternal(keyWrapper, updateMeHeader, value);
      } else {
        logger.log(Level.FINER, () -> "updateRecord: taking MOVE/ALLOCATE path");
        updateRecordInternal(keyWrapper, updateMeHeader, value);
      }
      logger.log(Level.FINE, () -> "updateRecord: EXIT completed");
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  private void writeRecordData(RecordHeader header, byte[] data) throws IOException {

    assert data.length <= header.dataCapacity() : "Record data does not fit";

    // FIXME this should delegate to the underlying which can do ByteBuffer stuff.
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bout);
    out.writeInt(data.length);
    out.write(data);
    long crc = -1;
    if (!disableCrc32) {
      int crcInt = (int) (RecordHeader.computeCrc32(data, data.length) & 0xFFFFFFFFL);
      out.writeInt(crcInt);
    }
    out.close();
    final var payload = bout.toByteArray();
    fileOperations.seek(header.dataPointer());
    fileOperations.write(payload, 0, payload.length); // drop
    byte[] lenBytes = Arrays.copyOfRange(payload, 0, Integer.BYTES);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">d fp:%d len:%d end:%d bytes:%s",
                header.dataPointer(),
                payload.length,
                header.dataPointer() + payload.length,
                print(lenBytes)));

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">d fp:%d len:%d end:%d crc:%d data:%s",
                header.dataPointer() + Integer.BYTES,
                payload.length,
                header.dataPointer() + payload.length,
                crc,
                print(data)));
  }

  /// Deletes the record with the specified key.
  ///
  /// @param key the key of the record to delete
  /// @throws IOException if an I/O error occurs
  /// @throws IllegalArgumentException if the key does not exist
  @Synchronized
  public void deleteRecord(byte[] key) throws IOException {
    ensureOpen();
    try {
      ensureNotReadOnly();
      logger.log(Level.FINE, () -> String.format("deleteRecord key:%s", print(key)));
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
      RecordHeader delRec = keyToRecordHeader(keyWrapper);
      int currentNumRecords = getNumRecords();
      deleteEntryFromIndex(delRec, currentNumRecords);
      headerState.remove(keyWrapper, delRec);
      freeMap.remove(delRec);

      if (getFileLength() == delRec.dataPointer() + delRec.dataCapacity()) {
        // shrink file since this is the last record in the file
        setFileLength(delRec.dataPointer());
      } else {
        final var previousOptional = getRecordAt(delRec.dataPointer() - 1);
        if (previousOptional.isPresent()) {
          // append space of deleted record onto previous record
          final var previous = previousOptional.get();
          previous.incrementDataCapacity(delRec.dataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          // make free space at the end of the index area
          writeDataStartPtrHeader(delRec.dataPointer() + delRec.dataCapacity());
        }
      }
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
  }

  /// Expands header space if needed during allocation.
  /// This is called when allocateRecord cannot find free space.
  private void expandHeaderIfNeeded(int requiredPayloadLength) throws IOException {
    if (!allowHeaderExpansion) {
      logger.log(
          Level.FINE, () -> "expandHeaderIfNeeded: Header expansion disabled, skipping expansion");
      return;
    }

    // Calculate current header usage
    int currentNumRecords = getNumRecords();
    int requiredNumRecords = currentNumRecords + 1; // Need at least one more record

    // Check if we need to expand
    long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
    long available = dataStartPtr - endIndexPtr - (2L * indexEntryLength);

    if (requiredPayloadLength <= available) {
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "expandHeaderIfNeeded: Sufficient space available (available=%d, required=%d), no expansion needed",
                  available, requiredPayloadLength));
      return;
    }

    logger.log(
        Level.FINE,
        () ->
            String.format(
                "expandHeaderIfNeeded: Insufficient space (available=%d, required=%d), triggering header expansion",
                available, requiredPayloadLength));
    ensureIndexSpace(requiredNumRecords);
  }

  /// Expands the file by the preferred expansion size to make room for new records.
  /// This is called when allocateRecord cannot find space in existing regions.
  /// Always expands by preferredExpansionSize (2 MiB default) for SSD optimization.
  private void expandFile(int requiredPayloadLength) throws IOException {
    long currentSize = fileOperations.length();
    long newSize = currentSize + preferredExpansionSize;

    logger.log(
        Level.FINE,
        () ->
            String.format(
                "expandFile: Expanding file from %d to %d bytes (expansion=%d, required=%d)",
                currentSize, newSize, preferredExpansionSize, requiredPayloadLength));

    fileOperations.setLength(newSize);

    // 🔧 Push the data-region boundary forward so the new tail area becomes usable
    dataStartPtr = newSize;
    writeDataStartPtrHeader(dataStartPtr);
  }

  /// Aligns a position to the nearest block boundary for SSD optimization.
  /// Ensures data pointers are aligned to preferredBlockSize boundaries.
  private long alignToBlockSize(long position) {
    if (preferredBlockSize <= 0) {
      return position; // No alignment needed
    }
    long remainder = position % preferredBlockSize;
    if (remainder == 0) {
      return position; // Already aligned
    }
    return position + (preferredBlockSize - remainder);
  }

  /// Checks to see if there is space for and additional index entry. If
  /// not, space is created by moving records to the end of the fileOperations.
  private void ensureIndexSpace(int requiredNumRecords) throws IOException {
    long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);

    // Check if header expansion is disabled
    if (!allowHeaderExpansion) {
      // Verify we have sufficient pre-allocated space
      if (endIndexPtr > dataStartPtr) {
        int currentRecords = getNumRecords();
        throw new IllegalStateException(
            String.format(
                "Header expansion disabled and insufficient pre-allocated space. "
                    + "Required: %d records, Current: %d records, Pre-allocated header space exhausted. "
                    + "Consider increasing preallocatedRecords or enabling header expansion.",
                requiredNumRecords, currentRecords));
      }
      // Sufficient space available, no expansion needed
      return;
    }

    if (isEmpty() && endIndexPtr > getFileLength()) {
      setFileLength(endIndexPtr);
      dataStartPtr = endIndexPtr;
      writeDataStartPtrHeader(dataStartPtr);
      return;
    }
    // move records to the back. if PAD_DATA_TO_KEY_LENGTH=true this should only move one record
    while (endIndexPtr > dataStartPtr) {
      // Find the first record whose dataPointer is >= dataStartPtr (boundary search)
      final var firstEntry = headerState.getCeilingEntry(dataStartPtr);
      final var firstOptional =
          (firstEntry != null) ? Optional.of(firstEntry.getValue()) : Optional.empty();
      // TODO figure out if there is some case where someone could trigger the following
      // IllegalStateException
      final var first =
          (RecordHeader)
              firstOptional.orElseThrow(
                  () ->
                      new IllegalStateException(
                          "no record at or after dataStartPtr " + dataStartPtr));
      // Need to find the key for this header to update properly
      KeyWrapper firstKey = null;
      for (var entry : headerState.keySet()) {
        if (headerState.getByKey(entry).equals(first)) {
          firstKey = entry;
          break;
        }
      }

      if (firstKey == null) {
        throw new IllegalStateException("Could not find key for header during index expansion");
      }

      freeMap.remove(first);
      byte[] data = readRecordData(first);
      long fileLen = getFileLength();

      // Use atomic update pattern with computeIfPresent and move
      // Ensure new position is aligned to preferredBlockSize for SSD optimization
      long alignedFileLen = alignToBlockSize(fileLen);
      final var updatedFirst = RecordHeader.move(first, alignedFileLen, payloadLength(data.length));

      setFileLength(alignedFileLen + payloadLength(data.length));
      writeRecordData(updatedFirst, data);
      writeRecordHeaderToIndex(updatedFirst);
      headerState.update(firstKey, first, updatedFirst);
      dataStartPtr = headerState.getCeilingEntry(dataStartPtr).getValue().dataPointer();
      writeDataStartPtrHeader(dataStartPtr);
    }

    // Keep dataStartPtr in sync after header expansion
    dataStartPtr = endIndexPtr;
    writeDataStartPtrHeader(dataStartPtr);
  }

  /// Logs detailed information about all records in the store.
  ///
  /// @param level the logging level to use
  /// @param disableCrc32 whether to disable CRC32 validation during logging
  @SneakyThrows
  @Synchronized
  @SuppressWarnings("unused")
  public void logAll(Level level, boolean disableCrc32) {
    final var oldDisableCdc32 = this.disableCrc32;
    try {
      this.disableCrc32 = disableCrc32;
      final var len = getFileLength();
      logger.log(
          level,
          () ->
              String.format(
                  "Records=%s, FileLength=%s, DataPointer=%s", getNumRecords(), len, dataStartPtr));
      for (int index = 0; index < getNumRecords(); index++) {
        final RecordHeader header = readRecordHeaderFromIndex(index);
        final var bk = readKeyFromIndex(index);
        final String k = java.util.Base64.getEncoder().encodeToString(bk.bytes());
        int finalIndex = index;
        logger.log(
            level,
            () ->
                String.format(
                    "%d header Key=%s, indexPosition=%s, dataCapacity()=%s, dataLength=%s, dataPointer=%s, crc32=%s",
                    finalIndex,
                    k,
                    header.indexPosition(),
                    header.dataCapacity(),
                    header.dataLength(),
                    header.dataPointer(),
                    header.crc32()));
        final byte[] data = readRecordData(bk.bytes());

        String d = java.util.Base64.getEncoder().encodeToString(data);
        int finalIndex1 = index;
        logger.log(
            level, () -> String.format("%d data  len=%d data=%s", finalIndex1, data.length, d));
      }
    } finally {
      this.disableCrc32 = oldDisableCdc32;
    }
  }

  /// Returns the number of records in the store.
  ///
  /// @return the number of records currently stored
  public int size() {
    return getNumRecords();
  }

  /// Throws an exception if this store is read-only.
  private void ensureNotReadOnly() {
    if (readOnly) {
      throw new UnsupportedOperationException("Cannot modify read-only store");
    }
  }

  /// Sets whether to allow in-place updates for smaller records regardless of CRC32 setting.
  /// When true (default), smaller records can be updated in-place even when CRC32 is disabled.
  /// When false, smaller records will use the dual-write pattern (old behavior).
  /// Same-size records can always be updated in-place regardless of this setting.
  ///
  /// This is a runtime operational mode toggle primarily intended for snapshotting scenarios.
  /// The feature enables append-only behavior during sequential scans to prevent record movement.
  ///
  /// @param allow true to allow in-place updates for smaller records, false to force dual-write
  /// @throws UnsupportedOperationException if the store is read-only
  /// @throws IllegalStateException if the store is not in OPEN state
  public void setAllowInPlaceUpdates(boolean allow) {
    ensureOpen();
    ensureNotReadOnly();
    this.allowInPlaceUpdates = allow;
  }

  /// Sets whether to allow header region expansion during operations.
  /// When true (default), the header region can expand to accommodate more records.
  /// When false, header expansion is disabled and operations will fail if pre-allocated
  /// space is exceeded. This is useful for snapshotting to maintain stable memory layout.
  ///
  /// This is a runtime operational mode toggle primarily intended for snapshotting scenarios.
  /// The feature prevents header/index expansion during sequential scans to maintain memory
  /// stability.
  ///
  /// @param allow true to allow header expansion, false to disable it
  /// @throws UnsupportedOperationException if the store is read-only
  /// @throws IllegalStateException if the store is not in OPEN state
  public void setAllowHeaderExpansion(boolean allow) {
    ensureOpen();
    ensureNotReadOnly();
    this.allowHeaderExpansion = allow;
  }

  /// Thread-safe state management for record headers.
  /// Encapsulates both memIndex and positionIndex maps with a single GuardedReentrantReadWriteLock
  /// to ensure atomic updates and prevent inconsistent map states.
  final class State {
    private final Map<KeyWrapper, RecordHeader> memIndex = new HashMap<>();
    private final NavigableMap<Long, RecordHeader> positionIndex = new TreeMap<>();
    private final GuardedReentrantReadWriteLock lock = new GuardedReentrantReadWriteLock();

    /// Returns the record header for the given key.
    RecordHeader getByKey(KeyWrapper key) {
      try (var ignored = lock.readLock()) {
        return memIndex.get(key);
      }
    }

    /// Returns the record header for the given data pointer.
    RecordHeader getByPointer(long ptr) {
      try (var ignored = lock.readLock()) {
        return positionIndex.get(ptr);
      }
    }

    /// Returns the floor entry for the given data pointer.
    Map.Entry<Long, RecordHeader> getFloorEntry(long ptr) {
      try (var ignored = lock.readLock()) {
        return positionIndex.floorEntry(ptr);
      }
    }

    /// Returns the ceiling entry for the given data pointer.
    Map.Entry<Long, RecordHeader> getCeilingEntry(long ptr) {
      try (var ignored = lock.readLock()) {
        return positionIndex.ceilingEntry(ptr);
      }
    }

    /// Atomically updates both maps with a new record header constructed from field changes.
    /// Only data pointer, data length, data capacity, and index position can be updated.
    /// Key and CRC remain immutable.
    void update(
        KeyWrapper key,
        RecordHeader oldHeader,
        long newDataPointer,
        int newDataLength,
        int newDataCapacity,
        int newIndexPosition) {
      Objects.requireNonNull(key, "key cannot be null");
      try (var ignored = lock.writeLock()) {
        RecordHeader updated;
        if (oldHeader == null) {
          // For new records, create a RecordHeader from scratch
          updated =
              new RecordHeader(newDataPointer, newDataLength, newDataCapacity, newIndexPosition);
        } else {
          // For existing records, update the old header
          updated =
              RecordHeader.withDataPointerAndCapacity(oldHeader, newDataPointer, newDataCapacity);
          // Update data length separately if it changed
          if (newDataLength != oldHeader.dataLength()) {
            updated = RecordHeader.withDataCount(updated, newDataLength);
          }
          // Update index position separately if it changed
          if (newIndexPosition != oldHeader.indexPosition()) {
            updated = RecordHeader.withIndexPosition(updated, newIndexPosition);
          }
          positionIndex.remove(oldHeader.dataPointer());
        }
        memIndex.put(key, updated);
        positionIndex.put(updated.dataPointer(), updated);

        // Consistency check - both maps should always have the same size
        if (memIndex.size() != positionIndex.size()) {
          String errorMsg =
              String.format(
                  "State consistency error: memIndex.size=%d != positionIndex.size=%d",
                  memIndex.size(), positionIndex.size());
          logger.severe(errorMsg);
          parentStore.state = StoreState.UNKNOWN;
          throw new IllegalStateException(errorMsg);
        }
      } catch (Exception e) {
        // Any exception during state update could leave maps in inconsistent state
        logger.severe(
            "State update failed, transitioning store to UNKNOWN state: " + e.getMessage());
        parentStore.state = StoreState.UNKNOWN;
        throw e;
      }
    }

    /// Convenience method for simple header replacement when only the header object changes.
    void update(KeyWrapper key, RecordHeader oldHeader, RecordHeader newHeader) {
      Objects.requireNonNull(key, "key cannot be null");
      Objects.requireNonNull(newHeader, "newHeader cannot be null");
      try (var ignored = lock.writeLock()) {
        if (oldHeader != null) {
          positionIndex.remove(oldHeader.dataPointer());
        }
        memIndex.put(key, newHeader);
        positionIndex.put(newHeader.dataPointer(), newHeader);

        // Consistency check - both maps should always have the same size
        if (memIndex.size() != positionIndex.size()) {
          String errorMsg =
              String.format(
                  "State consistency error: memIndex.size=%d != positionIndex.size=%d",
                  memIndex.size(), positionIndex.size());
          logger.severe(errorMsg);
          parentStore.state = StoreState.UNKNOWN;
          throw new IllegalStateException(errorMsg);
        }
      } catch (Exception e) {
        // Any exception during state update could leave maps in inconsistent state
        logger.severe(
            "State update failed, transitioning store to UNKNOWN state: " + e.getMessage());
        parentStore.state = StoreState.UNKNOWN;
        throw e;
      }
    }

    /// Atomically removes a record header from both maps.
    void remove(KeyWrapper key, RecordHeader header) {
      Objects.requireNonNull(key, "key cannot be null");
      Objects.requireNonNull(header, "header cannot be null");
      try (var ignored = lock.writeLock()) {
        memIndex.remove(key);
        positionIndex.remove(header.dataPointer());

        // Consistency check - both maps should always have the same size
        if (memIndex.size() != positionIndex.size()) {
          String errorMsg =
              String.format(
                  "State consistency error after remove: memIndex.size=%d != positionIndex.size=%d",
                  memIndex.size(), positionIndex.size());
          logger.severe(errorMsg);
          parentStore.state = StoreState.UNKNOWN;
          throw new IllegalStateException(errorMsg);
        }
      } catch (Exception e) {
        // Any exception during state update could leave maps in inconsistent state
        logger.severe(
            "State remove failed, transitioning store to UNKNOWN state: " + e.getMessage());
        parentStore.state = StoreState.UNKNOWN;
        throw e;
      }
    }

    /// Returns the number of records in the state.
    int size() {
      try (var ignored = lock.readLock()) {
        return memIndex.size();
      }
    }

    /// Returns whether the state is empty.
    boolean isEmpty() {
      try (var ignored = lock.readLock()) {
        return memIndex.isEmpty();
      }
    }

    /// Returns whether the state contains the given key.
    boolean containsKey(KeyWrapper key) {
      try (var ignored = lock.readLock()) {
        return memIndex.containsKey(key);
      }
    }

    /// Returns a defensive copy of all keys in the state.
    Set<KeyWrapper> keySet() {
      try (var ignored = lock.readLock()) {
        return new HashSet<>(memIndex.keySet());
      }
    }

    /// Clears both maps.
    void clear() {
      try (var ignored = lock.writeLock()) {
        memIndex.clear();
        positionIndex.clear();
      }
    }

    /// Returns a string representation of both maps for debugging.
    @Override
    public String toString() {
      try (var ignored = lock.readLock()) {
        return memIndex + " | " + positionIndex;
      }
    }
  }

  /// Builder for creating FileRecordStore instances with a fluent API inspired by H2 MVStore.
  /// Example usage:
  /// <pre>
  /// FileRecordStore store = new FileRecordStoreBuilder()
  ///     .path("/path/to/store.dat")
  ///     .maxKeyLength(128)
  ///     .useMemoryMapping(true)
  ///     .open();
  /// </pre>
  public static FileRecordStoreBuilder Builder() {
    return new FileRecordStoreBuilder();
  }
}
