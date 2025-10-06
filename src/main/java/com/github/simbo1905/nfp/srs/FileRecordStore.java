package com.github.simbo1905.nfp.srs;

import static com.github.simbo1905.nfp.srs.FileRecordStoreBuilder.*;
import static java.util.Optional.of;

import java.io.*; // FIXME remote any Input/Output streams push down to file ops which may use ByteBuffer not old IO
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Synchronized;

/// A persistent record store that maps keys to values with crash-safe guarantees.
/// Provides ACID properties with durable writes and supports both direct I/O and memory-mapped
/// access modes.
public class FileRecordStore implements AutoCloseable {

  private static final Logger logger = Logger.getLogger(FileRecordStore.class.getName());

  // this is an unsigned 32 int
  static final int CRC32_LENGTH = Integer.BYTES;
  
  /// The fixed size of the envelope (metadata) that follows each key in the index.
  /// Contains: 8 bytes (dataPointer) + 4 bytes (dataCapacity) + 4 bytes (dataCount) + 4 bytes (crc32) = 20 bytes total
  static final int ENVELOPE_SIZE = 20;

  /// The maximum key length this store was configured with. Immutable after creation.
  public final int maxKeyLength;

  /// The total length of one index entry - the key length plus the envelope
  /// size (20 bytes) and the CRC of the key which is an unsigned 32 bits.
  private final int indexEntryLength;

  @Getter private final Path filePath;

  /// Flag indicating if this store was opened as read-only
  private final boolean readOnly;

  private final Comparator<RecordHeader> compareRecordHeaderByFreeSpace =
      Comparator.comparingInt(o -> o.getFreeSpace(true));

  ///  FileOperations abstracts over memory mapped files and traditional IO ranom access file.
  /*default*/ FileOperations fileOperations;

  /// In-memory index mapping keys to record headers. Uses KeyWrapper for efficient hash code caching and optional defensive copying. Supports both byte array and UUID keys.
  private Map<KeyWrapper, RecordHeader> memIndex;

  /// Key type for optimized handling - enables JIT branch elimination since this is final after construction
  private final KeyType keyType;

  /// Whether to use defensive copying for byte array keys for example is not needed when the actual keys ware UUID or Strings
  private final boolean defensiveCopy;

  /// Returns whether in-place updates are allowed for smaller records. We will toggle this to do online backups.
  @Getter private volatile boolean allowInPlaceUpdates = true;

  /// Returns whether header region expansion is allowed. We will toggle this to do online backups.
  ///
  /// Whether to allow header region expansion during operations
  @Getter private volatile boolean allowHeaderExpansion = true;

  /// Expansion size in bytes for extending the file. These days on cloud hosts with SSDs 2 MiB is sensible default.
  final int preferredExpansionSize;

  /// Block size in bytes for data alignment (must be power of 2). Typical with SSD a 4 KiB, 8 KiB or even higher may be sensible.
  final int preferredBlockSize;

  /// Initial header region size in bytes. If you know you have small keys and will only have a hundred thousand we can just preallocate the space to avoid moving records to ever expand the header region.
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
  /// TreeMap of headers by file index.
  private TreeMap<Long, RecordHeader> positionIndex;
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
        this.indexEntryLength = Short.BYTES + maxKeyLength + CRC32_LENGTH + ENVELOPE_SIZE;
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

        // Initialize data structures before any file operations that might fail
        int numRecords = readNumRecordsHeader();

        memIndex =
            new HashMap<>(wasEmpty ? preallocatedRecords : Math.max((int) (numRecords * 1.2), 16));
        positionIndex = new TreeMap<>();

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

      memIndex.put(key, header);
      positionIndex.put(header.dataPointer(), header);

      updateFreeSpaceIndex(header);
    }
  }

  /// Reads the ith key from the index.
  private KeyWrapper readKeyFromIndex(int position) throws IOException {
    final var fp = indexPositionToKeyFp(position);
    fileOperations.seek(fp);

    final var keyLengthShort = fileOperations.readShort();
    final var len = keyLengthShort & 0xFFFF; // interpret as unsigned short
    final var writeLen = Short.BYTES + len + CRC32_LENGTH;

    // FINER logging: Key and envelope details coming from disk //// TODO remove this after two zeros gon
    FileRecordStore.logger.log(Level.FINER,
        "key and envelope details: position=" + position + ", width=" + len + ", fp=" + fp + ", writeLen=" + writeLen);

    assert len <= maxKeyLength : String.format("%d > %d", len, maxKeyLength);

    byte[] key = new byte[len];
    fileOperations.read(key);

    byte[] crcBytes = new byte[CRC32_LENGTH];
    fileOperations.readFully(crcBytes);
    ByteBuffer buffer = ByteBuffer.allocate(CRC32_LENGTH);
    buffer.put(crcBytes);
    buffer.flip();
    long crc32expected =
        buffer.getInt() & 0xffffffffL; // https://stackoverflow.com/a/22938125/329496

    // Compute CRC over key length (short) + key bytes to match write logic
    CRC32 crc = new CRC32();
    ByteBuffer keyDataBuffer = ByteBuffer.allocate(Short.BYTES + len);
    keyDataBuffer.putShort((short) len);
    keyDataBuffer.put(key);
    crc.update(keyDataBuffer.array());
    final var crc32actual = crc.getValue();

    // FINEST logging: Key and envelope details with first 256 bytes //// TODO remove this after two zeros gon
    byte[] first256Bytes = Arrays.copyOf(key, Math.min(256, key.length));
    FileRecordStore.logger.log(Level.FINEST,
        "key and envelope details: position=" + position + ", width=" + len + ", fp=" + fp + ", writeLen=" + writeLen + ", expectedCrc32=" + crc32expected + ", actualCrc32=" + crc32actual + ", first256Bytes=" + print(first256Bytes));

    if (crc32actual != crc32expected) {
      throw new IllegalStateException(
          String.format(
              "key and envelope details: position=%d, width=%d, fp=%d, expectedCrc32=%d, actualCrc32=%d, first256Bytes=%s",
              position,
              len,
              fp,
              crc32expected,
              crc32actual,
              print(Arrays.copyOf(key, Math.min(256, key.length)))));
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

  private static String print(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (byte b : bytes) {
      sb.append(String.format("0x%02X ", b));
    }
    sb.append("]");
    return sb.toString();
  }

  /// Returns a file pointer in the index pointing to the first byte in the
  /// record pointer located at the given index position.
  private long indexPositionToRecordHeaderFp(int pos) {
    return indexPositionToKeyFp(pos) + maxKeyLength;
  }

  private static RecordHeader read(int index, FileOperations in) throws IOException {
    byte[] header = new byte[ENVELOPE_SIZE];
    final var fp = in.getFilePointer();
    in.readFully(header);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "<h fp:%d idx:%d len:%d bytes:%s", fp, index, header.length, print(header)));

    ByteBuffer buffer = ByteBuffer.allocate(ENVELOPE_SIZE);
    buffer.put(header);
    buffer.flip();

    long dataPointer = buffer.getLong();
    int dataCapacity = buffer.getInt();
    int dataCount = buffer.getInt();
    long crc32 = buffer.getInt() & 0xFFFFFFFFL;

    RecordHeader rh = new RecordHeader(dataPointer, dataCount, dataCapacity, index, crc32);

    final var array = buffer.array();
    CRC32 crc = new CRC32();
    crc.update(array, 0, Long.BYTES + Integer.BYTES + Integer.BYTES);
    long crc32expected = crc.getValue();
    if (rh.crc32() != crc32expected) {
      throw new IllegalStateException(
          String.format("invalid header CRC32 expected %d for %s", crc32expected, rh));
    }

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "%d header Key=%s, indexPosition=%s, dataCapacity=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                fp,
                "key",
                rh.indexPosition(),
                rh.dataCapacity(),
                rh.dataCount(),
                rh.dataPointer(),
                rh.crc32()));

    return rh;
  }

  static int getMaxKeyLengthOrDefault() {
    final String key =
        String.format("%s.%s", FileRecordStore.class.getName(), "MAX_KEY_LENGTH");
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
    if (args.length < 1) {
      System.err.println("no file passed");
      System.exit(1);
    }
    if (args.length < 2) {
      System.err.println("no command passed");
      System.exit(2);
    }
    final String filename = args[0];
    logger.info("Reading from " + filename);

    boolean disableCrc32 = false;
    dumpFile(Level.INFO, filename, disableCrc32);
  }

  static void dumpFile(
      @SuppressWarnings("SameParameterValue") Level level, String filename, boolean disableCrc)
      throws IOException {
    try (FileRecordStore recordFile =
        FileRecordStore.Builder()
            .path(filename)
            .accessMode(AccessMode.READ_ONLY)
            .disablePayloadCrc32(disableCrc)
            .open()) {
      final var len = recordFile.getFileLength();
      logger.log(
          level,
          () ->
              String.format(
                  "Records=%s, FileLength=%s, DataPointer=%s",
                  recordFile.getNumRecords(), len, recordFile.dataStartPtr));
      for (int index = 0; index < recordFile.getNumRecords(); index++) {
        final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
        final var bk = recordFile.readKeyFromIndex(index);
        final String k = java.util.Base64.getEncoder().encodeToString(bk.bytes());
        logger.log(
            level,
            String.format(
                "%d header Key=%s, indexPosition=%s, dataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                index,
                k,
                header.indexPosition(),
                header.dataCapacity(),
                header.dataCount(),
                header.dataPointer(),
                header.crc32()));
        final byte[] data = recordFile.readRecordData(bk.bytes());

        String d = java.util.Base64.getEncoder().encodeToString(data);
        int finalIndex = index;
        logger.log(
            level, () -> String.format("%d data  len=%d data=%s", finalIndex, data.length, d));
      }
    }
  }

  long getFileLength() throws IOException {
    return fileOperations.length();
  }

  /// Returns the current number of records in the database.
  @Synchronized
  int getNumRecords() {
    return memIndex.size();
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
    RecordHeader h = memIndex.get(key);
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
            (header.dataCount() + len), getFileLength());

    // read the body
    byte[] buf = new byte[len];
    fileOperations.readFully(buf);

    if (!disableCrc32) {
      byte[] crcBytes = new byte[CRC32_LENGTH];
      fileOperations.readFully(crcBytes);
      final var expectedCrc =
          (new DataInputStream(new ByteArrayInputStream(crcBytes))).readInt() & 0xffffffffL;
      CRC32 crc32 = new CRC32();
      crc32.update(buf, 0, buf.length);

      long actualCrc = crc32.getValue();

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
      // Get the updated header from positionIndex since writeRecordData updated it
      final var updatedRecord = positionIndex.get(newRecord.dataPointer());
      addEntryToIndex(keyWrapper, updatedRecord, getNumRecords());
    } catch (Exception e) {
      state = StoreState.UNKNOWN;
      throw e;
    }
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
        // write the header with the main CRC
        writeRecordHeaderToIndex(updatedHeader);
        // Update memIndex with the final header that has correct CRC
        memIndex.put(keyWrapper, updatedHeader);
      } else { // Handle cases where in-place update is not possible
        final var endOfRecord = updateMeHeader.dataPointer() + updateMeHeader.dataCapacity();
        final var fileLength =
            getFileLength(); // perform a move. insert data to the end of the file then overwrite
        // header.
        if (endOfRecord == fileLength) {
          long length = fileLength + (value.length - updateMeHeader.dataCapacity());
          final var updatedHeader =
              RecordHeader.move(
                  updateMeHeader,
                  length,
                  value.length);
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
          memIndex.put(keyWrapper, updatedNewRecord);
          positionIndex.remove(updateMeHeader.dataPointer());
          positionIndex.put(updatedNewRecord.dataPointer(), updatedNewRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

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
          memIndex.put(keyWrapper, updatedNewRecord);
          positionIndex.remove(updateMeHeader.dataPointer());
          positionIndex.put(updatedNewRecord.dataPointer(), updatedNewRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

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
      final var memDeleted = memIndex.remove(keyWrapper);
      assert delRec.equals(memDeleted)
          : "memIndex header mismatch: expected " + delRec + " but got " + memDeleted;
      final var posDeleted = positionIndex.remove(delRec.dataPointer());
      assert delRec.equals(posDeleted)
          : "positionIndex header mismatch: expected " + delRec + " but got " + posDeleted;
      assert memIndex.size() == positionIndex.size()
          : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
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

  private int getDataLengthPadded(int dataLength) {
    return Math.max(indexEntryLength, dataLength);
  }

  /// Generates a defensive copy of all the keys in a thread safe manner.
  /// Returns byte arrays for BYTE_ARRAY mode, or UUIDs converted from 16-byte arrays for UUID mode.
  ///
  /// @return an iterable collection of all keys in the store
  public Iterable<byte[]> keysBytes() {
    ensureOpen();
    final var snapshot = snapshotKeys();
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
    final var snapshot = snapshotKeys();
    return snapshot.stream().map(KeyWrapper::toUUID).collect(Collectors.toSet());
  }

  @Synchronized
  private Set<KeyWrapper> snapshotKeys() {
    return new HashSet<>(memIndex.keySet());
  }

  /// Checks if the store contains no records.
  ///
  /// @return true if the store is empty, false otherwise
  @Synchronized
  public boolean isEmpty() {
    ensureOpen();
    return memIndex.isEmpty();
  }

  /// Checks if there is a record belonging to the given key.
  ///
  /// @param key the key to check
  /// @return true if a record exists for the key, false otherwise
  @Synchronized
  public boolean recordExists(byte[] key) {
    ensureOpen();
    final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
    return memIndex.containsKey(keyWrapper);
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
    return memIndex.containsKey(keyWrapper);
  }

  /// This method searches the free map for free space and then returns a
  /// RecordHeader which uses the space.
  private RecordHeader allocateRecord(int dataLength) throws IOException {

    // we needs space for the length int and the optional long crc32
    int payloadLength = payloadLength(dataLength);

    // we pad the record to be at least the size of a header to avoid moving many values to expand
    // the index
    int dataLengthPadded = getDataLengthPadded(payloadLength);

    // FIFO deletes cause free space after the index.
    long dataStart = readDataStartHeader();
    long endIndexPtr = indexPositionToKeyFp(getNumRecords());
    // we prefer speed overs space so we leave space for the header for this insert plus one for
    // future use
    long available = dataStart - endIndexPtr - (2L * indexEntryLength);

    RecordHeader newRecord = null;

    if (dataLengthPadded <= available) {
      newRecord = new RecordHeader(dataStart - dataLengthPadded, dataLengthPadded);
      dataStartPtr = dataStart - dataLengthPadded;
      writeDataStartPtrHeader(dataStartPtr);
      return newRecord;
    }

    // search for empty space
    for (RecordHeader next : this.freeMap.keySet()) {
      int free = next.getFreeSpace(disableCrc32);
      if (dataLengthPadded <= free) {
        newRecord = next.split(disableCrc32, payloadLength(0));
        updateFreeSpaceIndex(next);
        writeRecordHeaderToIndex(next);
        break;
      }
    }

    if (newRecord == null) {
      // append record to end of file - grows file to allocate space
      long fp = getFileLength();
      setFileLength(fp + dataLengthPadded);
      newRecord = new RecordHeader(fp, dataLengthPadded);
    }
    return newRecord;
  }

  /// Returns the record to which the target file pointer belongs - meaning the
  /// specified location in the file is part of the record data of the
  /// RecordHeader which is returned.
  private Optional<RecordHeader> getRecordAt(long targetFp) {
    final var floor = positionIndex.floorEntry(targetFp);
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
      if (memIndex != null) memIndex.clear();
      memIndex = null;
      if (positionIndex != null) positionIndex.clear();
      positionIndex = null;
      if (freeMap != null) freeMap.clear();
      freeMap = null;
      // Always transition to CLOSED after cleanup, regardless of previous state
      // This ensures consistent state even if exception occurred during close
      state = StoreState.CLOSED;
    }
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

    final var updatedNewRecord = RecordHeader.withIndexPosition(newRecord, currentNumRecords);
    fileOperations.seek(indexPositionToRecordHeaderFp(currentNumRecords));
    final var writtenHeader = write(updatedNewRecord, fileOperations);
    writeNumRecordsHeader(currentNumRecords + 1);

    logger.log(
        Level.FINEST,
        () -> String.format("before maps: %s | %s", memIndex.toString(), positionIndex.toString()));

    final var duplicate = memIndex.put(key, writtenHeader);
    if (duplicate != null) positionIndex.remove(duplicate.dataPointer());
    positionIndex.put(writtenHeader.dataPointer(), writtenHeader);

    logger.log(
        Level.FINEST,
        () -> String.format("after maps: %s | %s", memIndex.toString(), positionIndex.toString()));

    assert memIndex.size() == positionIndex.size()
        : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
  }

  private RecordHeader write(RecordHeader rh, FileOperations out) throws IOException {
    if (rh.dataCount() < 0) {
      throw new IllegalStateException("dataCount has not been initialized " + this);
    }
    final var fp = out.getFilePointer();

    // Create updated RecordHeader using constructor that computes CRC internally
    RecordHeader updatedRh =
        new RecordHeader(rh.dataPointer(), rh.dataCount(), rh.dataCapacity(), rh.indexPosition());

    ByteBuffer buffer = ByteBuffer.allocate(ENVELOPE_SIZE);
    buffer.putLong(updatedRh.dataPointer());
    buffer.putInt(updatedRh.dataCapacity());
    buffer.putInt(updatedRh.dataCount());
    buffer.putInt((int) (updatedRh.crc32() & 0xFFFFFFFFL));
    final var array = buffer.array();

    out.write(buffer.array(), 0, ENVELOPE_SIZE);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">h fp:%d idx:%d len:%d end:%d bytes:%s",
                fp, updatedRh.indexPosition(), array.length, fp + array.length, print(array)));

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

      // Update the maps with the written header (which has correct CRC)
      memIndex.put(lastKey, writtenHeader);
      positionIndex.put(writtenHeader.dataPointer(), writtenHeader);
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
    final var len = (short) key.length();
    // Write only the actual data length - no padding or uninitialized memory
    final var writeLen = Short.BYTES + len + CRC32_LENGTH;

    // FINER logging: Key and envelope details going to disk //// TODO remove this after two zeros gon
    final var fpk = indexPositionToKeyFp(index);
    FileRecordStore.logger.log(Level.FINER, 
        "key and envelope details: position=" + index + ", width=" + len + ", fp=" + fpk + ", writeLen=" + writeLen);

    ByteBuffer buffer = ByteBuffer.allocate(writeLen);
    buffer.putShort(len);
    buffer.put(key.bytes(), 0, key.length());  // Only actual key bytes

    // compute crc from exactly what we wrote (no padding or uninitialized memory)
    final var array = buffer.array();
    CRC32 crc = new CRC32();
    crc.update(array, 0, array.length);  // CRC covers length + key data only
    int crc32 = (int) (crc.getValue() & 0xFFFFFFFFL);

    // add the crc which will write through to the backing array
    buffer.putInt(crc32);

    // FINEST logging: Key and envelope details with first 256 bytes //// TODO remove this after two zeros gon
    byte[] first256Bytes = Arrays.copyOf(array, Math.min(256, array.length));
    FileRecordStore.logger.log(Level.FINEST,
        "key and envelope details: position=" + index + ", width=" + len + ", fp=" + fpk + ", writeLen=" + writeLen + ", crc32=" + crc32 + ", first256Bytes=" + print(first256Bytes));

    fileOperations.seek(fpk);
    fileOperations.write(array, 0, writeLen);  // Write only actual data
  }

  /// Writes the ith record header to the index.
  private void writeRecordHeaderToIndex(RecordHeader header) throws IOException {
    fileOperations.seek(indexPositionToRecordHeaderFp(header.indexPosition()));
    final var writtenHeader = write(header, fileOperations);
    // Update the positionIndex with the header that has the correct CRC
    positionIndex.put(writtenHeader.dataPointer(), writtenHeader);
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
      // Get the updated header from positionIndex since writeRecordData updated it
      final var updatedRecord = positionIndex.get(newRecord.dataPointer());
      addEntryToIndex(keyWrapper, updatedRecord, getNumRecords());
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
    ensureOpen();
    try {
      ensureNotReadOnly();
      logger.log(
          Level.FINE,
          () -> String.format("updateRecord value.len:%d key:%s", value.length, print(key)));
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
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
        // write the header with the main CRC
        writeRecordHeaderToIndex(updatedHeader);
        // Update memIndex with the final header that has correct CRC
        memIndex.put(keyWrapper, updatedHeader);
      } else { // Handle cases where in-place update is not possible
        final var endOfRecord = updateMeHeader.dataPointer() + updateMeHeader.dataCapacity();
        final var fileLength =
            getFileLength(); // perform a move. insert data to the end of the file then overwrite
        // header.
        if (endOfRecord == fileLength) {
          long length = fileLength + (value.length - updateMeHeader.dataCapacity());
          final var updatedHeader =
              RecordHeader.move(
                  updateMeHeader,
                  length,
                  value.length);
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
          memIndex.put(keyWrapper, updatedNewRecord);
          positionIndex.remove(updateMeHeader.dataPointer());
          positionIndex.put(updatedNewRecord.dataPointer(), updatedNewRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

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
          memIndex.put(keyWrapper, updatedNewRecord);
          positionIndex.remove(updateMeHeader.dataPointer());
          positionIndex.put(updatedNewRecord.dataPointer(), updatedNewRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

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

  /// Updates the contents of the given record. A RecordsFileException is
  /// thrown if the new data does not fit in the space allocated to the record.
  /// The header's data count is updated, but not written to the fileOperations.
  private void writeRecordData(RecordHeader header, byte[] data) throws IOException {

    assert data.length <= header.dataCapacity() : "Record data does not fit";
    // Use atomic update pattern to update the header in the positionIndex map
    final var updatedHeader = RecordHeader.withDataCount(header, data.length);
    positionIndex.put(header.dataPointer(), updatedHeader);

    // FIXME this should delegate to the underlying which can do ByteBuffer stuff.
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bout);
    out.writeInt(data.length);
    out.write(data);
    long crc = -1;
    if (!disableCrc32) {
      CRC32 crc32 = new CRC32();
      crc32.update(data, 0, data.length);
      int crcInt = (int) (crc32.getValue() & 0xFFFFFFFFL);
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
      final var memDeleted = memIndex.remove(keyWrapper);
      assert delRec.equals(memDeleted)
          : "memIndex header mismatch: expected " + delRec + " but got " + memDeleted;
      final var posDeleted = positionIndex.remove(delRec.dataPointer());
      assert delRec.equals(posDeleted)
          : "positionIndex header mismatch: expected " + delRec + " but got " + posDeleted;
      assert memIndex.size() == positionIndex.size()
          : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
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
      final var firstOptional = getRecordAt(dataStartPtr);
      if( firstOptional.isEmpty()){ // TODO this is slow so remove
        for (RecordHeader h : this.memIndex.values()) {
          RecordHeader other = this.positionIndex.get(h.dataPointer());
          if (other == null || other != h) {
            logger.severe(()->"MISMATCH: key=" + h
               + ", memIndex=" + h
                + ", positionIndex=" + other);
          }
        }
      }
      // TODO figure out if there is some case where someone could trigger the following
      // IllegalStateException
      final var first =
          firstOptional.orElseThrow(
              () -> new IllegalStateException("no record at dataStartPtr " + dataStartPtr));
      positionIndex.remove(first.dataPointer());
      freeMap.remove(first);
      byte[] data = readRecordData(first);
      long fileLen = getFileLength();

      // Use atomic update pattern with computeIfPresent and move
      final var updatedFirst =
          RecordHeader.move(first, fileLen, getDataLengthPadded(payloadLength(data.length)));

      setFileLength(fileLen + getDataLengthPadded(payloadLength(data.length)));
      writeRecordData(updatedFirst, data);
      writeRecordHeaderToIndex(updatedFirst);
      positionIndex.put(updatedFirst.dataPointer(), updatedFirst);
      dataStartPtr = positionIndex.ceilingEntry(dataStartPtr).getValue().dataPointer();
      writeDataStartPtrHeader(dataStartPtr);
    }
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
                    "%d header Key=%s, indexPosition=%s, dataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                    finalIndex,
                    k,
                    header.indexPosition(),
                    header.dataCapacity(),
                    header.dataCount(),
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
