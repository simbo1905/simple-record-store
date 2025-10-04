package com.github.simbo1905.nfp.srs;

import static java.util.Optional.of;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
// access modes.
public class FileRecordStore implements AutoCloseable {

  /// Magic number identifying valid FileRecordStore files (0xBEEBBEEB).
  /// Placed at the start of every file to detect corruption and incompatible formats.
  private static final int MAGIC_NUMBER = 0xBEEBBEEB;
  /// Default maximum key length in bytes. Can be overridden up to 2^8 - 4.
  public static final int DEFAULT_MAX_KEY_LENGTH = 64;
  /// Theoretical maximum key length based on file format constraints (2^8 - 4).
  public static final int MAX_KEY_LENGTH_THEORETICAL =
      Double.valueOf(Math.pow(2, 8)).intValue() - Integer.BYTES;
  private static final Logger logger = Logger.getLogger(FileRecordStore.class.getName());
  // Number of bytes in the record header.
  private static final int RECORD_HEADER_LENGTH = 20;
  // File index to the magic number header.
  private static final long MAGIC_NUMBER_HEADER_LOCATION = 0;
  // File index to the key length header (after magic number).
  private static final long KEY_LENGTH_HEADER_LOCATION = 4;
  // File index to the num records header.
  private static final long NUM_RECORDS_HEADER_LOCATION = 5;
  // File index to the start of the data region beyond the index region
  private static final long DATA_START_HEADER_LOCATION = 9;
  /// Total length in bytes of the global database headers:
  /// 1. 4-byte magic number (0xBEEBBEEB) for file format validation
  /// 2. 1 byte stores the key length the file was created with. (This cannot
  ///    be changed; copy into a new store to adjust the limit.)
  /// 3. 4-byte int tracking the number of records.
  /// 4. 8-byte long pointing to the start of the data region.
  private static final int FILE_HEADERS_REGION_LENGTH = 17;
  // this is an unsigned 32 int
  private static final int CRC32_LENGTH = 4;
  /// System property name for configuring the maximum key length.
  public static String MAX_KEY_LENGTH_PROPERTY = "MAX_KEY_LENGTH";
  private static final boolean PAD_DATA_TO_KEY_LENGTH = getPadDataToKeyLengthOrDefaultTrue();
  // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
  // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surrogate key out of
  // anything
  // unique about your data. You can also set it to be a max of 248 bytes. Note we store binary keys
  // with a header byte
  // and a CRC32 which is an unsigned 32 stored as a long.
  /// The maximum key length this store was configured with. Immutable after creation.
  public final int maxKeyLength;
  // The total length of one index entry - the key length plus the record
  // header length and the CRC of the key which is an unsigned 32 bits.
  private final int indexEntryLength;

  @Getter private final Path filePath;
  /// Flag indicating if this store is read-only
  private final boolean readOnly;
  private final Comparator<RecordHeader> compareRecordHeaderByFreeSpace =
      Comparator.comparingInt(o -> o.getFreeSpace(true));

  /*default*/ FileOperations fileOperations;
  /// In-memory index mapping keys to record headers. Uses KeyWrapper for efficient
  /// hash code caching and optional defensive copying. Supports both byte array and UUID keys.
  private Map<KeyWrapper, RecordHeader> memIndex;

  /// Key type for optimized handling - enables JIT branch elimination since this is final after
  // construction
  private final KeyType keyType;

  /// Whether to use defensive copying for byte array keys
  private final boolean defensiveCopy;

  /// Whether to allow in-place updates for smaller records regardless of CRC32 setting
  private volatile boolean allowInPlaceUpdates = true;

  /// Whether to allow header region expansion during operations
  private volatile boolean allowHeaderExpansion = true;

  /// Store state tracking for proper lifecycle management
  enum StoreState {
    NEW, // Initial state - store created but not yet validated/opened
    OPEN, // Store successfully opened and operational
    CLOSED, // Store cleanly closed via close() method
    UNKNOWN // Store encountered an exception, state is uncertain
  }

  /// Current state of the store
  private volatile StoreState state = StoreState.NEW;
  /// TreeMap of headers by file index.
  private TreeMap<Long, RecordHeader> positionIndex;
  /// ConcurrentSkipListMap makes scanning by ascending values fast and is sorted by smallest free
  // space first
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
  // record movement to expand the index region.
  ///                            Set to 0 for testing to force movement on every insert.
  /// @param maxKeyLength        maximum key length in bytes
  /// @param disablePayloadCrc32 if true, skips CRC32 on record values (keys and headers always
  // protected)
  /// @param useMemoryMapping    if true, use memory-mapped file access; does not affect write
  // amplification
  /// @throws IOException if file cannot be created or pre-allocation fails
  /// Constructor for backward compatibility - defaults to byte array keys with defensive copying
  @Deprecated
  FileRecordStore(
      File file,
      int preallocatedRecords,
      int maxKeyLength,
      boolean disablePayloadCrc32,
      boolean useMemoryMapping,
      String accessMode)
      throws IOException {
    this(
        file,
        preallocatedRecords,
        maxKeyLength,
        disablePayloadCrc32,
        useMemoryMapping,
        accessMode,
        KeyType.BYTE_ARRAY,
        true);
  }

  FileRecordStore(
      File file,
      int preallocatedRecords,
      int maxKeyLength,
      boolean disablePayloadCrc32,
      boolean useMemoryMapping,
      String accessMode,
      KeyType keyType,
      boolean defensiveCopy)
      throws IOException {
    try {
      // Validate maxKeyLength early
      if (maxKeyLength < 1 || maxKeyLength > MAX_KEY_LENGTH_THEORETICAL) {
        throw new IllegalArgumentException(
            String.format(
                "maxKeyLength must be between 1 and %d, got %d",
                MAX_KEY_LENGTH_THEORETICAL, maxKeyLength));
      }

      java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, accessMode);

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
      this.indexEntryLength = maxKeyLength + 1 + CRC32_LENGTH + RECORD_HEADER_LENGTH;
      this.readOnly = !"rw".equals(accessMode);
      this.keyType = keyType;
      this.defensiveCopy = defensiveCopy;

      // Validate UUID mode constraints
      if (keyType == KeyType.UUID && maxKeyLength != 16) {
        throw new IllegalArgumentException(
            "UUID key type requires maxKeyLength=16, got " + maxKeyLength);
      }

      // Check if file was empty when we opened it (before we modify it)
      boolean wasEmpty = raf.length() == 0;

      // Only set length for new files - don't overwrite existing data
      if (wasEmpty) {
        raf.setLength(FILE_HEADERS_REGION_LENGTH + (preallocatedRecords * indexEntryLength * 2L));
      }
      this.fileOperations =
          useMemoryMapping ? new MemoryMappedFile(raf) : new RandomAccessFile(raf);
      this.filePath = file.toPath();

      dataStartPtr = FILE_HEADERS_REGION_LENGTH + ((long) preallocatedRecords * indexEntryLength);

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

        boolean isOldFormat = false;
        int existingKeyLength;
        int existingRecords;

        if (firstFourBytes == MAGIC_NUMBER) {
          // New format with magic number
          existingKeyLength = readKeyLengthHeader();
          existingRecords = readNumRecordsHeader();
        } else {
          // Old format - reject with exception instead of warning
          throw new IllegalStateException(
              "Invalid file format: File does not contain required magic number 0xBEEBBEEB. " +
              "This appears to be an old format file or corrupted data. " +
              "Only files created with FileRecordStore.Builder are supported.");
        }

        // Validate key length matches before proceeding
        if (existingKeyLength != maxKeyLength) {
          throw new IllegalArgumentException(
              String.format(
                  "File has key length %d but builder specified %d",
                  existingKeyLength, maxKeyLength));
        }

        // Read dataStartPtr based on format
        if (isOldFormat) {
          fileOperations.seek(5); // Old DATA_START_HEADER_LOCATION
          dataStartPtr = fileOperations.readLong();
        } else {
          dataStartPtr = readDataStartHeader();
        }

        // Validate file has minimum required size for existing records
        // For old format, use old header length (13), for new format use new header length (17)
        long headerLength = isOldFormat ? 13 : FILE_HEADERS_REGION_LENGTH;
        long requiredFileSize = headerLength + ((long) existingRecords * indexEntryLength);
        if (fileOperations.length() < requiredFileSize) {
          throw new IOException(
              String.format(
                  "File too small for %d records. Required: %d bytes, Actual: %d bytes",
                  existingRecords, requiredFileSize, fileOperations.length()));
        }

        // Load existing index into memory - this may throw if data is corrupted
        loadExistingIndex(existingRecords);
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
    fileOperations.seek(MAGIC_NUMBER_HEADER_LOCATION);
    fileOperations.writeInt(MAGIC_NUMBER);
    logger.log(
        Level.FINEST, () -> String.format("Writing magic number header: 0x%08X", MAGIC_NUMBER));
  }

  /// Writes the max key length to the fileOperations (after magic number).
  private void writeKeyLengthHeader() throws IOException {
    fileOperations.seek(KEY_LENGTH_HEADER_LOCATION);
    final var keyLength = (byte) maxKeyLength;
    logger.log(
        Level.FINEST,
        "Writing key length header: " + keyLength + " (from maxKeyLength=" + maxKeyLength + ")");
    fileOperations.write(keyLength);
  }

  /// Reads the max key length from the fileOperations (after magic number).
  private int readKeyLengthHeader() throws IOException {
    fileOperations.seek(KEY_LENGTH_HEADER_LOCATION);
    int keyLength = fileOperations.readByte() & 0xFF;
    logger.log(Level.FINEST, "Reading key length header: " + keyLength);
    return keyLength;
  }

  /// Writes the number of records header to the fileOperations.
  private void writeNumRecordsHeader(int numRecords) throws IOException {
    fileOperations.seek(NUM_RECORDS_HEADER_LOCATION);
    fileOperations.writeInt(numRecords);
  }

  /// Reads the number of records header from the fileOperations.
  private int readNumRecordsHeader() throws IOException {
    fileOperations.seek(NUM_RECORDS_HEADER_LOCATION);
    return fileOperations.readInt();
  }

  /// Writes the data start pointer header to the fileOperations.
  private void writeDataStartPtrHeader(long dataStartPtr) throws IOException {
    fileOperations.seek(DATA_START_HEADER_LOCATION);
    fileOperations.writeLong(dataStartPtr);
  }

  /// Reads the data start pointer header from the fileOperations.
  private long readDataStartHeader() throws IOException {
    fileOperations.seek(DATA_START_HEADER_LOCATION);
    return fileOperations.readLong();
  }

  /// Loads existing index entries from file into memory.
  private void loadExistingIndex(int numRecords) throws IOException {
    for (int i = 0; i < numRecords; i++) {
      KeyWrapper key = readKeyFromIndex(i);
      RecordHeader header = readRecordHeaderFromIndex(i);

      memIndex.put(key, header);
      positionIndex.put(header.dataPointer, header);

      updateFreeSpaceIndex(header);
    }
  }

  /// Reads the ith key from the index.
  private KeyWrapper readKeyFromIndex(int position) throws IOException {
    final var fp = indexPositionToKeyFp(position);
    fileOperations.seek(fp);

    int len =
        fileOperations.readByte()
            & 0xFF; // interpret as unsigned byte https://stackoverflow.com/a/56052675/329496

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

    CRC32 crc = new CRC32();
    crc.update(key, 0, key.length);
    final var crc32actual = crc.getValue();

    FileRecordStore.logger.log(
        Level.FINEST,
        () ->
            String.format(
                "<k fp:%d idx:%d len:%d end:%d crc:%d key:%s bytes:%s",
                fp,
                position,
                len,
                fp + len,
                crc32actual,
                java.util.Base64.getEncoder().encodeToString(key),
                print(key)));

    if (crc32actual != crc32expected) {
      throw new IllegalStateException(
          String.format(
              "invalid key CRC32 expected %d and actual %s for len %d and fp %d found key %s with bytes %s",
              crc32expected,
              crc32actual,
              len,
              fp,
              java.util.Base64.getEncoder().encodeToString(key),
              print(key)));
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
    return FILE_HEADERS_REGION_LENGTH + ((long) indexEntryLength * pos);
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
    byte[] header = new byte[RECORD_HEADER_LENGTH];
    final var fp = in.getFilePointer();
    in.readFully(header);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                "<h fp:%d idx:%d len:%d bytes:%s", fp, index, header.length, print(header)));

    ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
    buffer.put(header);
    buffer.flip();

    RecordHeader rh = new RecordHeader(buffer.getLong(), buffer.getInt());
    rh.dataCount = buffer.getInt();
    rh.crc32 = buffer.getInt() & 0xFFFFFFFFL;

    final var array = buffer.array();
    CRC32 crc = new CRC32();
    crc.update(array, 0, 8 + 4 + 4);
    long crc32expected = crc.getValue();
    if (rh.crc32 != crc32expected) {
      throw new IllegalStateException(
          String.format("invalid header CRC32 expected %d for %s", crc32expected, rh));
    }
    return rh;
  }

  static int getMaxKeyLengthOrDefault() {
    final String key =
        String.format("%s.%s", FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY);
    String keyLength =
        System.getenv(key) == null
            ? Integer.valueOf(DEFAULT_MAX_KEY_LENGTH).toString()
            : System.getenv(key);
    keyLength = System.getProperty(key, keyLength);
    return Integer.parseInt(keyLength);
  }

  private static boolean getPadDataToKeyLengthOrDefaultTrue() {
    final String key =
        String.format("%s.%s", FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY);
    String keyLength =
        System.getenv(key) == null ? Boolean.valueOf(true).toString() : System.getenv(key);
    keyLength = System.getProperty(key, keyLength);
    return Boolean.parseBoolean(keyLength);
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
        new FileRecordStore.Builder()
            .path(filename)
            .accessMode(Builder.AccessMode.READ_ONLY)
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
                "%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                index,
                k,
                header.indexPosition,
                header.getDataCapacity(),
                header.dataCount,
                header.dataPointer,
                header.crc32));
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
    fileOperations.seek(header.dataPointer);
    byte[] lenBytes = new byte[4];
    fileOperations.readFully(lenBytes);
    int len = (new DataInputStream(new ByteArrayInputStream(lenBytes))).readInt();

    logger.log(
        Level.FINEST,
        () -> String.format("<d fp:%d len:%d bytes:%s ", header.dataPointer, len, print(lenBytes)));

    assert header.dataPointer + len < getFileLength()
        : String.format(
            "attempting to read up to %d beyond length of file %d",
            (header.dataCount + len), getFileLength());

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
                  header.dataPointer + 4, len, actualCrc, print(buf)));

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
      final var capacity = updateMeHeader.getDataCapacity();

      final var recordIsSameSize = value.length == capacity;
      final var recordIsSmaller = value.length < capacity;

      // can update in place if the record is same size no matter whether CRC32 is enabled.
      // for smaller records, allow in-place updates based on the allowInPlaceUpdates setting
      if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
        // write with the backup crc so one of the two CRCs will be valid after a crash
        writeRecordHeaderToIndex(updateMeHeader);
        updateMeHeader.dataCount = value.length;
        updateFreeSpaceIndex(updateMeHeader);
        // write the main data
        writeRecordData(updateMeHeader, value);
        // write the header with the main CRC
        writeRecordHeaderToIndex(updateMeHeader);
      } else { // Handle cases where in-place update is not possible
        final var endOfRecord = updateMeHeader.dataPointer + updateMeHeader.getDataCapacity();
        final var fileLength =
            getFileLength(); // perform a move. insert data to the end of the file then overwrite
        // header.
        if (endOfRecord == fileLength) {
          updateMeHeader.dataCount = value.length;
          setFileLength(fileLength + (value.length - updateMeHeader.getDataCapacity()));
          updateMeHeader.setDataCapacity(value.length);
          updateFreeSpaceIndex(updateMeHeader);
          writeRecordData(updateMeHeader, value);
          writeRecordHeaderToIndex(updateMeHeader);
        } else if (value.length > updateMeHeader.getDataCapacity()) {
          // allocate to next free space or expand the file
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded old record
          newRecord.dataCount = value.length;
          writeRecordData(newRecord, value);
          writeRecordHeaderToIndex(newRecord);
          memIndex.put(keyWrapper, newRecord);
          positionIndex.remove(updateMeHeader.dataPointer);
          positionIndex.put(newRecord.dataPointer, newRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer);
          }
        } else {
          // Not last record - need to move to new location
          // This handles both larger records and smaller records when in-place updates are disabled
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded/moved old record
          newRecord.dataCount = value.length;
          writeRecordData(newRecord, value);
          writeRecordHeaderToIndex(newRecord);
          memIndex.put(keyWrapper, newRecord);
          positionIndex.remove(updateMeHeader.dataPointer);
          positionIndex.put(newRecord.dataPointer, newRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer);
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
      deleteEntryFromIndex(delRec, currentNumRecords);
      final var memDeleted = memIndex.remove(keyWrapper);
      assert delRec == memDeleted;
      final var posDeleted = positionIndex.remove(delRec.dataPointer);
      assert delRec == posDeleted;
      assert memIndex.size() == positionIndex.size()
          : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
      freeMap.remove(delRec);

      if (getFileLength() == delRec.dataPointer + delRec.getDataCapacity()) {
        // shrink file since this is the last record in the file
        setFileLength(delRec.dataPointer);
      } else {
        final var previousOptional = getRecordAt(delRec.dataPointer - 1);
        if (previousOptional.isPresent()) {
          // append space of deleted record onto previous record
          final var previous = previousOptional.get();
          previous.incrementDataCapacity(delRec.getDataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          // make free space at the end of the index area
          writeDataStartPtrHeader(delRec.dataPointer + delRec.getDataCapacity());
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
    return (PAD_DATA_TO_KEY_LENGTH) ? Math.max(indexEntryLength, dataLength) : dataLength;
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
          if (targetFp >= rh.dataPointer
              && targetFp < rh.dataPointer + (long) rh.getDataCapacity()) {
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
              MAX_KEY_LENGTH_THEORETICAL,
              FileRecordStore.class.getName()));
    }

    writeKeyToIndex(key, currentNumRecords);

    fileOperations.seek(indexPositionToRecordHeaderFp(currentNumRecords));
    write(newRecord, fileOperations);
    newRecord.setIndexPosition(currentNumRecords);
    writeNumRecordsHeader(currentNumRecords + 1);

    logger.log(
        Level.FINEST,
        () -> String.format("before maps: %s | %s", memIndex.toString(), positionIndex.toString()));

    final var duplicate = memIndex.put(key, newRecord);
    if (duplicate != null) positionIndex.remove(duplicate.dataPointer);
    positionIndex.put(newRecord.dataPointer, newRecord);

    logger.log(
        Level.FINEST,
        () -> String.format("after maps: %s | %s", memIndex.toString(), positionIndex.toString()));

    assert memIndex.size() == positionIndex.size()
        : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
  }

  private void write(RecordHeader rh, FileOperations out) throws IOException {
    if (rh.dataCount < 0) {
      throw new IllegalStateException("dataCount has not been initialized " + this);
    }
    final var fp = out.getFilePointer();
    ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
    buffer.putLong(rh.dataPointer);
    buffer.putInt(rh.dataCapacity);
    buffer.putInt(rh.dataCount);
    final var array = buffer.array();
    CRC32 crc = new CRC32();
    crc.update(array, 0, 8 + 4 + 4);
    rh.crc32 = crc.getValue();
    int crc32int = (int) (rh.crc32 & 0xFFFFFFFFL);
    buffer.putInt(crc32int);
    out.write(buffer.array(), 0, RECORD_HEADER_LENGTH);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">h fp:%d idx:%d len:%d end:%d bytes:%s",
                fp, rh.indexPosition, array.length, fp + array.length, print(array)));
  }

  /// Removes the record from the index. Replaces the target with the entry at
  /// the end of the index.
  private void deleteEntryFromIndex(RecordHeader header, int currentNumRecords) throws IOException {
    if (header.indexPosition != currentNumRecords - 1) {
      final var lastKey = readKeyFromIndex(currentNumRecords - 1);
      RecordHeader last = keyToRecordHeader(lastKey);
      last.setIndexPosition(header.indexPosition);

      writeKeyToIndex(lastKey, last.indexPosition);

      fileOperations.seek(this.indexPositionToRecordHeaderFp(last.indexPosition));
      write(last, fileOperations);
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
    final var len = (byte) key.length();
    final var writeLen = key.length() + 1 + CRC32_LENGTH;

    ByteBuffer buffer = ByteBuffer.allocate(writeLen);
    buffer.put(len);
    buffer.put(key.bytes(), 0, key.length());

    // compute crc from the backing array with what we have written
    final var array = buffer.array();
    CRC32 crc = new CRC32();
    crc.update(array, 1, key.length());
    int crc32 = (int) (crc.getValue() & 0xFFFFFFFFL);

    // add the crc which will write through to the backing array
    buffer.putInt(crc32);

    final var fpk = indexPositionToKeyFp(index);
    fileOperations.seek(fpk);
    fileOperations.write(array, 0, writeLen);

    FileRecordStore.logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">k fp:%d idx:%d len:%d end:%d crc:%d key:%s bytes:%s",
                fpk,
                index,
                len & 0xFF,
                fpk + (len & 0xFF),
                crc32,
                java.util.Base64.getEncoder().encodeToString(key.bytes()),
                print(key.bytes())));
  }

  /// Writes the ith record header to the index.
  private void writeRecordHeaderToIndex(RecordHeader header) throws IOException {
    fileOperations.seek(indexPositionToRecordHeaderFp(header.indexPosition));
    write(header, fileOperations);
  }

  /// Inserts a new record. It tries to insert into free space at the end of the index space, or
  // free space between
  /// records, then finally extends the fileOperations. If the file has been set to a large initial
  // file it will initially all
  /// be considered space at the end of the index space such that inserts will be prepended into the
  // back of the
  /// fileOperations. When there is no more space in the index area the file will be expanded and
  // record(s) will be into the new
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
    ensureOpen();
    try {
      ensureNotReadOnly();
      logger.log(
          Level.FINE,
          () -> String.format("updateRecord value.len:%d key:%s", value.length, print(key)));
      final var keyWrapper = KeyWrapper.of(key, defensiveCopy);
      final var updateMeHeader = keyToRecordHeader(keyWrapper);
      final var capacity = updateMeHeader.getDataCapacity();

      final var recordIsSameSize = value.length == capacity;
      final var recordIsSmaller = value.length < capacity;

      // can update in place if the record is same size no matter whether CRC32 is enabled.
      // for smaller records, allow in-place updates based on the allowInPlaceUpdates setting
      if (recordIsSameSize || (recordIsSmaller && allowInPlaceUpdates)) {
        // write with the backup crc so one of the two CRCs will be valid after a crash
        writeRecordHeaderToIndex(updateMeHeader);
        updateMeHeader.dataCount = value.length;
        updateFreeSpaceIndex(updateMeHeader);
        // write the main data
        writeRecordData(updateMeHeader, value);
        // write the header with the main CRC
        writeRecordHeaderToIndex(updateMeHeader);
      } else { // Handle cases where in-place update is not possible
        final var endOfRecord = updateMeHeader.dataPointer + updateMeHeader.getDataCapacity();
        final var fileLength =
            getFileLength(); // perform a move. insert data to the end of the file then overwrite
        // header.
        if (endOfRecord == fileLength) {
          updateMeHeader.dataCount = value.length;
          setFileLength(fileLength + (value.length - updateMeHeader.getDataCapacity()));
          updateMeHeader.setDataCapacity(value.length);
          updateFreeSpaceIndex(updateMeHeader);
          writeRecordData(updateMeHeader, value);
          writeRecordHeaderToIndex(updateMeHeader);
        } else if (value.length > updateMeHeader.getDataCapacity()) {
          // allocate to next free space or expand the file
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded old record
          newRecord.dataCount = value.length;
          writeRecordData(newRecord, value);
          writeRecordHeaderToIndex(newRecord);
          memIndex.put(keyWrapper, newRecord);
          positionIndex.remove(updateMeHeader.dataPointer);
          positionIndex.put(newRecord.dataPointer, newRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer);
          }
        } else {
          // Not last record - need to move to new location
          // This handles both larger records and smaller records when in-place updates are disabled
          RecordHeader newRecord = allocateRecord(value.length);
          // new record is expanded/moved old record
          newRecord.dataCount = value.length;
          writeRecordData(newRecord, value);
          writeRecordHeaderToIndex(newRecord);
          memIndex.put(keyWrapper, newRecord);
          positionIndex.remove(updateMeHeader.dataPointer);
          positionIndex.put(newRecord.dataPointer, newRecord);
          assert memIndex.size() == positionIndex.size()
              : String.format(
                  "memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

          // if there is a previous record add space to it
          final var previousIndex = updateMeHeader.dataPointer - 1;
          final var previousOptional = getRecordAt(previousIndex);

          if (previousOptional.isPresent()) {
            RecordHeader previous = previousOptional.get();
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
          } else {
            // record free space at the end of the index area
            writeDataStartPtrHeader(updateMeHeader.dataPointer);
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

    assert data.length <= header.getDataCapacity() : "Record data does not fit";
    header.dataCount = data.length;

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bout);
    out.writeInt(header.dataCount);
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
    fileOperations.seek(header.dataPointer);
    fileOperations.write(payload, 0, payload.length); // drop
    byte[] lenBytes = Arrays.copyOfRange(payload, 0, 4);

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">d fp:%d len:%d end:%d bytes:%s",
                header.dataPointer,
                payload.length,
                header.dataPointer + payload.length,
                print(lenBytes)));

    logger.log(
        Level.FINEST,
        () ->
            String.format(
                ">d fp:%d len:%d end:%d crc:%d data:%s",
                header.dataPointer + 4,
                payload.length,
                header.dataPointer + payload.length,
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
      assert delRec == memDeleted;
      final var posDeleted = positionIndex.remove(delRec.dataPointer);
      assert delRec == posDeleted;
      assert memIndex.size() == positionIndex.size()
          : String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
      freeMap.remove(delRec);

      if (getFileLength() == delRec.dataPointer + delRec.getDataCapacity()) {
        // shrink file since this is the last record in the file
        setFileLength(delRec.dataPointer);
      } else {
        final var previousOptional = getRecordAt(delRec.dataPointer - 1);
        if (previousOptional.isPresent()) {
          // append space of deleted record onto previous record
          final var previous = previousOptional.get();
          previous.incrementDataCapacity(delRec.getDataCapacity());
          updateFreeSpaceIndex(previous);
          writeRecordHeaderToIndex(previous);
        } else {
          // make free space at the end of the index area
          writeDataStartPtrHeader(delRec.dataPointer + delRec.getDataCapacity());
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
                "Header expansion disabled and insufficient pre-allocated space. " +
                "Required: %d records, Current: %d records, Pre-allocated header space exhausted. " +
                "Consider increasing preallocatedRecords or enabling header expansion.",
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
      // TODO figure out if there is some case where someone could trigger the following
      // IllegalStateException
      final var first =
          firstOptional.orElseThrow(
              () -> new IllegalStateException("no record at dataStartPtr " + dataStartPtr));
      positionIndex.remove(first.dataPointer);
      freeMap.remove(first);
      byte[] data = readRecordData(first);
      long fileLen = getFileLength();
      first.dataPointer = fileLen;
      int dataLength = payloadLength(data.length);
      int dataLengthPadded = getDataLengthPadded(dataLength);
      first.setDataCapacity(dataLengthPadded);
      setFileLength(fileLen + dataLengthPadded);
      writeRecordData(first, data);
      writeRecordHeaderToIndex(first);
      positionIndex.put(first.dataPointer, first);
      dataStartPtr = positionIndex.ceilingEntry(dataStartPtr).getValue().dataPointer;
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
                    "%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                    finalIndex,
                    k,
                    header.indexPosition,
                    header.getDataCapacity(),
                    header.dataCount,
                    header.dataPointer,
                    header.crc32));
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
  /// The feature prevents header/index expansion during sequential scans to maintain memory stability.
  ///
  /// @param allow true to allow header expansion, false to disable it
  /// @throws UnsupportedOperationException if the store is read-only
  /// @throws IllegalStateException if the store is not in OPEN state
  public void setAllowHeaderExpansion(boolean allow) {
    ensureOpen();
    ensureNotReadOnly();
    this.allowHeaderExpansion = allow;
  }

  /// Returns whether header region expansion is allowed.
  ///
  /// @return true if header region expansion is allowed
  public boolean isAllowHeaderExpansion() {
    return allowHeaderExpansion;
  }

  /// Returns whether in-place updates are allowed for smaller records.
  ///
  /// @return true if in-place updates are allowed for smaller records
  public boolean isAllowInPlaceUpdates() {
    return allowInPlaceUpdates;
  }

  /// Builder for creating FileRecordStore instances with a fluent API inspired by H2 MVStore.
  /// This provides a secure, explicit way to configure and create stores.
  public static class Builder {
    private Path path;
    private String tempFilePrefix;
    private String tempFileSuffix;
    private int preallocatedRecords = 0;
    private int maxKeyLength = DEFAULT_MAX_KEY_LENGTH;
    private boolean disablePayloadCrc32 = false;
    private boolean useMemoryMapping = false;
    private AccessMode accessMode = AccessMode.READ_WRITE;
    private KeyType keyType = KeyType.BYTE_ARRAY;
    private boolean defensiveCopy = true;
    private boolean allowInPlaceUpdates = true;
    private boolean allowHeaderExpansion = true;

    /// Sets the path for the database fileOperations.
    ///
    /// @param path the path to the database file
    /// @return this builder for chaining
    public Builder path(Path path) {
      this.path = path;
      return this;
    }

    // private boolean inMemory = false; // TODO: Implement in-memory store support (see GitHub
    // issue #66)

    /// Sets the path for the database file using a string.
    /// The string will be converted to a Path and normalized.
    ///
    /// @param path the path string to the database file
    /// @return this builder for chaining
    public Builder path(String path) {
      this.path = Paths.get(path).normalize();
      return this;
    }

    /// Creates a temporary file for the database.
    /// The file will be automatically deleted on JVM exit.
    ///
    /// @param prefix the prefix for the temporary file
    /// @param suffix the suffix for the temporary file
    /// @return this builder for chaining
    public Builder tempFile(String prefix, String suffix) {
      this.tempFilePrefix = prefix;
      this.tempFileSuffix = suffix;
      return this;
    }

    /// Sets the number of records to pre-allocate space for.
    ///
    /// @param preallocatedRecords the number of records to pre-allocate
    /// @return this builder for chaining
    public Builder preallocatedRecords(int preallocatedRecords) {
      this.preallocatedRecords = preallocatedRecords;
      return this;
    }

    /// Configures the store to use UUID keys (16-byte) for optimized UUID storage.
    /// Sets maxKeyLength to 16 automatically and enables UUID-specific optimizations.
    ///
    /// @return this builder for chaining
    public Builder uuidKeys() {
      this.keyType = KeyType.UUID;
      this.maxKeyLength = 16;
      return this;
    }

    /// Configures the store to use byte array keys with specified maximum length.
    /// This is the default mode if neither uuidKeys() nor byteArrayKeys() is called.
    ///
    /// @param maxKeyLength the maximum key length in bytes
    /// @return this builder for chaining
    public Builder byteArrayKeys(int maxKeyLength) {
      this.keyType = KeyType.BYTE_ARRAY;
      this.maxKeyLength = maxKeyLength;
      return this;
    }

    /// Sets whether to use defensive copying for byte array keys.
    /// When true (default), byte arrays are cloned before storage to prevent external mutation.
    /// When false, zero-copy is used for performance-critical code with trusted callers.
    ///
    /// @param defensiveCopy true to enable defensive copying, false for zero-copy
    /// @return this builder for chaining
    public Builder defensiveCopy(boolean defensiveCopy) {
      this.defensiveCopy = defensiveCopy;
      return this;
    }

    /// Sets the maximum key length in bytes.
    ///
    /// @param maxKeyLength the maximum key length
    /// @return this builder for chaining
    public Builder maxKeyLength(int maxKeyLength) {
      this.maxKeyLength = maxKeyLength;
      return this;
    }

    /// Disables CRC32 checking for record payloads.
    ///
    /// @param disable true to disable CRC32, false to enable (default)
    /// @return this builder for chaining
    public Builder disablePayloadCrc32(boolean disable) {
      this.disablePayloadCrc32 = disable;
      return this;
    }

    /// Enables memory-mapped file access.
    ///
    /// @param useMemoryMapping true to enable memory mapping
    /// @return this builder for chaining
    public Builder useMemoryMapping(boolean useMemoryMapping) {
      this.useMemoryMapping = useMemoryMapping;
      return this;
    }

    /// Opens the store in read-only mode.
    ///
    /// @param readOnly true for read-only access
    /// @return this builder for chaining
    public Builder readOnly(boolean readOnly) {
      this.accessMode = readOnly ? AccessMode.READ_ONLY : AccessMode.READ_WRITE;
      return this;
    }

    /// Sets whether to allow header region expansion during operations.
    /// When true (default), the header region can expand to accommodate more records.
    /// When false, header expansion is disabled and operations will fail if pre-allocated
    /// space is exceeded. This is useful for snapshotting to maintain stable memory layout.
    ///
    /// @param allow true to allow header expansion, false to disable it
    /// @return this builder for chaining
    public Builder allowHeaderExpansion(boolean allow) {
      this.allowHeaderExpansion = allow;
      return this;
    }

    /// Sets the access mode for the store.
    ///
    /// @param accessMode the access mode (READ_ONLY or READ_WRITE)
    /// @return this builder for chaining
    public Builder accessMode(AccessMode accessMode) {
      this.accessMode = accessMode;
      return this;
    }

    /// Sets whether to allow in-place updates for smaller records regardless of CRC32 setting.
    /// When true (default), smaller records can be updated in-place even when CRC32 is disabled.
    /// When false, smaller records will use the dual-write pattern (old behavior).
    /// Same-size records can always be updated in-place regardless of this setting.
    ///
    /// @param allow true to allow in-place updates for smaller records, false to force dual-write
    /// @return this builder for chaining
    public Builder allowInPlaceUpdates(boolean allow) {
      this.allowInPlaceUpdates = allow;
      return this;
    }

    /// Opens the FileRecordStore.
    /// Automatically detects whether to create a new database or open an existing one.
    /// If tempFile is set, always creates a new temporary file.
    ///
    /// @return a new FileRecordStore instance
    /// @throws IOException if the store cannot be opened
    public FileRecordStore open() throws IOException {
      if (tempFilePrefix != null && tempFileSuffix != null) {
        // Create temporary file - always creates new
        Path tempPath = Files.createTempFile(tempFilePrefix, tempFileSuffix);
        tempPath.toFile().deleteOnExit();
        return new FileRecordStore(
            tempPath.toFile(),
            preallocatedRecords,
            maxKeyLength,
            disablePayloadCrc32,
            useMemoryMapping,
            accessMode.getMode(),
            keyType,
            defensiveCopy);
      }

      if (path == null) {
        throw new IllegalStateException("Either path or tempFile must be specified");
      }

      // AUTO-DETECTION LOGIC: Check if file exists and has valid headers
      if (Files.exists(path)) {
        // File exists - try to validate and open existing store
        try {
          // Check if it's a valid FileRecordStore file
          boolean isValid = isValidFileRecordStore(path, maxKeyLength);
          logger.log(Level.FINE, "File validation for " + path + ": " + isValid);

          if (isValid) {
            // Open existing - use preallocatedRecords=0 for existing files
            return new FileRecordStore(
                path.toFile(),
                0,
                maxKeyLength,
                disablePayloadCrc32,
                useMemoryMapping,
                accessMode.getMode(),
                keyType,
                defensiveCopy);
          } else {
            // File exists but isn't a valid store - create new store (overwrite)
            // This preserves backward compatibility with tests that expect overwrite behavior
            return new FileRecordStore(
                path.toFile(),
                preallocatedRecords,
                maxKeyLength,
                disablePayloadCrc32,
                useMemoryMapping,
                accessMode.getMode(),
                keyType,
                defensiveCopy);
          }
        } catch (IOException e) {
          // Can't read file - create new store (overwrite existing)
          return new FileRecordStore(
              path.toFile(),
              preallocatedRecords,
              maxKeyLength,
              disablePayloadCrc32,
              useMemoryMapping,
              accessMode.getMode(),
              keyType,
              defensiveCopy);
        }
      } else {
        // File doesn't exist - create new
        return new FileRecordStore(
            path.toFile(),
            preallocatedRecords,
            maxKeyLength,
            disablePayloadCrc32,
            useMemoryMapping,
            accessMode.getMode(),
            keyType,
            defensiveCopy);
      }
    }

    /// Validates that a file contains a valid FileRecordStore format.
    /// Checks file size and header structure to determine if it's a valid store.
    /// A valid store must have proper headers and key length matching expected value.
    ///
    /// @param path the path to validate
    /// @param expectedMaxKeyLength the expected max key length for validation
    /// @return true if the file appears to be a valid FileRecordStore
    /// @throws IOException if the file cannot be read
    private boolean isValidFileRecordStore(Path path, int expectedMaxKeyLength) throws IOException {
      try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path.toFile(), "r")) {
        // Empty files are never valid stores
        if (raf.length() == 0) {
          logger.log(Level.FINE, "Validation failed: file is empty");
          return false;
        }

        // Check if file has minimum required size for headers
        if (raf.length() < FILE_HEADERS_REGION_LENGTH) {
          logger.log(
              Level.FINE,
              "Validation failed: file too short ("
                  + raf.length()
                  + " < "
                  + FILE_HEADERS_REGION_LENGTH
                  + ")");
          return false;
        }

        // Read and validate the key length header (first byte)
        raf.seek(0);
        int keyLength;
        try {
          keyLength = raf.readByte() & 0xFF;
        } catch (EOFException e) {
          // File is too short to read header
          logger.log(Level.FINE, "Validation failed: EOF reading key length");
          return false;
        }

        // Key length must be non-negative and within reasonable bounds
        // Allow keyLength == 0 as valid (maxKeyLength could be 0)
        logger.log(
            Level.FINEST,
            "Validation: keyLength=" + keyLength + " expected=" + expectedMaxKeyLength);
        if (keyLength > MAX_KEY_LENGTH_THEORETICAL) {
          logger.log(Level.FINE, "Validation failed: invalid key length " + keyLength);
          return false;
        }

        // Validate that file's key length matches expected maxKeyLength
        if (keyLength != expectedMaxKeyLength) {
          logger.log(
              Level.FINE,
              "Validation failed: file key length "
                  + keyLength
                  + " does not match expected "
                  + expectedMaxKeyLength);
          return false;
        }

        // Additional validation: check if we can read the number of records header
        try {
          raf.seek(NUM_RECORDS_HEADER_LOCATION);
          int numRecords = raf.readInt();
          // Number of records should be non-negative and reasonable
          if (numRecords < 0 || numRecords > 1000000) { // Arbitrary reasonable upper bound
            logger.log(Level.FINE, "Validation failed: invalid numRecords " + numRecords);
            return false;
          }
        } catch (EOFException e) {
          logger.log(Level.FINE, "Validation failed: EOF reading numRecords");
          return false;
        }

        return true;
      }
    }

    /// Access mode for opening FileRecordStore instances
    /// Access mode for opening a FileRecordStore.
    public enum AccessMode {
      /// Read-only access mode - files are opened for reading only.
      READ_ONLY("r"),
      /// Read-write access mode - files can be read and modified.
      READ_WRITE("rw");

      private final String mode;

      AccessMode(String mode) {
        this.mode = mode;
      }

      String getMode() {
        return mode;
      }
    }
  }
}
