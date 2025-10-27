package com.github.simbo1905.nfp.srs;

import static com.github.simbo1905.nfp.srs.RecordHeader.ENVELOPE_SIZE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Builder for creating FileRecordStore instances with a fluent API inspired by H2 MVStore.
/// Provides user-friendly sizing hints that are converted to constructor parameters.
///
/// Example usage:
/// <pre>
/// FileRecordStore store = new FileRecordStoreBuilder()
///     .path("/path/to/store.dat")
///     .maxKeyLength(128)
///     .hintPreferredBlockSize(4)  // 4 KiB
///     .hintPreferredExpandSize(1)  // 1 MiB
///     .hintInitialKeyCount(1000)
///     .useMemoryMapping(true)
///     .open();
/// </pre>
public class FileRecordStoreBuilder {

  private static final Logger logger = Logger.getLogger(FileRecordStoreBuilder.class.getName());
  private int maxKeyLength;

  /// Access mode for the FileRecordStore.
  @SuppressWarnings("LombokGetterMayBeUsed")
  public enum AccessMode {
    READ_ONLY("r"),
    READ_WRITE("rw");

    final String mode;

    AccessMode(String mode) {
      this.mode = mode;
    }

    public String getMode() {
      return mode;
    }
  }

  /// Key type for optimized key handling.
  /// Uses the KeyType enum from the main package.
  private com.github.simbo1905.nfp.srs.KeyType keyType =
      com.github.simbo1905.nfp.srs.KeyType.BYTE_ARRAY;

  /// Magic number identifying valid FileRecordStore files (0xBEEBBEEBBEEBBEEB).
  /// Placed at the start of every file to detect corruption and incompatible formats.
  /// Written as long (8 bytes) for 64-byte aligned file header.
  static final long MAGIC_NUMBER = 0xBEEBBEEBBEEBBEEBL;
  /// Default maximum key length in bytes. Optimized for SSD performance and modern hash sizes.
  public static final int DEFAULT_MAX_KEY_LENGTH = 128;
  /// Theoretical maximum key length based on file format constraints
  private static final int MAX_KEY_LENGTH_THEORETICAL = Short.MAX_VALUE - ENVELOPE_SIZE;
  /// Maximum key length allowing for 8-byte alignment = 32760 bytes.
  /// This is the largest multiple of 8 that fits within the theoretical maximum.
  /// Calculated as (MAX_KEY_LENGTH_THEORETICAL / 8) * 8 to ensure proper memory alignment.
  public static final int MAX_KEY_LENGTH = (MAX_KEY_LENGTH_THEORETICAL / 8) * 8;

  // File format constants
  /// File index to the magic number header.
  static final long MAGIC_NUMBER_HEADER_LOCATION = 0;
  /// File index to the key length header (after magic number).
  /// Key length is written as long but read back as short for alignment.
  static final long KEY_LENGTH_HEADER_LOCATION = Long.BYTES;
  /// File index to the num records header.
  /// Record count is written as long but read back as int for alignment.
  static final long NUM_RECORDS_HEADER_LOCATION = Long.BYTES + Long.BYTES;
  /// File index to the start of the data region beyond the index region
  static final long DATA_START_HEADER_LOCATION = Long.BYTES + Long.BYTES + Long.BYTES;
  /// Total length in bytes of the global database headers (before padding).
  /// Magic (8) + KeyLength (8) + RecordCount (8) + DataStartPtr (8) = 32 bytes.
  /// Padded to 64 bytes minimum with 32 bytes reserved for future metadata.
  static final int FILE_HEADERS_REGION_LENGTH = 64;

  // Default sizing constants
  // 1 MiB
  // 4 KiB
  private static final int DEFAULT_INITIAL_HEADER_SIZE = 64 * 1024; // 64 KiB

  private Path path;
  private String tempFilePrefix;
  private String tempFileSuffix;
  private int preallocatedRecords =
      1024; // SSD optimized: pre-allocate for better sequential performance

  private boolean disablePayloadCrc32 = false; // SSD optimized: keep CRC32 enabled
  private boolean useMemoryMapping = true; // SSD optimized: enable by default
  private AccessMode accessMode = AccessMode.READ_WRITE;

  /// Safety flag to allow zero pre-allocation (dangerous configuration)
  private boolean allowZeroPreallocation = false;
  private boolean defensiveCopy = true;
  private int hintInitialKeyCount = 0; // 0 means use default calculation
  private int hintPreferredBlockSize = 4 * 1024 * 1024; // 4 MiB default (general alignment size; SSD erasure blocks typically range from 256 KiB to 4 MiB depending on the drive)
  private int hintPreferredExpandSize = 2 * 1024 * 1024; // 2 MiB default

  /// Extra capacity percent for expansion (0.0 to 1.0), default 0.2 for 20% growth.
  /// This is converted to expansionMultiplier (1.0 + expansionExtraPercent) in the store.
  private double expansionExtraPercent = 0.2;

  /// Hint for average record size in bytes (default 2 KiB).
  /// Used to estimate initial header region size based on preferredBlockSize.
  private int averageRecordSizeHintBytes = 2048;

  /// Sets the path for the database file.
  ///
  /// @param path the path to the database file
  /// @return this builder for chaining
  public FileRecordStoreBuilder path(Path path) {
    this.path = path;
    return this;
  }

  /// Sets the path for the database file using a string.
  /// The string will be converted to a Path and normalized.
  ///
  /// @param path the path string to the database file
  /// @return this builder for chaining
  public FileRecordStoreBuilder path(String path) {
    this.path = Paths.get(path).normalize();
    return this;
  }

  /// Creates a temporary file for the database.
  /// The file will be automatically deleted on JVM exit.
  ///
  /// @param prefix the prefix for the temporary file
  /// @param suffix the suffix for the temporary file
  /// @return this builder for chaining
  public FileRecordStoreBuilder tempFile(String prefix, String suffix) {
    this.tempFilePrefix = prefix;
    this.tempFileSuffix = suffix;
    return this;
  }

  /// Sets the number of records to pre-allocate space for.
  ///
  /// @param preallocatedRecords the number of records to pre-allocate
  /// @return this builder for chaining
  public FileRecordStoreBuilder preallocatedRecords(int preallocatedRecords) {
    this.preallocatedRecords = preallocatedRecords;
    return this;
  }

  /// Configures the store to use UUID keys (16-byte) for optimized UUID storage.
  /// Sets maxKeyLength to 16 automatically and enables UUID-specific optimizations.
  ///
  /// @return this builder for chaining
  public FileRecordStoreBuilder uuidKeys() {
    this.keyType = com.github.simbo1905.nfp.srs.KeyType.UUID;
    this.maxKeyLength = 16;
    return this;
  }

  /// Sets whether to use defensive copying for byte array keys.
  /// When true (default), byte arrays are cloned before storage to prevent external mutation.
  /// When false, zero-copy is used for performance-critical code with trusted callers.
  ///
  /// @param defensiveCopy true to enable defensive copying, false for zero-copy
  /// @return this builder for chaining
  public FileRecordStoreBuilder defensiveCopy(boolean defensiveCopy) {
    this.defensiveCopy = defensiveCopy;
    return this;
  }

  /// Sets the maximum key length in bytes.
  /// Required parameter - no default value provided by builder.
  ///
  /// @param maxKeyLength the maximum key length (1-32760 bytes for 8-byte alignment)
  /// @return this builder for chaining
  public FileRecordStoreBuilder maxKeyLength(int maxKeyLength) {
    if (maxKeyLength < 1 || maxKeyLength > MAX_KEY_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "maxKeyLength must be between 1 and %d (8-byte aligned maximum), got %d",
              MAX_KEY_LENGTH, maxKeyLength));
    }
    this.maxKeyLength = maxKeyLength;
    return this;
  }

  /// Opt-in to dangerous zero pre-allocation (header expansion on first insert).
  ///
  /// @return this builder for chaining
  public FileRecordStoreBuilder allowZeroPreallocation() {
    this.allowZeroPreallocation = true;
    return this;
  }

  /// Disables CRC32 checking for record payloads.
  ///
  /// @param disable true to disable CRC32, false to enable (default)
  /// @return this builder for chaining
  public FileRecordStoreBuilder disablePayloadCrc32(boolean disable) {
    this.disablePayloadCrc32 = disable;
    return this;
  }

  /// Enables memory-mapped file access.
  ///
  /// @param useMemoryMapping true to enable memory mapping
  /// @return this builder for chaining
  public FileRecordStoreBuilder useMemoryMapping(boolean useMemoryMapping) {
    this.useMemoryMapping = useMemoryMapping;
    return this;
  }

  /// Opens the store in read-only mode.
  ///
  /// @param readOnly true for read-only access
  /// @return this builder for chaining
  public FileRecordStoreBuilder readOnly(boolean readOnly) {
    this.accessMode = readOnly ? AccessMode.READ_ONLY : AccessMode.READ_WRITE;
    return this;
  }

  /// Sets the access mode for the store.
  ///
  /// @param accessMode the access mode (READ_ONLY or READ_WRITE)
  /// @return this builder for chaining
  public FileRecordStoreBuilder accessMode(AccessMode accessMode) {
    this.accessMode = accessMode;
    return this;
  }

  /// Sets the initial key count hint for sizing the header region.
  /// The builder will calculate the optimal initial header size based on this count.
  /// If not set, the builder uses default calculations.
  ///
  /// @param keyCount the estimated number of initial keys
  /// @return this builder for chaining
  public FileRecordStoreBuilder hintInitialKeyCount(int keyCount) {
    if (keyCount < 0) {
      throw new IllegalArgumentException(
          "hintInitialKeyCount must be non-negative, got " + keyCount);
    }
    this.hintInitialKeyCount = keyCount;
    return this;
  }

  /// Sets the preferred block size hint in KiB (must be power of 2).
  /// Common values: 1, 2, 4, 8, 16, 32, 64 KiB.
  /// The builder converts this to bytes for the constructor.
  ///
  /// @param blockSizeKiB the preferred block size in KiB (must be power of 2)
  /// @return this builder for chaining
  public FileRecordStoreBuilder hintPreferredBlockSize(int blockSizeKiB) {
    if (blockSizeKiB <= 0 || (blockSizeKiB & (blockSizeKiB - 1)) != 0) {
      throw new IllegalArgumentException(
          "hintPreferredBlockSize must be positive and power of 2, got " + blockSizeKiB);
    }
    this.hintPreferredBlockSize = blockSizeKiB * 1024;
    return this;
  }

  /// Sets the preferred expansion size hint in MiB.
  /// The builder converts this to bytes for the constructor.
  ///
  /// @param expandSizeMiB the preferred expansion size in MiB
  /// @return this builder for chaining
  public FileRecordStoreBuilder hintPreferredExpandSize(int expandSizeMiB) {
    if (expandSizeMiB <= 0) {
      throw new IllegalArgumentException(
          "hintPreferredExpandSize must be positive, got " + expandSizeMiB);
    }
    this.hintPreferredExpandSize = expandSizeMiB * 1024 * 1024;
    return this;
  }

  /// Sets the expansion extra percent for header and data region growth.
  /// Must be between 0.0 (exclusive) and 1.0 (exclusive).
  /// Default is 0.2 (20% growth, i.e., 1.2x multiplier).
  ///
  /// @param percent the expansion percentage (0.0 < percent < 1.0)
  /// @return this builder for chaining
  public FileRecordStoreBuilder withExpansionExtraPercent(double percent) {
    if (percent <= 0.0 || percent >= 1.0) {
      throw new IllegalArgumentException(
          "expansionExtraPercent must be between 0.0 and 1.0 (exclusive), got " + percent);
    }
    this.expansionExtraPercent = percent;
    return this;
  }

  /// Sets the average record size hint in bytes.
  /// Used to estimate initial header region size.
  /// Default is 2048 bytes (2 KiB).
  ///
  /// @param bytes the average record size in bytes
  /// @return this builder for chaining
  public FileRecordStoreBuilder withAverageRecordSizeHint(int bytes) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("averageRecordSizeHint must be positive, got " + bytes);
    }
    this.averageRecordSizeHintBytes = bytes;
    return this;
  }

  /// Resolves the final sizing parameters based on user hints and defaults.
  /// Applies 8-byte alignment to key length and calculates derived values.
  ///
  /// @return Config object containing resolved values
  Config build() {
    // Apply 8-byte alignment: round key length + short length + CRC up to nearest 8 bytes, but cap
    // at maxKeyLength
    int keyDataLength = maxKeyLength + Short.BYTES + Integer.BYTES; // key + short length + int CRC
    int alignedKeyLength = ((keyDataLength + 7) / 8) * 8;
    // Ensure we don't exceed the actual max key length to avoid padding issues
    alignedKeyLength = Math.min(alignedKeyLength, maxKeyLength + Short.BYTES + Integer.BYTES);
    final var finalAlignedKeyLength = alignedKeyLength;

    // Use already-converted values (now in bytes)
    int expansionSize = hintPreferredExpandSize;
    int blockSize = hintPreferredBlockSize;

    // Calculate initial header region size based on key count hint or average record size
    int initialHeaderSize;
    if (hintInitialKeyCount > 0) {
      // User provided hint: calculate based on key count
      int indexEntryLength =
          alignedKeyLength + Short.BYTES + Integer.BYTES + 20; // key + short len + int crc + header
      initialHeaderSize =
          Math.max(hintInitialKeyCount * indexEntryLength, DEFAULT_INITIAL_HEADER_SIZE);
    } else {
      // Estimate based on average record size and preferredBlockSize
      int estimatedRecordCount = blockSize / averageRecordSizeHintBytes;
      int indexEntryLength = alignedKeyLength + Short.BYTES + Integer.BYTES + 20;
      int headerBytes = estimatedRecordCount * indexEntryLength + FILE_HEADERS_REGION_LENGTH;
      // Round up to nearest 4 KiB block
      initialHeaderSize = ((headerBytes + 4095) / 4096) * 4096;
      initialHeaderSize = Math.max(initialHeaderSize, DEFAULT_INITIAL_HEADER_SIZE);
    }

    final var finalInitialHeaderSize = initialHeaderSize;

    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Resolved sizing: alignedKeyLength=%d, expansionSize=%d, blockSize=%d, initialHeaderSize=%d",
                finalAlignedKeyLength, expansionSize, blockSize, finalInitialHeaderSize));

    return new Config(expansionSize, blockSize, initialHeaderSize);
  }

  /// Package-private record to hold resolved sizing parameters
  record Config(int expansionSize, int blockSize, int initialHeaderSize) {}

  /// Opens the FileRecordStore.
  /// Automatically detects whether to create a new database or open an existing one.
  /// If tempFile is set, always creates a new temporary file.
  ///
  /// @return a new FileRecordStore instance
  /// @throws IOException if the store cannot be opened
  public FileRecordStore open() throws IOException {
    // Build the configuration first
    Config config = build();

    // Safety check: refuse read-write stores with zero pre-allocation unless explicitly opted in
    if (accessMode == AccessMode.READ_WRITE
        && preallocatedRecords == 0
        && !allowZeroPreallocation) {
      throw new IllegalArgumentException(
          "Cannot create read-write FileRecordStore with zero pre-allocation. "
              + "This dangerous configuration can lead to header space exhaustion. "
              + "Either: (1) Add .allowZeroPreallocation() to explicitly opt-in, "
              + "(2) Use .preallocatedRecords(n) with n > 0, or "
              + "(3) Open in read-only mode with .accessMode(AccessMode.READ_ONLY)");
    }

    if (tempFilePrefix != null && tempFileSuffix != null) {
      // Create temporary file - always creates new
      Path tempPath = Files.createTempFile(tempFilePrefix, tempFileSuffix);
      tempPath.toFile().deleteOnExit();

      // Safety check: refuse read-write stores with zero pre-allocation unless explicitly opted in
      if (accessMode == AccessMode.READ_WRITE
          && preallocatedRecords == 0
          && !allowZeroPreallocation) {
        throw new IllegalArgumentException(
            "Cannot create read-write FileRecordStore with zero pre-allocation. "
                + "This dangerous configuration can lead to header space exhaustion. "
                + "Either: (1) Add .allowZeroPreallocation() to explicitly opt-in, "
                + "(2) Use .preallocatedRecords(n) with n > 0, or "
                + "(3) Open in read-only mode with .accessMode(AccessMode.READ_ONLY)");
      }

      // Log severe error if maxKeyLength is 0 to help identify test issues
      if (maxKeyLength == 0) {
        logger.log(
            Level.SEVERE,
            "FileRecordStoreBuilder.open() called with maxKeyLength=0. This will cause IllegalArgumentException. "
                + "Builder configuration: tempFilePrefix={0}, tempFileSuffix={1}, preallocatedRecords={2}, keyType={3}",
            new Object[] {tempFilePrefix, tempFileSuffix, preallocatedRecords, keyType});
      }

      return new FileRecordStore(
          tempPath.toFile(),
          preallocatedRecords,
          maxKeyLength,
          disablePayloadCrc32,
          useMemoryMapping,
          accessMode.getMode(),
          keyType,
          defensiveCopy,
          config.expansionSize,
          config.blockSize,
          config.initialHeaderSize,
          expansionExtraPercent);
    }

    if (path == null) {
      throw new IllegalStateException("Either path or tempFile must be specified");
    }

    // AUTO-DETECTION LOGIC: Check if file exists and has valid headers
    if (Files.exists(path)) {
      // File exists - try to validate and open existing store
      try {
        // Log severe error if maxKeyLength is 0 to help identify test issues
        if (maxKeyLength == 0) {
          logger.log(
              Level.SEVERE,
              "FileRecordStoreBuilder.open() called with maxKeyLength=0 for existing file. This will cause IllegalArgumentException. "
                  + "Builder configuration: path={0}, accessMode={1}, keyType={2}",
              new Object[] {path, accessMode, keyType});
        }

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
              defensiveCopy,
              config.expansionSize,
              config.blockSize,
              config.initialHeaderSize,
              expansionExtraPercent);
        } else {
          // File exists but isn't a valid store
          // Only delete if opening in READ_WRITE mode; otherwise fail fast
          if (accessMode == AccessMode.READ_ONLY) {
            throw new IllegalArgumentException(
                "File exists at " + path + " but is not a valid FileRecordStore. "
                    + "Cannot open in READ_ONLY mode.");
          }
          
          // Delete invalid file and create new store (overwrite behavior)
          logger.log(
              Level.WARNING,
              "Deleting invalid store file at " + path + " before creating new store");
          try {
            Files.deleteIfExists(path);
          } catch (SecurityException se) {
            throw new IOException(
                "Permission denied when attempting to delete invalid file: " + path, se);
          }

          // Safety check: refuse read-write stores with zero pre-allocation unless explicitly opted
          // in
          if (accessMode == AccessMode.READ_WRITE
              && preallocatedRecords == 0
              && !allowZeroPreallocation) {
            throw new IllegalArgumentException(
                "Cannot create read-write FileRecordStore with zero pre-allocation. "
                    + "This dangerous configuration can lead to header space exhaustion. "
                    + "Either: (1) Add .allowZeroPreallocation() to explicitly opt-in, "
                    + "(2) Use .preallocatedRecords(n) with n > 0, or "
                    + "(3) Open in read-only mode with .accessMode(AccessMode.READ_ONLY)");
          }

          return new FileRecordStore(
              path.toFile(),
              preallocatedRecords,
              maxKeyLength,
              disablePayloadCrc32,
              useMemoryMapping,
              accessMode.getMode(),
              keyType,
              defensiveCopy,
              config.expansionSize,
              config.blockSize,
              config.initialHeaderSize,
              expansionExtraPercent);
        }
      } catch (IOException e) {
        // Can't read file - only delete if opening in READ_WRITE mode
        if (accessMode == AccessMode.READ_ONLY) {
          throw new IOException(
              "Failed to validate existing file at " + path + " in READ_ONLY mode", e);
        }
        
        // Delete corrupted file and create new store
        logger.log(
            Level.WARNING,
            "Failed to validate store file at " + path + ": " + e.getMessage() 
                + ". Deleting and creating new store");
        try {
          Files.deleteIfExists(path);
        } catch (SecurityException se) {
          throw new IOException(
              "Permission denied when attempting to delete corrupted file: " + path, se);
        }

        // Safety check: refuse read-write stores with zero pre-allocation unless explicitly opted
        // in
        if (accessMode == AccessMode.READ_WRITE
            && preallocatedRecords == 0
            && !allowZeroPreallocation) {
          throw new IllegalArgumentException(
              "Cannot create read-write FileRecordStore with zero pre-allocation. "
                  + "This dangerous configuration can lead to header space exhaustion. "
                  + "Either: (1) Add .allowZeroPreallocation() to explicitly opt-in, "
                  + "(2) Use .preallocatedRecords(n) with n > 0, or "
                  + "(3) Open in read-only mode with .accessMode(AccessMode.READ_ONLY)");
        }

        return new FileRecordStore(
            path.toFile(),
            preallocatedRecords,
            maxKeyLength,
            disablePayloadCrc32,
            useMemoryMapping,
            accessMode.getMode(),
            keyType,
            defensiveCopy,
            config.expansionSize,
            config.blockSize,
            config.initialHeaderSize,
            expansionExtraPercent);
      }
    } else {
      // File doesn't exist - create new

      // Safety check: refuse read-write stores with zero pre-allocation unless explicitly opted in
      if (accessMode == AccessMode.READ_WRITE
          && preallocatedRecords == 0
          && !allowZeroPreallocation) {
        throw new IllegalArgumentException(
            "Cannot create read-write FileRecordStore with zero pre-allocation. "
                + "This dangerous configuration can lead to header space exhaustion. "
                + "Either: (1) Add .allowZeroPreallocation() to explicitly opt-in, "
                + "(2) Use .preallocatedRecords(n) with n > 0, or "
                + "(3) Open in read-only mode with .accessMode(AccessMode.READ_ONLY)");
      }

      // Log severe error if maxKeyLength is 0 to help identify test issues
      if (maxKeyLength == 0) {
        logger.log(
            Level.SEVERE,
            "FileRecordStoreBuilder.open() called with maxKeyLength=0 for new file. This will cause IllegalArgumentException. "
                + "Builder configuration: path={0}, preallocatedRecords={1}, accessMode={2}, keyType={3}",
            new Object[] {path, preallocatedRecords, accessMode, keyType});
      }

      return new FileRecordStore(
          path.toFile(),
          preallocatedRecords,
          maxKeyLength,
          disablePayloadCrc32,
          useMemoryMapping,
          accessMode.getMode(),
          keyType,
          defensiveCopy,
          config.expansionSize,
          config.blockSize,
          config.initialHeaderSize,
          expansionExtraPercent);
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
      // Empty files are valid - they will be initialized as new stores
      if (raf.length() == 0) {
        logger.log(Level.FINE, "Empty file detected - will initialize as new store");
        return true;
      }

      // Check if file has minimum required size for headers (64 bytes)
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

      // Read magic number (long, 8 bytes)
      raf.seek(0);
      long magicNumber;
      try {
        magicNumber = raf.readLong();
      } catch (java.io.EOFException e) {
        logger.log(Level.FINE, "Validation failed: EOF reading magic number");
        return false;
      }

      int keyLength;
      int numRecords;

      if (magicNumber == MAGIC_NUMBER) {
        // Valid format with magic number
        try {
          raf.seek(KEY_LENGTH_HEADER_LOCATION);
          long keyLengthLong = raf.readLong();
          keyLength = (int) keyLengthLong;
          raf.seek(NUM_RECORDS_HEADER_LOCATION);
          long numRecordsLong = raf.readLong();
          numRecords = (int) numRecordsLong;
        } catch (java.io.EOFException e) {
          logger.log(Level.FINE, "Validation failed: EOF reading new format headers");
          return false;
        }
      } else {
        // Invalid magic number - reject
        logger.log(
            Level.FINE,
            "Validation failed: invalid magic number 0x" + Long.toHexString(magicNumber));
        return false;
      }

      // Validate key length
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

      // Validate number of records (no upper bound, only non-negative)
      if (numRecords < 0) {
        logger.log(Level.FINE, "Validation failed: negative numRecords " + numRecords);
        return false;
      }

      return true;
    }
  }
}
