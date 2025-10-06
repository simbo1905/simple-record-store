package com.github.simbo1905.nfp.srs;

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

  // Core constants moved from FileRecordStore
  /// Magic number identifying valid FileRecordStore files (0xBEEBBEEB).
  /// Placed at the start of every file to detect corruption and incompatible formats.
  static final int MAGIC_NUMBER = 0xBEEBBEEB;
  /// Default maximum key length in bytes. Optimized for SSD performance and modern hash sizes.
  public static final int DEFAULT_MAX_KEY_LENGTH = 128;
  /// Theoretical maximum key length based on file format constraints (Short.MAX_VALUE - 4).
  public static final int MAX_KEY_LENGTH_THEORETICAL = Short.MAX_VALUE - Integer.BYTES;

  // File format constants
  /// File index to the magic number header.
  static final long MAGIC_NUMBER_HEADER_LOCATION = 0;
  /// File index to the key length header (after magic number).
  static final long KEY_LENGTH_HEADER_LOCATION = Integer.BYTES;
  /// File index to the num records header.
  static final long NUM_RECORDS_HEADER_LOCATION = Integer.BYTES + Short.BYTES;
  /// File index to the start of the data region beyond the index region
  static final long DATA_START_HEADER_LOCATION = Integer.BYTES + Short.BYTES + Integer.BYTES;
  /// Total length in bytes of the global database headers.
  static final int FILE_HEADERS_REGION_LENGTH =
      Integer.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES;

  // Default sizing constants
  // 1 MiB
  // 4 KiB
  private static final int DEFAULT_INITIAL_HEADER_SIZE = 64 * 1024; // 64 KiB

  private Path path;
  private String tempFilePrefix;
  private String tempFileSuffix;
  private int preallocatedRecords =
      1024; // SSD optimized: pre-allocate for better sequential performance
  private int maxKeyLength =
      FileRecordStore.DEFAULT_MAX_KEY_LENGTH; // 128 bytes - optimized for SHA256/SHA512
  private boolean disablePayloadCrc32 = false; // SSD optimized: keep CRC32 enabled
  private boolean useMemoryMapping = true; // SSD optimized: enable by default
  private AccessMode accessMode = AccessMode.READ_WRITE;
  private boolean defensiveCopy = true;
  private int hintInitialKeyCount = 0; // 0 means use default calculation
  private int hintPreferredBlockSize = 4; // 4 KiB default
  private int hintPreferredExpandSize = 1; // 1 MiB default

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
  /// @param maxKeyLength the maximum key length (1-32763 bytes)
  /// @return this builder for chaining
  public FileRecordStoreBuilder maxKeyLength(int maxKeyLength) {
    if (maxKeyLength < 1 || maxKeyLength > FileRecordStore.MAX_KEY_LENGTH_THEORETICAL) {
      throw new IllegalArgumentException(
          String.format(
              "maxKeyLength must be between 1 and %d, got %d",
              FileRecordStore.MAX_KEY_LENGTH_THEORETICAL, maxKeyLength));
    }
    this.maxKeyLength = maxKeyLength;
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
    this.hintPreferredBlockSize = blockSizeKiB;
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
    this.hintPreferredExpandSize = expandSizeMiB;
    return this;
  }

  /// Resolves the final sizing parameters based on user hints and defaults.
  /// Applies 8-byte alignment to key length and calculates derived values.
  ///
  /// @return Config object containing resolved values
  Config build() {
    // Apply 8-byte alignment: round key length + CRC up to nearest 8 bytes
    int alignedKeyLength = ((maxKeyLength + 4 + 7) / 8) * 8;

    // Convert user-friendly units to bytes
    int expansionSize = hintPreferredExpandSize * 1024 * 1024; // MiB to bytes
    int blockSize = hintPreferredBlockSize * 1024; // KiB to bytes

    // Calculate initial header region size based on key count hint
    int initialHeaderSize;
    if (hintInitialKeyCount > 0) {
      // User provided hint: calculate based on key count
      int indexEntryLength = alignedKeyLength + 1 + 4 + 20; // key + len + crc + header
      initialHeaderSize =
          Math.max(hintInitialKeyCount * indexEntryLength, DEFAULT_INITIAL_HEADER_SIZE);
    } else {
      // Use default
      initialHeaderSize = DEFAULT_INITIAL_HEADER_SIZE;
    }

    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Resolved sizing: alignedKeyLength=%d, expansionSize=%d, blockSize=%d, initialHeaderSize=%d",
                alignedKeyLength, expansionSize, blockSize, initialHeaderSize));

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
          defensiveCopy,
          config.expansionSize,
          config.blockSize,
          config.initialHeaderSize);
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
              defensiveCopy,
              config.expansionSize,
              config.blockSize,
              config.initialHeaderSize);
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
              defensiveCopy,
              config.expansionSize,
              config.blockSize,
              config.initialHeaderSize);
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
            defensiveCopy,
            config.expansionSize,
            config.blockSize,
            config.initialHeaderSize);
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
          defensiveCopy,
          config.expansionSize,
          config.blockSize,
          config.initialHeaderSize);
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

      // First check the magic number to determine file format
      raf.seek(0);
      int magicNumber;
      try {
        magicNumber = raf.readInt();
      } catch (java.io.EOFException e) {
        logger.log(Level.FINE, "Validation failed: EOF reading magic number");
        return false;
      }

      int keyLength;
      int numRecords;

      if (magicNumber == FileRecordStore.MAGIC_NUMBER) {
        // New format with magic number
        try {
          raf.seek(KEY_LENGTH_HEADER_LOCATION);
          keyLength = raf.readShort() & 0xFFFF;
          raf.seek(NUM_RECORDS_HEADER_LOCATION);
          numRecords = raf.readInt();
        } catch (java.io.EOFException e) {
          logger.log(Level.FINE, "Validation failed: EOF reading new format headers");
          return false;
        }
      } else {
        // Old format without magic number - reject it
        logger.log(Level.FINE, "Validation failed: old format file without magic number");
        return false;
      }

      // Validate key length
      if (keyLength > FileRecordStore.MAX_KEY_LENGTH_THEORETICAL) {
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
