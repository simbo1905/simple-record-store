package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import org.junit.Assert;
import org.junit.Test;

public class FileRecordStoreConstructorResourceLeakTest extends JulLoggingConfig {

  @Test
  public void demonstrateResourceLeakOnKeyLengthValidationFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing resource leak on key length validation failure ===");

    Path tempFile = Files.createTempFile("resource-leak-key-len", ".dat");

    try {
      // Create a valid store first
      try (FileRecordStore store1 =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        store1.insertRecord(("testkey".getBytes()), "testdata".getBytes());
      }

      // Corrupt the key length header to trigger validation failure
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.seek(4); // Key length position in new format (after magic number)
        raf.writeByte(32); // Change key length from 64 to 32
      }

      logger.log(Level.FINE, "Attempting to open store with mismatched key length...");

      // This should fail with IllegalArgumentException
      try (@SuppressWarnings("unused") FileRecordStore store2 =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(64) // Expect 64, but file has 32
              .open()) {
        Assert.fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        logger.log(Level.FINE, "✓ Got expected validation exception: " + e.getMessage());

        // Now try to test if resources were leaked
        // If resources were properly cleaned up, we should be able to delete the file
        try {
          Files.delete(tempFile);
          logger.log(Level.FINE, "✓ File deleted successfully - no resource leak detected");
          // Recreate the file for cleanup
          Files.createFile(tempFile);
        } catch (Exception deleteEx) {
          logger.log(
              Level.FINE, "✗ Resource leak detected - file still locked: " + deleteEx.getMessage());
          // This indicates the RandomAccessFile was not closed
        }
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateResourceLeakOnFileSizeValidationFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing resource leak on file size validation failure ===");

    Path tempFile = Files.createTempFile("resource-leak-size", ".dat");

    try {
      // Create a valid store first, then corrupt it to have invalid record count
      try (FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        store.insertRecord("testkey".getBytes(), "testdata".getBytes());
      }

      // Truncate the file to make it too small for the claimed records
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        // Truncate to just the header size + minimal data, not enough for actual records
        raf.setLength(50); // Much smaller than needed for even 1 record
      }

      logger.log(Level.FINE, "Attempting to open store with truncated file...");

      // This should fail with IOException due to file size validation
      try (@SuppressWarnings("unused") FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        Assert.fail("Should have thrown IOException");
      } catch (IOException e) {
        logger.log(Level.FINE, "✓ Got expected file size exception: " + e.getMessage());

        // Test if resources were leaked
        try {
          Files.delete(tempFile);
          logger.log(Level.FINE, "✓ File deleted successfully - no resource leak detected");
          Files.createFile(tempFile);
        } catch (Exception deleteEx) {
          logger.log(
              Level.FINE, "✗ Resource leak detected - file still locked: " + deleteEx.getMessage());
        }
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateEarlyFieldInitializationIssue() throws Exception {
    logger.log(Level.FINE, "=== Testing early field initialization before validation ===");

    Path tempFile = Files.createTempFile("early-init", ".dat");

    try {
      // Create a valid store first, then corrupt the record data
      try (FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        store.insertRecord("testkey".getBytes(), "testdata".getBytes());
      }

      // Corrupt the record data to cause CRC validation failure during index loading
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        long crcOffset =
            FileRecordStoreBuilder.FILE_HEADERS_REGION_LENGTH
                + Short.BYTES
                + 64; // maxKeyLength used above
        raf.seek(crcOffset);
        raf.writeInt(999999); // Invalid CRC32 for key
      }

      logger.log(Level.FINE, "Attempting to open store that will fail during index loading...");

      // This should fail during loadExistingIndex due to invalid CRC
      try (@SuppressWarnings("unused") FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        Assert.fail("Should have thrown exception during index loading");
      } catch (Exception e) {
        logger.log(
            Level.FINE,
            "✓ Got expected exception during index loading: "
                + e.getClass().getSimpleName()
                + ": "
                + e.getMessage());

        // The key issue: fields like indexEntryLength, maxKeyLength were set before validation
        // But resources should still be cleaned up
        try {
          Files.delete(tempFile);
          logger.log(Level.FINE, "✓ File deleted successfully - resources cleaned up");
          Files.createFile(tempFile);
        } catch (Exception deleteEx) {
          logger.log(Level.FINE, "✗ Resource leak detected: " + deleteEx.getMessage());
        }
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
