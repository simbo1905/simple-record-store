package com.github.simbo1905.nfp.srs;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import org.junit.Assert;
import org.junit.Test;

public class FileRecordStoreConstructorExistingFileBugTest extends JulLoggingConfig {

  @Test
  public void demonstrateExistingFileCompatibilityIssue() throws Exception {
    logger.log(Level.FINE, "=== Testing FileRecordStore existing file compatibility ===");

    Path tempFile = Files.createTempFile("existing-file-compat", ".dat");

    try {
      // Create a store with maxKeyLength = 64
      try (FileRecordStore store1 =
          new FileRecordStore.Builder().path(tempFile).maxKeyLength(64).open()) {
        store1.insertRecord(("testkey".getBytes()), "testdata".getBytes());
        logger.log(Level.FINE, "Created store with maxKeyLength=64, inserted test record");
      }

      // Try to reopen with same parameters - should work
      logger.log(Level.FINE, "Attempting to reopen existing store...");
      try (FileRecordStore store2 =
          new FileRecordStore.Builder().path(tempFile).maxKeyLength(64).open()) {
        logger.log(Level.FINE, "✓ Successfully reopened existing store");
        byte[] data = store2.readRecordData(("testkey".getBytes()));
        Assert.assertArrayEquals("Data should be preserved", "testdata".getBytes(), data);
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateResourceLeakOnValidationFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing resource leak on header validation failure ===");

    Path tempFile = Files.createTempFile("resource-leak", ".dat");

    try {
      // Create a valid store
      try (FileRecordStore store1 =
          new FileRecordStore.Builder().path(tempFile).maxKeyLength(64).open()) {
        store1.insertRecord(("testkey".getBytes()), "testdata".getBytes());
      }

      // Corrupt the key length header to trigger validation failure
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.seek(0);
        raf.writeByte(128); // Write invalid key length (larger than theoretical max)
      }

      // Try to open with different maxKeyLength - should fail but not leak resources
      logger.log(Level.FINE, "Attempting to open store with corrupted header...");
      try (FileRecordStore store2 =
          new FileRecordStore.Builder().path(tempFile).maxKeyLength(64).open()) {
        Assert.fail("Should have thrown exception due to corrupted header");
      } catch (IllegalArgumentException e) {
        logger.log(Level.FINE, "✓ Got expected validation exception: " + e.getMessage());
        // Check if resources were properly cleaned up
        // In a proper implementation, we should be able to delete the file
        // indicating no open file handles
        try {
          Files.delete(tempFile);
          logger.log(Level.FINE, "✓ File deleted successfully - no resource leak");
          // Recreate file for cleanup
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
  public void demonstrateMemIndexSizingWithUnvalidatedData() throws Exception {
    logger.log(Level.FINE, "=== Testing memIndex sizing with potentially unvalidated data ===");

    Path tempFile = Files.createTempFile("memindex-sizing", ".dat");

    try {
      // Create a minimal valid file
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.setLength(13); // Just header size
        raf.writeByte(64); // key length
        raf.writeInt(1000000); // Write a huge numRecords value (unvalidated)
        raf.writeLong(13); // data start
      }

      // Try to open - this could cause issues if numRecords isn't validated
      logger.log(Level.FINE, "Attempting to open store with huge numRecords value...");
      try (FileRecordStore store =
          new FileRecordStore.Builder().path(tempFile).maxKeyLength(64).open()) {
        logger.log(Level.FINE, "Store opened successfully");
        // The issue would be if memIndex was sized with the huge numRecords value
        // causing memory issues
      } catch (Exception e) {
        logger.log(
            Level.FINE, "Got exception (could be good if it prevents OOM): " + e.getMessage());
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateDataStartPtrInitializationBug() throws Exception {
    logger.log(Level.FINE, "=== Testing dataStartPtr initialization for existing files ===");

    Path tempFile = Files.createTempFile("datastart-ptr", ".dat");

    try {
      // Create a store to establish proper dataStartPtr
      try (FileRecordStore store1 =
          new FileRecordStore.Builder()
              .path(tempFile)
              .maxKeyLength(64)
              .preallocatedRecords(10)
              .open()) {
        store1.insertRecord(("testkey".getBytes()), "testdata".getBytes());
        logger.log(Level.FINE, "Created store with preallocatedRecords=10");
      }

      // Reopen with different preallocatedRecords - should use file's dataStartPtr, not
      // preallocated value
      logger.log(Level.FINE, "Reopening store with different preallocatedRecords...");
      try (FileRecordStore store2 =
          new FileRecordStore.Builder()
              .path(tempFile)
              .maxKeyLength(64)
              .preallocatedRecords(5) // Different value
              .open()) {
        logger.log(Level.FINE, "✓ Successfully reopened store with different preallocatedRecords");

        // Verify data integrity
        byte[] data = store2.readRecordData(("testkey".getBytes()));
        Assert.assertArrayEquals("Data should be preserved", "testdata".getBytes(), data);
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
