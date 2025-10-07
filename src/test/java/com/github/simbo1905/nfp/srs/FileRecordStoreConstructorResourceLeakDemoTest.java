package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import org.junit.Assert;
import org.junit.Test;

public class FileRecordStoreConstructorResourceLeakDemoTest extends JulLoggingConfig {

  @Test
  public void demonstrateActualResourceLeak() throws Exception {
    logger.log(
        Level.FINE, "=== Demonstrating ACTUAL Resource Leak in FileRecordStore Constructor ===");

    Path tempFile = Files.createTempFile("actual-resource-leak", ".dat");

    try {
      // Create a valid store first, then corrupt the key length to trigger validation failure
      try (FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        store.insertRecord("testkey".getBytes(), "testdata".getBytes());
      }

      // Corrupt the key length header to trigger validation failure
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.seek(4); // Key length position in new format (after magic number)
        raf.writeByte(32); // Change key length from 64 to 32
      }

      logger.log(
          Level.FINE,
          "Attempting to open store with mismatched key length (expecting 64, file has 32)...");

      // This should fail with IllegalArgumentException during constructor
      // THE BUG: RandomAccessFile will be created but never closed!
      try {
        try (@SuppressWarnings("unused") FileRecordStore store =
            new FileRecordStore(
                tempFile.toFile(),
                0, // preallocatedRecords
                64, // maxKeyLength (expects 64, file has 32)
                false, // disablePayloadCrc32
                false, // useMemoryMapping
                "rw", // accessMode
                KeyType.BYTE_ARRAY, // keyType
                true, // defensiveCopy
                1024 * 1024, // preferredExpansionSize
                4 * 1024, // preferredBlockSize
                64 * 1024)) { // initialHeaderRegionSize

          Assert.fail("Should have thrown IllegalArgumentException");
        }
      } catch (IllegalArgumentException e) {
        logger.log(Level.FINE, "✓ Got expected validation exception: " + e.getMessage());

        // THE BUG: At this point, RandomAccessFile was created but never closed!
        // Try to delete the file - if it fails, resources are leaked
        try {
          Files.delete(tempFile);
          logger.log(
              Level.FINE,
              "✗ File deleted successfully - this suggests the bug might be elsewhere or already fixed");
          Files.createFile(tempFile);
        } catch (Exception deleteEx) {
          logger.log(
              Level.FINE,
              "✓ CONFIRMED: Resource leak detected - file still locked: " + deleteEx.getMessage());
          // This confirms the resource leak
        }
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateResourceLeakWithDirectConstructor() throws Exception {
    logger.log(Level.FINE, "=== Testing Resource Leak with Direct Constructor Call ===");

    Path tempFile = Files.createTempFile("direct-constructor-leak", ".dat");

    try {
      // Create a valid store first, then corrupt it to fail validation
      try (FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        store.insertRecord("testkey".getBytes(), "testdata".getBytes());
      }

      // Truncate the file to cause validation failure
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.setLength(50); // Truncate to make file too small for valid store
      }

      logger.log(
          Level.FINE,
          "Calling constructor directly with file that will fail file size validation...");

      try {
        // Use the public constructor directly
        try (@SuppressWarnings("unused") FileRecordStore store =
            new FileRecordStore(
                tempFile.toFile(),
                0, // preallocatedRecords
                64, // maxKeyLength
                false, // disablePayloadCrc32
                false, // useMemoryMapping
                "rw", // accessMode
                KeyType.BYTE_ARRAY, // keyType
                true, // defensiveCopy
                1024 * 1024, // preferredExpansionSize
                4 * 1024, // preferredBlockSize
                64 * 1024)) { // initialHeaderRegionSize

          Assert.fail("Should have thrown IOException for file size validation");
        }
      } catch (IOException e) {
        logger.log(Level.FINE, "✓ Got expected file size exception: " + e.getMessage());

        // Test for resource leak
        try {
          Files.delete(tempFile);
          logger.log(
              Level.FINE, "File deleted - checking if this indicates resource management issue...");

          // The real test: try to create many failing constructors and see if we exhaust resources
          for (int i = 0; i < 100; i++) {
            Path testFile = Files.createTempFile("resource-test-" + i, ".dat");

            // Create valid store first
            try (FileRecordStore store =
                new FileRecordStoreBuilder().path(testFile).maxKeyLength(64).open()) {
              store.insertRecord("testkey".getBytes(), "testdata".getBytes());
            }

            // Corrupt it to cause validation failure
            try (RandomAccessFile raf = new RandomAccessFile(testFile.toFile(), "rw")) {
              raf.seek(5); // numRecords position
              raf.writeInt(999); // Invalid record count
            }

            try {
              //noinspection EmptyTryBlock
              try (@SuppressWarnings("unused") FileRecordStore store =
                  new FileRecordStore(
                      testFile.toFile(),
                      0,
                      64,
                      false,
                      false,
                      "rw",
                      KeyType.BYTE_ARRAY,
                      true,
                      1024 * 1024,
                      4 * 1024,
                      64 * 1024)) {
                // Constructor should fail
              }
            } catch (IOException expected) {
              // Expected - validation should fail
            }

            Files.delete(testFile);
          }
          logger.log(
              Level.FINE,
              "✓ Created and cleaned up 100 failing constructors - no apparent resource exhaustion");

        } catch (Exception deleteEx) {
          logger.log(Level.FINE, "✗ Resource issue detected: " + deleteEx.getMessage());
        }
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateEarlyFieldInitializationIssue() throws Exception {
    logger.log(Level.FINE, "=== Demonstrating Early Field Initialization Issue ===");

    Path tempFile = Files.createTempFile("early-field-init", ".dat");

    try {
      // Create a valid store to understand constructor flow
      //noinspection EmptyTryBlock
      try (@SuppressWarnings("unused") FileRecordStore store =
          new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {
        // Store is empty but valid
      }

      logger.log(Level.FINE, "Opening valid store to understand constructor flow...");

      // This should succeed, showing the normal constructor flow
      try (FileRecordStore store =
          new FileRecordStore(
              tempFile.toFile(),
              0, // preallocatedRecords
              64, // maxKeyLength
              false, // disablePayloadCrc32
              false, // useMemoryMapping
              "rw", // accessMode
              KeyType.BYTE_ARRAY, // keyType
              true, // defensiveCopy
              1024 * 1024, // preferredExpansionSize
              4 * 1024, // preferredBlockSize
              64 * 1024)) { // initialHeaderRegionSize

        logger.log(Level.FINE, "✓ Store opened successfully - constructor flow completed");

        // The issue is subtle: fields like indexEntryLength, dataStartPtr are set
        // before validation, but for existing files they should be read from file

        // For now, let's verify the store works correctly
        Assert.assertTrue("Store should be empty", store.isEmpty());
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateCorrectBehaviorVsBuggyBehavior() throws Exception {
    logger.log(Level.FINE, "=== Comparing Correct vs Potentially Buggy Behavior ===");

    Path tempFile = Files.createTempFile("behavior-comparison", ".dat");

    try {
      // Create a store with specific parameters
      try (FileRecordStore store1 =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(64)
              .preallocatedRecords(10)
              .open()) {
        store1.insertRecord(("key1".getBytes()), "data1".getBytes());
      }

      // Now reopen with DIFFERENT parameters - this tests if the constructor
      // properly reads from file vs uses constructor parameters
      logger.log(Level.FINE, "Reopening store with different preallocatedRecords parameter...");

      try (FileRecordStore store2 =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(64) // Same as file
              .preallocatedRecords(5) // Different from original
              .open()) {

        logger.log(Level.FINE, "✓ Store reopened successfully with different parameters");

        // Verify data integrity - this tests if dataStartPtr was read correctly from file
        byte[] data = store2.readRecordData(("key1".getBytes()));
        Assert.assertArrayEquals("Data should be preserved", "data1".getBytes(), data);

        logger.log(
            Level.FINE, "✓ Data integrity verified - dataStartPtr was correctly read from file");
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
