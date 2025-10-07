package com.github.simbo1905.nfp.srs;

import java.nio.file.Files;
import java.util.logging.Level;
import org.junit.Assert;
import org.junit.Test;

/// Comprehensive test that forces header expansion with small block sizes
/// and verifies proper behavior during record movement scenarios
public class HeaderExpansionTest extends JulLoggingConfig {

  @Test
  public void testHeaderExpansionWithSmallBlocks() throws Exception {
    logger.log(Level.FINE, "=== Testing header expansion with small blocks ===");

    var tempFile = Files.createTempFile("header-expansion-", ".db");

    try {
      // Create store with very small pre-allocation to force expansion
      try (var store =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(32) // SHA256 size
              .preallocatedRecords(2) // Very small to force expansion
              .hintPreferredBlockSize(1) // 1 KiB blocks
              .hintPreferredExpandSize(1) // 1 MiB expansion
              .open()) {

        logger.log(
            Level.FINE, "Store created with 2 preallocated records, 1KB blocks, 1MB expansion");

        // Insert exactly 2 records to fill pre-allocated space
        byte[] key1 = new byte[32];
        byte[] key2 = new byte[32];
        for (int i = 0; i < 32; i++) {
          key1[i] = (byte) (i + 1);
          key2[i] = (byte) (i + 2);
        }

        logger.log(Level.FINE, "Inserting first 2 records into pre-allocated space");
        store.insertRecord(key1, "data1".getBytes());
        store.insertRecord(key2, "data2".getBytes());

        Assert.assertEquals("Should have 2 records", 2, store.size());
        logger.log(Level.FINE, "Successfully inserted 2 records, now forcing expansion...");

        // This third record should force header expansion and record movement
        byte[] key3 = new byte[32];
        for (int i = 0; i < 32; i++) {
          key3[i] = (byte) (i + 3);
        }

        logger.log(Level.FINE, "Inserting third record - this should trigger header expansion");
        store.insertRecord(key3, "data3".getBytes());

        Assert.assertEquals("Should have 3 records after expansion", 3, store.size());
        logger.log(Level.FINE, "Successfully expanded and inserted third record");

        // Verify all data is intact after expansion
        Assert.assertArrayEquals(
            "Key1 data should be intact", "data1".getBytes(), store.readRecordData(key1));
        Assert.assertArrayEquals(
            "Key2 data should be intact", "data2".getBytes(), store.readRecordData(key2));
        Assert.assertArrayEquals(
            "Key3 data should be intact", "data3".getBytes(), store.readRecordData(key3));

        logger.log(Level.FINE, "All data verified intact after header expansion");

        // Force more expansions with additional records
        for (int i = 4; i <= 10; i++) {
          byte[] key = new byte[32];
          for (int j = 0; j < 32; j++) {
            key[j] = (byte) (j + i);
          }
          String data = "data" + i;
          logger.log(Level.FINE, "Inserting record " + i + " to force continued expansion");
          store.insertRecord(key, data.getBytes());
        }

        Assert.assertEquals("Should have 10 records total", 10, store.size());
        logger.log(Level.FINE, "Successfully inserted 10 records with multiple expansions");

        // Verify a few more records
        byte[] key10 = new byte[32];
        for (int j = 0; j < 32; j++) {
          key10[j] = (byte) (j + 10);
        }
        Assert.assertArrayEquals(
            "Key10 data should be intact", "data10".getBytes(), store.readRecordData(key10));

        logger.log(Level.FINE, "=== Header expansion test completed successfully ===");
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testRecordMovementDuringExpansion() throws Exception {
    logger.log(Level.FINE, "=== Testing record movement during header expansion ===");

    var tempFile = Files.createTempFile("record-movement-", ".db");

    try {
      // Create store with extremely small settings to force maximum movement
      try (var store =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(64) // SHA512 size
              .preallocatedRecords(1) // Only 1 pre-allocated record
              .hintPreferredBlockSize(1) // 1 KiB blocks
              .hintPreferredExpandSize(1) // 1 MiB expansion
              .open()) {

        logger.log(
            Level.FINE, "Store created with 1 preallocated record, 1KB blocks, 1MB expansion");

        // Insert first record
        byte[] key1 = new byte[64];
        for (int i = 0; i < 64; i++) {
          key1[i] = (byte) (i + 1);
        }

        logger.log(Level.FINE, "Inserting first record");
        store.insertRecord(key1, "first record data".getBytes());

        // Insert second record - this should force the first record to move
        byte[] key2 = new byte[64];
        for (int i = 0; i < 64; i++) {
          key2[i] = (byte) (i + 2);
        }

        logger.log(Level.FINE, "Inserting second record - should force first record to move");
        store.insertRecord(key2, "second record data".getBytes());

        Assert.assertEquals("Should have 2 records", 2, store.size());

        // Verify both records are intact after movement
        Assert.assertArrayEquals(
            "Key1 data should be intact after movement",
            "first record data".getBytes(),
            store.readRecordData(key1));
        Assert.assertArrayEquals(
            "Key2 data should be intact",
            "second record data".getBytes(),
            store.readRecordData(key2));

        logger.log(Level.FINE, "Both records intact after movement");

        // Insert a large record to test different movement patterns
        byte[] largeData = new byte[1024]; // 1KB of data
        for (int i = 0; i < largeData.length; i++) {
          largeData[i] = (byte) (i % 256);
        }

        byte[] key3 = new byte[64];
        for (int i = 0; i < 64; i++) {
          key3[i] = (byte) (i + 3);
        }

        logger.log(
            Level.FINE, "Inserting large record (1KB data) - should trigger complex movement");
        store.insertRecord(key3, largeData);

        Assert.assertEquals("Should have 3 records", 3, store.size());

        // Verify all records including the large one
        Assert.assertArrayEquals(
            "Key1 data should still be intact",
            "first record data".getBytes(),
            store.readRecordData(key1));
        Assert.assertArrayEquals(
            "Key2 data should still be intact",
            "second record data".getBytes(),
            store.readRecordData(key2));
        Assert.assertArrayEquals(
            "Key3 large data should be intact", largeData, store.readRecordData(key3));

        logger.log(Level.FINE, "All records intact after complex movement patterns");

        logger.log(Level.FINE, "=== Record movement test completed successfully ===");
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testHeaderExpansionWithUpdates() throws Exception {
    logger.log(Level.FINE, "=== Testing header expansion with updates ===");

    var tempFile = Files.createTempFile("header-expansion-updates-", ".db");

    try {
      try (var store =
          new FileRecordStoreBuilder()
              .path(tempFile)
              .maxKeyLength(32)
              .preallocatedRecords(3)
              .hintPreferredBlockSize(1) // 1 KiB blocks
              .hintPreferredExpandSize(1) // 1 MiB expansion
              .open()) {

        logger.log(
            Level.FINE,
            "Store created - will fill pre-allocated space then force expansion with updates");

        // Insert records to fill pre-allocated space
        for (int i = 1; i <= 3; i++) {
          byte[] key = new byte[32];
          for (int j = 0; j < 32; j++) {
            key[j] = (byte) (j + i);
          }
          String data = "initial-data-" + i;
          logger.log(Level.FINE, "Inserting record " + i);
          store.insertRecord(key, data.getBytes());
        }

        Assert.assertEquals("Should have 3 records", 3, store.size());

        // Now update with larger data to force expansion
        byte[] largeUpdateData = new byte[512]; // 512 bytes - larger than original
        for (int i = 0; i < largeUpdateData.length; i++) {
          largeUpdateData[i] = (byte) (i % 256);
        }

        byte[] key1 = new byte[32];
        for (int j = 0; j < 32; j++) {
          key1[j] = (byte) (j + 1);
        }

        logger.log(
            Level.FINE,
            "Updating record 1 with larger data (512 bytes) - should trigger expansion");
        store.updateRecord(key1, largeUpdateData);

        // Verify the update worked
        Assert.assertArrayEquals(
            "Large update should be intact", largeUpdateData, store.readRecordData(key1));

        logger.log(
            Level.FINE,
            "Large update successful, now inserting new record to force further expansion");

        // Insert a new record to force further expansion
        byte[] key4 = new byte[32];
        for (int j = 0; j < 32; j++) {
          key4[j] = (byte) (j + 4);
        }
        store.insertRecord(key4, "fourth-record-data".getBytes());

        Assert.assertEquals("Should have 4 records", 4, store.size());

        // Verify all records are intact
        Assert.assertArrayEquals(
            "Large update should still be intact", largeUpdateData, store.readRecordData(key1));

        byte[] key2 = new byte[32];
        for (int j = 0; j < 32; j++) {
          key2[j] = (byte) (j + 2);
        }
        Assert.assertArrayEquals(
            "Record 2 should be intact", "initial-data-2".getBytes(), store.readRecordData(key2));

        Assert.assertArrayEquals(
            "Record 4 should be intact",
            "fourth-record-data".getBytes(),
            store.readRecordData(key4));

        logger.log(Level.FINE, "=== Header expansion with updates test completed successfully ===");
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
