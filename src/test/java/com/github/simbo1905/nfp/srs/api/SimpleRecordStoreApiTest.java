package com.github.simbo1905.nfp.srs.api;

import com.github.simbo1905.nfp.srs.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleRecordStoreApiTest extends JulLoggingConfig {
  String fileName;
  FileRecordStore recordsFile = null;
  int initialSize;

  private static final Logger LOGGER = Logger.getLogger(SimpleRecordStoreApiTest.class.getName());

  @Before
  public void setup() throws Exception {
    LOGGER.setLevel(Level.ALL);
    Path tempFile = Files.createTempFile("junit-records-", ".db");
    tempFile.toFile().deleteOnExit();
    fileName = tempFile.toString();
    initialSize = 0;
    cleanupFiles();
  }

  @After
  public void cleanup() {
    cleanupFiles();
  }

  private void cleanupFiles() {
    if (fileName != null) {
      File db = new File(fileName);
      if (db.exists()) {
        if (!db.delete()) {
          throw new IllegalStateException("Failed to delete " + db);
        }
      }
    }
  }

  @Test
  public void testInsertOneRecordMapEntry() throws Exception {
    // given
    recordsFile =
        new FileRecordStoreBuilder()
            .path(Paths.get(fileName))
            .preallocatedRecords(initialSize)
            .allowZeroPreallocation()
            .maxKeyLength(64)
            .open();
    String uuid = UUIDGenerator.generateUUID().toString();
    final var key = uuid.getBytes();

    // when
    this.recordsFile.insertRecord(key, uuid.getBytes());
    if (recordsFile.recordExists(uuid.getBytes())) {
      this.recordsFile.deleteRecord(uuid.getBytes());
    }

    Assert.assertTrue(this.recordsFile.isEmpty());
    Assert.assertFalse(this.recordsFile.recordExists(uuid.getBytes()));

    this.recordsFile.insertRecord(uuid.getBytes(), uuid.getBytes());

    Assert.assertFalse(this.recordsFile.isEmpty());
    Assert.assertTrue(this.recordsFile.recordExists(uuid.getBytes()));

    this.recordsFile.fsync();

    final var data = this.recordsFile.readRecordData(uuid.getBytes());
    Assert.assertEquals(new String(data), uuid);

    this.recordsFile.updateRecord(uuid.getBytes(), "updated".getBytes());

    this.recordsFile.fsync();

    this.recordsFile.close();

    // then
    recordsFile =
        new FileRecordStoreBuilder()
            .path(Paths.get(fileName))
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
            .maxKeyLength(64)
            .open();
    final var updated = this.recordsFile.readRecordData(uuid.getBytes());
    Assert.assertEquals("updated", new String(updated));
    Assert.assertEquals(1, recordsFile.size());

    Assert.assertArrayEquals(recordsFile.keysBytes().iterator().next(), key);
  }

  @Test
  public void testStoreCreationAndConfiguration() throws Exception {
    LOGGER.info("=== Testing Store Creation & Configuration ===");

    // Test 1: Default byte array keys
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-default-", ".db").maxKeyLength(64).open()) {
      Assert.assertNotNull("Store should be created", store);
      Assert.assertTrue("Store should be empty initially", store.isEmpty());
      Assert.assertEquals("Should have 0 records", 0, store.size());

      byte[] key = "testkey".getBytes();
      byte[] value = "testvalue".getBytes();
      store.insertRecord(key, value);
      Assert.assertTrue("Record should exist", store.recordExists(key));
    }

    // Test 2: UUID keys mode
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-uuid-", ".db").uuidKeys().maxKeyLength(16).open()) {
      Assert.assertNotNull("UUID store should be created", store);

      UUID uuidKey = UUID.randomUUID();
      byte[] value = "uuid-value".getBytes();
      store.insertRecord(uuidKey, value);
      Assert.assertTrue("UUID record should exist", store.recordExists(uuidKey));

      // Test mixed operations - UUID mode accepts byte arrays that are 16 bytes
      byte[] uuidBytes = new byte[16];
      new Random().nextBytes(uuidBytes);
      store.insertRecord(uuidBytes, "16-byte-value".getBytes());
      Assert.assertTrue("16-byte key should work in UUID mode", store.recordExists(uuidBytes));
    }

    // Test 3: Read-only mode
    Path readOnlyPath = Files.createTempFile("test-readonly-", ".db");
    readOnlyPath.toFile().deleteOnExit();
    try (FileRecordStore writeStore = new FileRecordStoreBuilder().path(readOnlyPath).maxKeyLength(64).open()) {
      writeStore.insertRecord("key1".getBytes(), "value1".getBytes());
    }

    try (FileRecordStore readStore =
        new FileRecordStoreBuilder()
            .path(readOnlyPath)
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
            .maxKeyLength(64)
            .open()) {
      Assert.assertTrue(
          "Should be able to read in read-only mode", readStore.recordExists("key1".getBytes()));

      try {
        readStore.insertRecord("key2".getBytes(), "value2".getBytes());
        Assert.fail("Should throw UnsupportedOperationException in read-only mode");
      } catch (UnsupportedOperationException e) {
        // Expected
      }
    }

    // Test 4: With/without CRC32 validation
    try (FileRecordStore storeWithCrc =
        new FileRecordStoreBuilder()
            .tempFile("test-crc-", ".db")
            .disablePayloadCrc32(false)
            .maxKeyLength(64)
            .open()) {
      storeWithCrc.insertRecord("crc-key".getBytes(), "crc-value".getBytes());
      Assert.assertArrayEquals(
          "Should read correct value with CRC",
          "crc-value".getBytes(),
          storeWithCrc.readRecordData("crc-key".getBytes()));
    }

    try (FileRecordStore storeNoCrc =
        new FileRecordStoreBuilder()
            .tempFile("test-no-crc-", ".db")
            .disablePayloadCrc32(true)
            .maxKeyLength(64)
            .open()) {
      storeNoCrc.insertRecord("no-crc-key".getBytes(), "no-crc-value".getBytes());
      Assert.assertArrayEquals(
          "Should read correct value without CRC",
          "no-crc-value".getBytes(),
          storeNoCrc.readRecordData("no-crc-key".getBytes()));
    }

    // Test 5: Memory-mapped vs direct I/O
    try (FileRecordStore mmapStore =
        new FileRecordStoreBuilder().tempFile("test-mmap-", ".db").useMemoryMapping(true).maxKeyLength(64).open()) {
      mmapStore.insertRecord("mmap-key".getBytes(), "mmap-value".getBytes());
      Assert.assertTrue(
          "Memory-mapped store should work", mmapStore.recordExists("mmap-key".getBytes()));
    }

    try (FileRecordStore directStore =
        new FileRecordStoreBuilder()
            .tempFile("test-direct-", ".db")
            .useMemoryMapping(false)
            .maxKeyLength(64)
            .open()) {
      directStore.insertRecord("direct-key".getBytes(), "direct-value".getBytes());
      Assert.assertTrue(
          "Direct I/O store should work", directStore.recordExists("direct-key".getBytes()));
    }

    // Test 6: Different preallocated record counts
    try (FileRecordStore preallocStore =
        new FileRecordStoreBuilder()
            .tempFile("test-prealloc-", ".db")
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .open()) {
      // Should be able to insert up to 100 records without file expansion
      for (int i = 0; i < 50; i++) {
        String key = "prealloc" + i;
        String value = "value" + i;
        preallocStore.insertRecord(key.getBytes(), value.getBytes());
      }
      Assert.assertEquals("Should have 50 records", 50, preallocStore.size());
    }

    // Test 7: Various max key lengths
    try (FileRecordStore shortKeyStore =
        new FileRecordStoreBuilder().tempFile("test-short-", ".db").maxKeyLength(16).open()) {
      Assert.assertEquals("Max key length should be 16", 16, shortKeyStore.maxKeyLength);

      byte[] shortKey = "1234567890123456".getBytes(); // exactly 16 bytes
      shortKeyStore.insertRecord(shortKey, "short-value".getBytes());
      Assert.assertTrue("Should accept 16-byte key", shortKeyStore.recordExists(shortKey));
    }

    // Test 8: Temporary file stores
    try (FileRecordStore tempStore =
        new FileRecordStoreBuilder().tempFile("test-temp-", ".temp").maxKeyLength(64).open()) {
      Assert.assertNotNull("Temporary store should be created", tempStore);
      tempStore.insertRecord("temp-key".getBytes(), "temp-value".getBytes());
      Assert.assertTrue(
          "Temporary store should work", tempStore.recordExists("temp-key".getBytes()));
    }

    LOGGER.info("=== Store Creation & Configuration Tests Completed ===");
  }

  @Test
  public void testBasicCrudOperations() throws Exception {
    LOGGER.info("=== Testing Basic CRUD Operations ===");

    // Test with byte array keys
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-crud-byte-", ".db").maxKeyLength(64).open()) {

      // Insert operations
      byte[] key1 = "crud-key-1".getBytes();
      byte[] value1 = "crud-value-1".getBytes();
      store.insertRecord(key1, value1);
      Assert.assertTrue("Record should exist after insert", store.recordExists(key1));

      byte[] key2 = "crud-key-2".getBytes();
      byte[] value2 = "crud-value-2".getBytes();
      store.insertRecord(key2, value2);
      Assert.assertEquals("Should have 2 records", 2, store.size());

      // Read operations
      byte[] read1 = store.readRecordData(key1);
      Assert.assertArrayEquals("Should read correct value 1", value1, read1);

      byte[] read2 = store.readRecordData(key2);
      Assert.assertArrayEquals("Should read correct value 2", value2, read2);

      // Update operations
      byte[] newValue1 = "updated-value-1".getBytes();
      store.updateRecord(key1, newValue1);
      byte[] updated1 = store.readRecordData(key1);
      Assert.assertArrayEquals("Should read updated value", newValue1, updated1);

      // Delete operations
      store.deleteRecord(key2);
      Assert.assertFalse("Record should not exist after delete", store.recordExists(key2));
      Assert.assertEquals("Should have 1 record after delete", 1, store.size());

      // Verify key1 still exists
      Assert.assertTrue("Key1 should still exist", store.recordExists(key1));
    }

    // Test with UUID keys
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-crud-uuid-", ".db").uuidKeys().maxKeyLength(16).open()) {

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      byte[] value1 = "uuid-value-1".getBytes();
      byte[] value2 = "uuid-value-2".getBytes();

      // Insert operations
      store.insertRecord(uuid1, value1);
      store.insertRecord(uuid2, value2);
      Assert.assertEquals("Should have 2 UUID records", 2, store.size());

      // Read operations
      byte[] read1 = store.readRecordData(uuid1);
      Assert.assertArrayEquals("Should read correct UUID value 1", value1, read1);

      byte[] read2 = store.readRecordData(uuid2);
      Assert.assertArrayEquals("Should read correct UUID value 2", value2, read2);

      // Update operations
      byte[] newValue1 = "updated-uuid-value-1".getBytes();
      store.updateRecord(uuid1, newValue1);
      byte[] updated1 = store.readRecordData(uuid1);
      Assert.assertArrayEquals("Should read updated UUID value", newValue1, updated1);

      // Delete operations
      store.deleteRecord(uuid2);
      Assert.assertFalse("UUID record should not exist after delete", store.recordExists(uuid2));
      Assert.assertEquals("Should have 1 UUID record after delete", 1, store.size());
    }

    LOGGER.info("=== Basic CRUD Operations Tests Completed ===");
  }

  @Test
  public void testQueryOperations() throws Exception {
    LOGGER.info("=== Testing Query Operations ===");

    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-query-", ".db").maxKeyLength(64).open()) {

      // Test isEmpty()
      Assert.assertTrue("Store should be empty initially", store.isEmpty());

      store.insertRecord("key1".getBytes(), "value1".getBytes());
      Assert.assertFalse("Store should not be empty after insert", store.isEmpty());

      // Test size()
      Assert.assertEquals("size() should return 1", 1, store.size());

      store.insertRecord("key2".getBytes(), "value2".getBytes());
      Assert.assertEquals("size() should return 2", 2, store.size());

      // Test recordExists()
      Assert.assertTrue("key1 should exist", store.recordExists("key1".getBytes()));
      Assert.assertTrue("key2 should exist", store.recordExists("key2".getBytes()));
      Assert.assertFalse(
          "nonexistent should not exist", store.recordExists("nonexistent".getBytes()));

      // Test keysBytes() iteration
      int keyCount = 0;
      for (byte[] key : store.keysBytes()) {
        keyCount++;
        Assert.assertNotNull("Key should not be null", key);
        Assert.assertTrue(
            "Key should be either key1 or key2",
            java.util.Arrays.equals(key, "key1".getBytes())
                || java.util.Arrays.equals(key, "key2".getBytes()));
      }
      Assert.assertEquals("Should iterate over 2 keys", 2, keyCount);
    }

    // Test uuidKeys() iteration
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-query-uuid-", ".db").uuidKeys().maxKeyLength(16).open()) {

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      store.insertRecord(uuid1, "uuid-value-1".getBytes());
      store.insertRecord(uuid2, "uuid-value-2".getBytes());

      int uuidCount = 0;
      for (UUID uuid : store.uuidKeys()) {
        uuidCount++;
        Assert.assertNotNull("UUID should not be null", uuid);
        Assert.assertTrue(
            "UUID should be either uuid1 or uuid2", uuid.equals(uuid1) || uuid.equals(uuid2));
      }
      Assert.assertEquals("Should iterate over 2 UUIDs", 2, uuidCount);
    }

    LOGGER.info("=== Query Operations Tests Completed ===");
  }

  @Test
  public void testFileOperations() throws Exception {
    LOGGER.info("=== Testing File Operations ===");

    Path persistencePath = Files.createTempFile("test-persistence-", ".db");
    persistencePath.toFile().deleteOnExit();

    // Create and populate store
    try (FileRecordStore store = new FileRecordStoreBuilder().path(persistencePath).maxKeyLength(64).open()) {

      store.insertRecord("persist-key-1".getBytes(), "persist-value-1".getBytes());
      store.insertRecord("persist-key-2".getBytes(), "persist-value-2".getBytes());

      // Test fsync()
      store.fsync();
      LOGGER.info("fsync() completed successfully");

      // Store should still be operational after fsync
      Assert.assertTrue(
          "Records should still exist after fsync", store.recordExists("persist-key-1".getBytes()));
    }

    // Reopen store to verify persistence
    try (FileRecordStore reopenedStore =
        new FileRecordStoreBuilder().path(persistencePath).maxKeyLength(64).open()) {

      Assert.assertEquals("Should have 2 records after reopen", 2, reopenedStore.size());
      Assert.assertTrue(
          "persist-key-1 should exist after reopen",
          reopenedStore.recordExists("persist-key-1".getBytes()));
      Assert.assertTrue(
          "persist-key-2 should exist after reopen",
          reopenedStore.recordExists("persist-key-2".getBytes()));

      byte[] read1 = reopenedStore.readRecordData("persist-key-1".getBytes());
      Assert.assertArrayEquals(
          "Should read correct persisted value 1", "persist-value-1".getBytes(), read1);

      byte[] read2 = reopenedStore.readRecordData("persist-key-2".getBytes());
      Assert.assertArrayEquals(
          "Should read correct persisted value 2", "persist-value-2".getBytes(), read2);
    }

    LOGGER.info("=== File Operations Tests Completed ===");
  }

  @Test
  public void testEdgeCasesAndErrorHandling() throws Exception {
    LOGGER.info("=== Testing Edge Cases & Error Handling ===");

    // Test with keys at maximum allowed length
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-max-key-", ".db").maxKeyLength(248).open()) {

      byte[] maxKey = new byte[248];
      for (int i = 0; i < 248; i++) {
        maxKey[i] = (byte) (i % 256);
      }
      byte[] value = "max-key-value".getBytes();

      store.insertRecord(maxKey, value);
      Assert.assertTrue("Max length key should work", store.recordExists(maxKey));

      byte[] readValue = store.readRecordData(maxKey);
      Assert.assertArrayEquals("Should read correct max key value", value, readValue);
    }

    // Test with empty values and large values
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-values-", ".db").maxKeyLength(64).open()) {

      // Empty value
      byte[] emptyValue = new byte[0];
      store.insertRecord("empty-key".getBytes(), emptyValue);
      byte[] readEmpty = store.readRecordData("empty-key".getBytes());
      Assert.assertArrayEquals("Empty value should round-trip", emptyValue, readEmpty);

      // Large value
      byte[] largeValue = new byte[1024];
      new Random().nextBytes(largeValue);
      store.insertRecord("large-key".getBytes(), largeValue);
      byte[] readLarge = store.readRecordData("large-key".getBytes());
      Assert.assertArrayEquals("Large value should round-trip", largeValue, readLarge);
    }

    // Test operations on closed stores
    FileRecordStore closedStore =
        new FileRecordStoreBuilder().tempFile("test-closed-", ".db").maxKeyLength(64).open();
    closedStore.insertRecord("test-key".getBytes(), "test-value".getBytes());
    closedStore.close();

    try {
      closedStore.insertRecord("another-key".getBytes(), "another-value".getBytes());
      Assert.fail("Should throw IllegalStateException on closed store");
    } catch (IllegalStateException e) {
      // Expected
      LOGGER.info("Correctly caught IllegalStateException for closed store: " + e.getMessage());
    }

    try {
      closedStore.readRecordData("test-key".getBytes());
      Assert.fail("Should throw IllegalStateException on closed store read");
    } catch (IllegalStateException e) {
      // Expected
    }

    // Test modifying read-only stores
    Path readOnlyPath = Files.createTempFile("test-readonly-error-", ".db");
    readOnlyPath.toFile().deleteOnExit();
    try (FileRecordStore writeStore = new FileRecordStoreBuilder().path(readOnlyPath).maxKeyLength(64).open()) {
      writeStore.insertRecord("readonly-test-key".getBytes(), "readonly-test-value".getBytes());
    }

    try (FileRecordStore readStore =
        new FileRecordStoreBuilder()
            .path(readOnlyPath)
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
            .maxKeyLength(64)
            .open()) {

      try {
        readStore.insertRecord("new-key".getBytes(), "new-value".getBytes());
        Assert.fail("Should throw UnsupportedOperationException in read-only mode");
      } catch (UnsupportedOperationException e) {
        // Expected
        LOGGER.info(
            "Correctly caught UnsupportedOperationException for read-only: " + e.getMessage());
      }

      // Create a new read-only store for each operation to avoid state issues
      try (FileRecordStore readStore2 =
          new FileRecordStoreBuilder()
              .path(readOnlyPath)
              .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
              .maxKeyLength(64)
              .open()) {
        try {
          readStore2.updateRecord("readonly-test-key".getBytes(), "updated-value".getBytes());
          Assert.fail("Should throw UnsupportedOperationException for update in read-only mode");
        } catch (UnsupportedOperationException e) {
          // Expected
        }
      }

      try (FileRecordStore readStore3 =
          new FileRecordStoreBuilder()
              .path(readOnlyPath)
              .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
              .maxKeyLength(64)
              .open()) {
        try {
          readStore3.deleteRecord("readonly-test-key".getBytes());
          Assert.fail("Should throw UnsupportedOperationException for delete in read-only mode");
        } catch (UnsupportedOperationException e) {
          // Expected
        }
      }
    }

    // Test inserting duplicate keys
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-duplicate-", ".db").maxKeyLength(64).open()) {

      byte[] duplicateKey = "duplicate-test-key".getBytes();
      store.insertRecord(duplicateKey, "first-value".getBytes());

      try {
        store.insertRecord(duplicateKey, "second-value".getBytes());
        Assert.fail("Should throw IllegalArgumentException for duplicate key");
      } catch (IllegalArgumentException e) {
        // Expected
        LOGGER.info(
            "Correctly caught IllegalArgumentException for duplicate key: " + e.getMessage());
      }
    }

    // Test reading/updating/deleting non-existent keys
    // Use separate stores for each operation to avoid state issues

    // Test non-existent key read
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-nonexistent-read-", ".db").maxKeyLength(64).open()) {

      byte[] nonExistentKey = "nonexistent-key".getBytes();

      try {
        store.readRecordData(nonExistentKey);
        Assert.fail("Should throw IllegalArgumentException for non-existent key read");
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }

    // Test non-existent key update
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-nonexistent-update-", ".db").maxKeyLength(64).open()) {

      byte[] nonExistentKey = "nonexistent-key".getBytes();

      try {
        store.updateRecord(nonExistentKey, "new-value".getBytes());
        Assert.fail("Should throw IllegalArgumentException for non-existent key update");
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }

    // Test non-existent key delete
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-nonexistent-delete-", ".db").maxKeyLength(64).open()) {

      byte[] nonExistentKey = "nonexistent-key".getBytes();

      try {
        store.deleteRecord(nonExistentKey);
        Assert.fail("Should throw IllegalArgumentException for non-existent key delete");
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }

    // Test UUID operations on byte array mode stores
    try (FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("test-uuid-wrong-mode-", ".db").maxKeyLength(64).open()) {

      UUID uuid = UUID.randomUUID();

      try {
        store.insertRecord(uuid, "value".getBytes());
        Assert.fail("Should throw UnsupportedOperationException for UUID in byte array mode");
      } catch (UnsupportedOperationException e) {
        // Expected
        LOGGER.info(
            "Correctly caught UnsupportedOperationException for UUID in wrong mode: "
                + e.getMessage());
      }

      try {
        store.readRecordData(uuid);
        Assert.fail("Should throw UnsupportedOperationException for UUID read in byte array mode");
      } catch (UnsupportedOperationException e) {
        // Expected
      }
    }

    LOGGER.info("=== Edge Cases & Error Handling Tests Completed ===");
  }

  @Test
  public void testStateManagement() throws Exception {
    LOGGER.info("=== Testing State Management ===");

    FileRecordStore store = new FileRecordStoreBuilder().tempFile("test-state-", ".db").maxKeyLength(64).open();

    try {
      // Test initial state
      Assert.assertFalse("Store should not be closed initially", store.isClosed());

      // Test operations work in open state
      store.insertRecord("state-key".getBytes(), "state-value".getBytes());
      Assert.assertTrue(
          "Record should exist in open state", store.recordExists("state-key".getBytes()));

      // Test state after close
      store.close();
      Assert.assertTrue("Store should be closed after close()", store.isClosed());

      // Test behavior after close (should throw exceptions)
      try {
        store.insertRecord("after-close".getBytes(), "should-fail".getBytes());
        Assert.fail("Should throw IllegalStateException after close");
      } catch (IllegalStateException e) {
        // Expected
        LOGGER.info("Correctly caught IllegalStateException after close: " + e.getMessage());
      }

    } finally {
      // Ensure cleanup
      if (!store.isClosed()) {
        store.close();
      }
    }

    LOGGER.info("=== State Management Tests Completed ===");
  }

  @Test
  public void testDefensiveCopyVsZeroCopy() throws Exception {
    LOGGER.info("=== Testing Defensive Copy vs Zero Copy ===");

    // Test with defensive copying (default)
    try (FileRecordStore defensiveStore =
        new FileRecordStoreBuilder()
            .tempFile("test-defensive-", ".db")
            .defensiveCopy(true)
            .maxKeyLength(64)
            .open()) {

      byte[] mutableKey = "mutable-key".getBytes();
      byte[] mutableValue = "mutable-value".getBytes();

      defensiveStore.insertRecord(mutableKey, mutableValue);

      // Modify the original arrays (should not affect stored data due to defensive copy)
      mutableKey[0] = 'X';
      mutableValue[0] = 'X';

      byte[] readValue = defensiveStore.readRecordData("mutable-key".getBytes());
      Assert.assertArrayEquals(
          "Defensive copy should protect against external mutation",
          "mutable-value".getBytes(),
          readValue);
    }

    // Test with zero copy
    try (FileRecordStore zeroCopyStore =
        new FileRecordStoreBuilder()
            .tempFile("test-zero-copy-", ".db")
            .defensiveCopy(false)
            .maxKeyLength(64)
            .open()) {

      byte[] key = "zero-copy-key".getBytes();
      byte[] value = "zero-copy-value".getBytes();

      zeroCopyStore.insertRecord(key, value);
      byte[] readValue = zeroCopyStore.readRecordData(key);
      Assert.assertArrayEquals("Zero copy should work correctly", value, readValue);
    }

    LOGGER.info("=== Defensive Copy vs Zero Copy Tests Completed ===");
  }

  @Test
  public void testComprehensiveApiCoverage() throws Exception {
    LOGGER.info("=== Testing Comprehensive API Coverage ===");

    // This test exercises every public method to ensure complete coverage
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .tempFile("test-comprehensive-", ".db")
            .preallocatedRecords(10)
            .maxKeyLength(32)
            .disablePayloadCrc32(false)
            .useMemoryMapping(false)
            .open()) {

      // Test all insert methods
      store.insertRecord("byte-key-1".getBytes(), "byte-value-1".getBytes());

      // Test all read methods
      byte[] read1 = store.readRecordData("byte-key-1".getBytes());
      Assert.assertArrayEquals("byte-value-1".getBytes(), read1);

      // Test all update methods
      store.updateRecord("byte-key-1".getBytes(), "updated-byte-value-1".getBytes());
      byte[] updated1 = store.readRecordData("byte-key-1".getBytes());
      Assert.assertArrayEquals("updated-byte-value-1".getBytes(), updated1);

      // Test all delete methods
      store.insertRecord("byte-key-to-delete".getBytes(), "delete-me".getBytes());
      store.deleteRecord("byte-key-to-delete".getBytes());
      Assert.assertFalse(
          "Deleted record should not exist", store.recordExists("byte-key-to-delete".getBytes()));

      // Test all query methods
      Assert.assertFalse("Store should not be empty", store.isEmpty());
      Assert.assertEquals("Should have correct size", 1, store.size());
      Assert.assertEquals("size should be 1", 1, store.size());
      Assert.assertTrue("Record should exist", store.recordExists("byte-key-1".getBytes()));
      Assert.assertFalse(
          "Non-existent record should not exist", store.recordExists("nonexistent".getBytes()));

      // Test key iteration
      int keyCount = 0;
      for (byte[] key : store.keysBytes()) {
        keyCount++;
        Assert.assertArrayEquals("Key should be byte-key-1", key, "byte-key-1".getBytes());
      }
      Assert.assertEquals("Should have 1 key", 1, keyCount);

      // Test file operations
      store.fsync();
      Assert.assertFalse("Store should not be closed", store.isClosed());

      // Test UUID store with all methods
    }

    try (FileRecordStore uuidStore =
        new FileRecordStoreBuilder()
            .tempFile("test-comprehensive-uuid-", ".db")
            .uuidKeys()
            .maxKeyLength(16)
            .open()) {

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      // Test UUID insert
      uuidStore.insertRecord(uuid1, "uuid-value-1".getBytes());
      uuidStore.insertRecord(uuid2, "uuid-value-2".getBytes());

      // Test UUID read
      byte[] uuidRead1 = uuidStore.readRecordData(uuid1);
      Assert.assertArrayEquals("uuid-value-1".getBytes(), uuidRead1);

      // Test UUID update
      uuidStore.updateRecord(uuid1, "updated-uuid-value-1".getBytes());
      byte[] uuidUpdated1 = uuidStore.readRecordData(uuid1);
      Assert.assertArrayEquals("updated-uuid-value-1".getBytes(), uuidUpdated1);

      // Test UUID delete
      uuidStore.deleteRecord(uuid2);
      Assert.assertFalse("UUID record should be deleted", uuidStore.recordExists(uuid2));

      // Test UUID query methods
      Assert.assertFalse("UUID store should not be empty", uuidStore.isEmpty());
      Assert.assertEquals("UUID store should have 1 record", 1, uuidStore.size());
      Assert.assertTrue("UUID record should exist", uuidStore.recordExists(uuid1));

      // Test UUID key iteration
      int uuidCount = 0;
      for (UUID uuid : uuidStore.uuidKeys()) {
        uuidCount++;
        Assert.assertEquals("Should be uuid1", uuid1, uuid);
      }
      Assert.assertEquals("Should have 1 UUID key", 1, uuidCount);

      // Test UUID file operations
      uuidStore.fsync();
      Assert.assertFalse("UUID store should not be closed", uuidStore.isClosed());
    }

    LOGGER.info("=== Comprehensive API Coverage Tests Completed ===");
  }

  @Test
  public void testKeyLengthRecordedInFile() throws Exception {
    // set a super sized key - use the 8-byte aligned maximum
    System.setProperty(
        String.format("%s.%s", FileRecordStore.class.getName(), "MAX_KEY_LENGTH"),
        Integer.valueOf(FileRecordStoreBuilder.MAX_KEY_LENGTH).toString());
    // create a store with this key
    recordsFile =
        new FileRecordStoreBuilder()
            .path(Paths.get(fileName))
            .preallocatedRecords(initialSize)
            .allowZeroPreallocation()
            .maxKeyLength(FileRecordStoreBuilder.MAX_KEY_LENGTH)
            .open();

    // Use a pattern that won't produce CRC ending in zeros - alternating bytes
    StringBuilder keyBuilder = new StringBuilder();
    for (int i = 0; i < recordsFile.maxKeyLength; i++) {
      keyBuilder.append((char) ((i % 2 == 0) ? 0x42 : 0x69)); // B i B i B i...
    }
    final String longestKey = keyBuilder.toString();
    byte[] key = longestKey.getBytes();
    byte[] value = longestKey.getBytes();
    recordsFile.insertRecord(key, value);

    // reset to the normal default
    System.setProperty(
        String.format("%s.%s", FileRecordStore.class.getName(), "MAX_KEY_LENGTH"),
        Integer.valueOf(FileRecordStoreBuilder.DEFAULT_MAX_KEY_LENGTH).toString());
    recordsFile =
        new FileRecordStoreBuilder()
            .path(Paths.get(fileName))
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
            .maxKeyLength(FileRecordStoreBuilder.MAX_KEY_LENGTH)
            .open();

    String put0 = new String(recordsFile.readRecordData(longestKey.getBytes()));

    Assert.assertEquals(put0, longestKey);
  }
}
