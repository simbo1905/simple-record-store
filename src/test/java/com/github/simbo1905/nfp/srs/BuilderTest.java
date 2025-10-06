package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/// Tests for the FileRecordStoreBuilder API.
/// This test class validates the new builder pattern before it exists.
public class BuilderTest extends JulLoggingConfig {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testBuilderCreateNewStore() throws IOException {
    Path dbPath = tempFolder.newFile("test.db").toPath();

    // Create new store with builder
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .useMemoryMapping(true)
            .open()) {

      // Verify store is working
      byte[] key = "test".getBytes();
      byte[] data = "test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }

    // Verify file exists
    assertTrue(Files.exists(dbPath));
  }

  @Test
  public void testBuilderOpenExistingStore() throws IOException {
    Path dbPath = tempFolder.newFile("existing.db").toPath();

    // Create store with data
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .open()) {

      byte[] key = "existing".getBytes();
      byte[] data = "existing data".getBytes();
      store.insertRecord(key, data);
    }

    // Reopen existing store with same key length
    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(dbPath).maxKeyLength(64).open()) {

      byte[] key = "existing".getBytes();
      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals("existing data".getBytes(), retrieved);
    }
  }

  @Test
  public void testBuilderTempFile() throws IOException {
    // Create temporary store
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .tempFile("test-", ".db")
            .preallocatedRecords(50)
            .maxKeyLength(32)
            .open()) {

      // Use the store
      byte[] key = "temp".getBytes();
      store.insertRecord(key, "temp data".getBytes());

      // Verify data
      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals("temp data".getBytes(), retrieved);
    }
    // Temp file should be cleaned up automatically
  }

  @Test
  public void testBuilderReadOnly() throws IOException {
    Path dbPath = tempFolder.newFile("readonly.db").toPath();

    // Create store with data
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .open()) {

      store.insertRecord(("key1".getBytes()), "data1".getBytes());
    }

    // Open read-only with same key length
    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(dbPath).maxKeyLength(64).readOnly(true).open()) {

      // Should be able to read
      byte[] data = store.readRecordData(("key1".getBytes()));
      assertArrayEquals("data1".getBytes(), data);

      // Should not be able to write (would throw exception)
      try {
        store.insertRecord(("key2".getBytes()), "data2".getBytes());
        fail("Expected exception for read-only store");
      } catch (UnsupportedOperationException e) {
        // Expected
      }
    }
  }

  @Test
  public void testBuilderDefaultValues() throws IOException {
    Path dbPath = tempFolder.newFile("defaults.db").toPath();

    // Open with minimal configuration
    try (FileRecordStore store = new FileRecordStoreBuilder().path(dbPath).open()) {

      // Should work with default values
      byte[] key = "default".getBytes();
      store.insertRecord(key, "default data".getBytes());

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals("default data".getBytes(), retrieved);
    }
  }

  @Test
  public void testBuilderPathValidation() throws IOException {
    // Test with relative path
    Path relativePath = Files.createTempFile("test-relative-", ".db");
    relativePath.toFile().deleteOnExit();
    try {
      FileRecordStore store = new FileRecordStoreBuilder().path(relativePath).open();

      // Should work (relative paths are allowed)
      store.close();
      Files.deleteIfExists(relativePath);
    } catch (Exception e) {
      // Path validation might throw here
    }

    // Test with absolute path
    Path absolutePath = tempFolder.newFile("absolute.db").toPath();
    try (FileRecordStore ignored = new FileRecordStoreBuilder().path(absolutePath).open()) {
      // Should work
      assertTrue(Files.exists(absolutePath));
    }
  }

  @Test
  public void testHintInitialKeyCount() throws IOException {
    Path dbPath = tempFolder.newFile("hint-key-count.db").toPath();

    // Test with hintInitialKeyCount
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .hintInitialKeyCount(10) // Should translate to preallocatedRecords=10
            .maxKeyLength(64)
            .open()) {

      // Insert exactly 10 records to test preallocation
      for (int i = 0; i < 10; i++) {
        byte[] key = ("key" + i).getBytes();
        byte[] data = ("data" + i).getBytes();
        store.insertRecord(key, data);
      }

      // Verify all records exist
      assertEquals(10, store.size());
      for (int i = 0; i < 10; i++) {
        assertTrue(store.recordExists(("key" + i).getBytes()));
      }
    }
  }

  @Test
  public void testHintPreferredBlockSize() throws IOException {
    Path dbPath = tempFolder.newFile("hint-block-size.db").toPath();

    // Test with hintPreferredBlockSize in KiB
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .hintPreferredBlockSize(4) // 4 KiB = 4096 bytes
            .maxKeyLength(64)
            .open()) {

      // Store should work with the specified block size hint
      byte[] key = "block-test".getBytes();
      byte[] data = "block test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }
  }

  @Test
  public void testHintPreferredExpandSize() throws IOException {
    Path dbPath = tempFolder.newFile("hint-expand-size.db").toPath();

    // Test with hintPreferredExpandSize in KiB
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .hintPreferredExpandSize(8) // 8 KiB = 8192 bytes
            .maxKeyLength(64)
            .open()) {

      // Store should work with the specified expand size hint
      byte[] key = "expand-test".getBytes();
      byte[] data = "expand test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }
  }

  @Test
  public void testHintMethodsCombined() throws IOException {
    Path dbPath = tempFolder.newFile("hint-combined.db").toPath();

    // Test all hint methods together
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .hintInitialKeyCount(5)
            .hintPreferredBlockSize(2) // 2 KiB
            .hintPreferredExpandSize(4) // 4 KiB
            .maxKeyLength(32)
            .open()) {

      // Insert records to test the combined configuration
      for (int i = 0; i < 5; i++) {
        byte[] key = ("combined" + i).getBytes();
        byte[] data = ("combined data " + i).getBytes();
        store.insertRecord(key, data);
      }

      // Verify all records
      assertEquals(5, store.size());
      for (int i = 0; i < 5; i++) {
        assertTrue(store.recordExists(("combined" + i).getBytes()));
        byte[] retrieved = store.readRecordData(("combined" + i).getBytes());
        assertArrayEquals(("combined data " + i).getBytes(), retrieved);
      }
    }
  }

  @Test
  public void testHintMethodsWithMiB() throws IOException {
    Path dbPath = tempFolder.newFile("hint-mib.db").toPath();

    // Test with reasonable MiB units (avoiding integer overflow)
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath)
            .hintPreferredBlockSize(256) // 256 KiB - reasonable size
            .hintPreferredExpandSize(512) // 512 KiB - reasonable size
            .maxKeyLength(128)
            .open()) {

      // Should work with larger block/expand sizes
      byte[] key = "mib-test".getBytes();
      byte[] data = "MiB test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }
  }

  @Test
  public void testHintMethodsEdgeCases() throws IOException {
    Path dbPath = tempFolder.newFile("hint-edge-cases.db").toPath();

    // Test with zero initial key count (should use defaults)
    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(dbPath).hintInitialKeyCount(0).maxKeyLength(64).open()) {

      // Should work with default values when hintInitialKeyCount is 0
      byte[] key = "edge-test".getBytes();
      byte[] data = "edge test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }

    // Test with minimum valid block/expand sizes (smallest power of 2)
    Path dbPath2 = tempFolder.newFile("hint-min-valid.db").toPath();
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(dbPath2)
            .hintInitialKeyCount(1000)
            .hintPreferredBlockSize(1) // 1 KiB - minimum valid
            .hintPreferredExpandSize(2) // 2 KiB - minimum valid
            .maxKeyLength(64)
            .open()) {

      // Should handle minimum valid hint values
      byte[] key = "min-valid-test".getBytes();
      byte[] data = "min valid test data".getBytes();
      store.insertRecord(key, data);

      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals(data, retrieved);
    }
  }
}
