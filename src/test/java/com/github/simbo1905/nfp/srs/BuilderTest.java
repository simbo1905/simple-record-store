package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/// Tests for the FileRecordStore.Builder API.
/// This test class validates the new builder pattern before it exists.
public class BuilderTest extends JulLoggingConfig {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testBuilderCreateNewStore() throws IOException {
    Path dbPath = tempFolder.newFile("test.db").toPath();

    // Create new store with builder
    try (FileRecordStore store =
        new FileRecordStore.Builder()
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
        new FileRecordStore.Builder()
            .path(dbPath)
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .open()) {

      byte[] key = "existing".getBytes();
      byte[] data = "existing data".getBytes();
      store.insertRecord(key, data);
    }

    // Reopen existing store
    try (FileRecordStore store = new FileRecordStore.Builder().path(dbPath).open()) {

      byte[] key = "existing".getBytes();
      byte[] retrieved = store.readRecordData(key);
      assertArrayEquals("existing data".getBytes(), retrieved);
    }
  }

  @Test
  public void testBuilderTempFile() throws IOException {
    // Create temporary store
    try (FileRecordStore store =
        new FileRecordStore.Builder()
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
        new FileRecordStore.Builder()
            .path(dbPath)
            .preallocatedRecords(100)
            .maxKeyLength(64)
            .open()) {

      store.insertRecord(("key1".getBytes()), "data1".getBytes());
    }

    // Open read-only
    try (FileRecordStore store = new FileRecordStore.Builder().path(dbPath).readOnly(true).open()) {

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
    try (FileRecordStore store = new FileRecordStore.Builder().path(dbPath).open()) {

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
    Path relativePath = Paths.get("relative.db");
    try {
      FileRecordStore store = new FileRecordStore.Builder().path(relativePath).open();

      // Should work (relative paths are allowed)
      store.close();
      Files.deleteIfExists(relativePath);
    } catch (Exception e) {
      // Path validation might throw here
    }

    // Test with absolute path
    Path absolutePath = tempFolder.newFile("absolute.db").toPath();
    try (FileRecordStore ignored = new FileRecordStore.Builder().path(absolutePath).open()) {
      // Should work
      assertTrue(Files.exists(absolutePath));
    }
  }
}
