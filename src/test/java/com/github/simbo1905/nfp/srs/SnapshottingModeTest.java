package com.github.simbo1905.nfp.srs;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

/// Tests for the new snapshotting mode functionality.
/// Validates the in-place update and header expansion control flags.
public class SnapshottingModeTest extends JulLoggingConfig {

  @Test
  public void testInPlaceUpdateToggle() throws Exception {
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .tempFile("test-inplace-toggle-", ".db")
            .maxKeyLength(64)
            .open()) {

      // Verify default state
      Assert.assertTrue(
          "In-place updates should be enabled by default", store.isAllowInPlaceUpdates());

      // Test toggling off
      store.setAllowInPlaceUpdates(false);
      Assert.assertFalse("In-place updates should be disabled", store.isAllowInPlaceUpdates());

      // Test toggling back on
      store.setAllowInPlaceUpdates(true);
      Assert.assertTrue("In-place updates should be re-enabled", store.isAllowInPlaceUpdates());
    }
  }

  @Test
  public void testHeaderExpansionToggle() throws Exception {
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .tempFile("test-header-toggle-", ".db")
            .maxKeyLength(64)
            .open()) {

      // Verify default state
      Assert.assertTrue(
          "Header expansion should be enabled by default", store.isAllowHeaderExpansion());

      // Test toggling off
      store.setAllowHeaderExpansion(false);
      Assert.assertFalse("Header expansion should be disabled", store.isAllowHeaderExpansion());

      // Test toggling back on
      store.setAllowHeaderExpansion(true);
      Assert.assertTrue("Header expansion should be re-enabled", store.isAllowHeaderExpansion());
    }
  }

  @Test
  public void testSnapshottingModeBasicOperations() throws Exception {
    Path tempPath = Files.createTempFile("test-snapshotting-", ".db");
    tempPath.toFile().deleteOnExit();

    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(tempPath)
            .preallocatedRecords(10) // Limited pre-allocation
            .maxKeyLength(64)
            .open()) {

      // Enter snapshotting mode
      store.setAllowInPlaceUpdates(false);
      store.setAllowHeaderExpansion(false);

      // Test basic operations still work
      byte[] key = "test-key".getBytes();
      byte[] value = "test-value".getBytes();

      store.insertRecord(key, value);
      Assert.assertTrue("Record should exist", store.recordExists(key));

      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Value should match", value, readValue);

      // Test update with same size (should work even with in-place disabled)
      byte[] sameSizeValue = "same-size".getBytes();
      store.updateRecord(key, sameSizeValue);
      byte[] updatedValue = store.readRecordData(key);
      Assert.assertArrayEquals("Updated value should match", sameSizeValue, updatedValue);

      // Test update with larger size (should work - forces move to end)
      byte[] largerValue = "this-is-a-much-larger-value".getBytes();
      store.updateRecord(key, largerValue);
      byte[] largerReadValue = store.readRecordData(key);
      Assert.assertArrayEquals("Larger value should match", largerValue, largerReadValue);

      // Exit snapshotting mode
      store.setAllowInPlaceUpdates(true);
      store.setAllowHeaderExpansion(true);
    }
  }

  @Test
  public void testSnapshottingModeWithUUIDKeys() throws Exception {
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .tempFile("test-snapshotting-uuid-", ".db")
            .uuidKeys()
            .maxKeyLength(16)
            .preallocatedRecords(10)
            .open()) {

      // Enter snapshotting mode
      store.setAllowInPlaceUpdates(false);
      store.setAllowHeaderExpansion(false);

      // Test UUID operations
      java.util.UUID uuid = java.util.UUID.randomUUID();
      byte[] value = "uuid-value".getBytes();

      store.insertRecord(uuid, value);
      Assert.assertTrue("UUID record should exist", store.recordExists(uuid));

      byte[] readValue = store.readRecordData(uuid);
      Assert.assertArrayEquals("UUID value should match", value, readValue);

      // Exit snapshotting mode
      store.setAllowInPlaceUpdates(true);
      store.setAllowHeaderExpansion(true);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testHeaderExpansionDisabledWithInsufficientSpace() throws Exception {
    Path tempPath = Files.createTempFile("test-header-rejection-", ".db");
    tempPath.toFile().deleteOnExit();

    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(tempPath)
            .preallocatedRecords(2) // Very limited pre-allocation
            .maxKeyLength(64)
            .open()) {

      // Disable header expansion
      store.setAllowHeaderExpansion(false);

      // Fill up the pre-allocated space
      for (int i = 0; i < 2; i++) {
        byte[] key = ("key" + i).getBytes();
        byte[] value = ("value" + i).getBytes();
        store.insertRecord(key, value);
      }

      // This should throw IllegalStateException due to insufficient space
      byte[] key = "overflow-key".getBytes();
      byte[] value = "overflow-value".getBytes();
      store.insertRecord(key, value); // Should throw exception
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testReadOnlyStoreCannotToggleFlags() throws Exception {
    Path tempPath = Files.createTempFile("test-readonly-toggle-", ".db");
    tempPath.toFile().deleteOnExit();

    // Create and populate a store
    try (FileRecordStore writeStore =
        new FileRecordStoreBuilder().path(tempPath).maxKeyLength(64).open()) {
      writeStore.insertRecord("key".getBytes(), "value".getBytes());
    }

    // Open in read-only mode
    try (FileRecordStore readStore =
        new FileRecordStoreBuilder()
            .path(tempPath)
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_ONLY)
            .maxKeyLength(64)
            .open()) {

      // This should throw UnsupportedOperationException
      readStore.setAllowInPlaceUpdates(false);
    }
  }
}
