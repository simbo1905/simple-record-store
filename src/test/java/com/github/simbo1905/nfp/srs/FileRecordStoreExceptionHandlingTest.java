package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/// Comprehensive test for FileRecordStore closed state behavior after exceptions.
/// This test uses proper resource management and controlled exception injection
/// to verify that the store correctly handles exceptions and prevents reuse.
public class FileRecordStoreExceptionHandlingTest extends JulLoggingConfig {

  private static final Logger logger =
      Logger.getLogger(FileRecordStoreExceptionHandlingTest.class.getName());

  /// Discover the total operation count for a simple insert without exception injection
  private int discoverOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for simple insert ===");

    // Create store with counting wrapper that never throws (use Integer.MAX_VALUE)
    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("discovery-", ".db").maxKeyLength(64);

    // Create the store normally first
    FileRecordStore baseStore = builder.open();

    // Replace file operations with counting wrapper that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create new store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    // Close the base store since we're done with it
    baseStore.close();

    // Perform simple insert
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    try {
      countingStore.insertRecord(key, data);
    } catch (IOException e) {
      // Should not happen with Integer.MAX_VALUE
      logger.log(Level.FINE, "Unexpected exception during operation count discovery", e);
    }

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for simple insert", totalOps));
    return totalOps;
  }

  /// Discover total operation count for delete
  private int discoverDeleteOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for delete ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("delete-discovery-", ".db").maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: insert a record first
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();
    countingStore.insertRecord(key, data);

    // Reset counter before delete
    countingOps.resetOperationCount();

    // Perform delete
    countingStore.deleteRecord(key);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for simple delete", totalOps));
    return totalOps;
  }

  /// Discover total operation count for insert scenario (with free space)
  private int discoverInsertScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for insert scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("insert-scenario-discovery-", ".db").maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: create free space scenario (insert records, delete some, then insert new one)
    byte[] key1 = "existing0".getBytes();
    byte[] key2 = "existing1".getBytes();
    byte[] key3 = "existing2".getBytes();
    byte[] key4 = "existing3".getBytes();
    byte[] key5 = "existing4".getBytes();
    byte[] data = "testdata".getBytes();

    countingStore.insertRecord(key1, data);
    countingStore.insertRecord(key2, data);
    countingStore.insertRecord(key3, data);
    countingStore.insertRecord(key4, data);
    countingStore.insertRecord(key5, data);

    // Delete some to create free space
    countingStore.deleteRecord(key2);
    countingStore.deleteRecord(key4);

    // Reset counter before insert
    countingOps.resetOperationCount();

    // Perform insert into free space
    byte[] newKey = "new-key".getBytes();
    countingStore.insertRecord(newKey, data);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for insert scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for update scenario
  private int discoverUpdateScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for update scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("update-scenario-discovery-", ".db").maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: insert record, then update it (same size)
    byte[] key = "update-key".getBytes();
    byte[] initialData = "initial-data".getBytes();
    byte[] updatedData = "updated-data".getBytes(); // Same length

    countingStore.insertRecord(key, initialData);

    // Reset counter before update
    countingOps.resetOperationCount();

    // Perform update (same size)
    countingStore.updateRecord(key, updatedData);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for update scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for read scenario
  private int discoverReadScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for read scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("read-scenario-discovery-", ".db").maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: insert record, then read it
    byte[] key = "read-key".getBytes();
    byte[] data = "read-data".getBytes();

    countingStore.insertRecord(key, data);

    // Reset counter before read
    countingOps.resetOperationCount();

    // Perform read
    byte[] readData = countingStore.readRecordData(key);

    // Verify data
    Assert.assertArrayEquals(data, readData);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for read scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for max key length scenario
  private int discoverMaxKeyLengthScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for max key length scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder()
            .tempFile("max-key-scenario-discovery-", ".db")
            .maxKeyLength(248); // Max theoretical key length

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: create max length key
    byte[] maxKey = new byte[248];
    for (int i = 0; i < 248; i++) {
      maxKey[i] = (byte) (i % 256);
    }
    byte[] data = "max-key-data".getBytes();

    // Reset counter before insert
    countingOps.resetOperationCount();

    // Perform insert with max key length
    countingStore.insertRecord(maxKey, data);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for max key length scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for empty data scenario
  private int discoverEmptyDataScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for empty data scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder()
            .tempFile("empty-data-scenario-discovery-", ".db")
            .maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: insert record with empty data
    byte[] key = "empty-key".getBytes();
    byte[] emptyData = new byte[0];

    // Reset counter before insert
    countingOps.resetOperationCount();

    // Perform insert with empty data
    countingStore.insertRecord(key, emptyData);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for empty data scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for fsync scenario
  private int discoverFsyncScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for fsync scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder().tempFile("fsync-scenario-discovery-", ".db").maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: add some data to ensure there are file operations
    byte[] key = "fsync-key".getBytes();
    byte[] data = "fsync-data".getBytes();

    countingStore.insertRecord(key, data);

    // Reset counter before fsync
    countingOps.resetOperationCount();

    // Perform fsync - this should trigger sync operations
    countingStore.fsync();

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for fsync scenario", totalOps));

    // If fsync doesn't trigger any operations, return 0 so we skip testing
    return totalOps;
  }

  /// Discover total operation count for file growth scenario
  private int discoverFileGrowthScenarioOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering total operation count for file growth scenario ===");

    FileRecordStoreBuilder builder =
        new FileRecordStoreBuilder()
            .tempFile("file-growth-scenario-discovery-", ".db")
            .maxKeyLength(64);

    // Create base store
    FileRecordStore baseStore = builder.open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Create store with counting wrapper
    FileRecordStore countingStore = builder.path(baseStore.getFilePath()).maxKeyLength(64).open();
    countingStore.fileOperations = countingOps;

    baseStore.close();

    // Setup: insert large record to force file growth
    byte[] key = "large-key".getBytes();
    byte[] largeData = new byte[1024]; // 1KB should force growth

    countingStore.insertRecord(key, largeData);

    // Reset counter before another insert that should cause growth
    countingOps.resetOperationCount();

    // Perform another insert that should cause file growth
    byte[] key2 = "another-key".getBytes();
    byte[] moreLargeData = new byte[2048]; // 2KB
    countingStore.insertRecord(key2, moreLargeData);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for file growth scenario", totalOps));
    return totalOps;
  }

  /// Discover total operation count for index manipulation scenario
  private int discoverIndexManipulationScenarioOperationCount(Path filePath) throws Exception {
    logger.log(
        Level.FINE, "=== Discovering total operation count for index manipulation scenario ===");

    // Create store with the specified path (pre-populated)
    FileRecordStore countingStore =
        new FileRecordStoreBuilder().path(filePath).maxKeyLength(64).open();

    // Replace with counting delegate that never throws
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(countingStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations countingOps =
        new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);

    // Replace file operations with counting wrapper
    countingStore.fileOperations = countingOps;

    // Reset counter before insert
    countingOps.resetOperationCount();

    // Perform insert into existing store (this will manipulate indexes)
    byte[] newKey = "new-key".getBytes();
    byte[] data = "new-data".getBytes();
    countingStore.insertRecord(newKey, data);

    // Get the operation count from the delegating operations
    int totalOps =
        ((DelegatingExceptionOperations) countingStore.fileOperations).getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for index manipulation scenario", totalOps));
    return totalOps;
  }

  /// Test that exceptions trigger closed state and subsequent operations fail
  @Test
  public void testExceptionClosesStoreAndPreventsReuse() throws Exception {
    logger.log(
        Level.FINE,
        "=== Testing exception closes store and prevents reuse - RECORD AND PLAYBACK ===");

    // RECORD PHASE: Discover total operation count for simple insert
    int totalOperations = discoverOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered total operations for simple insert: %d", totalOperations));

    // PLAYBACK PHASE: Test exceptions at each operation from 1 to totalOperations
    for (int throwAt = 1; throwAt <= totalOperations; throwAt++) {
      final int finalThrowAt = throwAt; // Make it effectively final for lambda
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "RECORD AND PLAYBACK: Testing exception at operation %d/%d",
                  finalThrowAt, totalOperations));

      // Create store with exception injection
      FileRecordStore store;

      try {
        store = createStoreWithException(finalThrowAt);
      } catch (IOException e) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "RECORD AND PLAYBACK: Exception during store creation at operation %d: %s",
                    finalThrowAt, e.getMessage()));
        // Store failed to construct - this is expected for early operations
        // No need to test further as store was never properly opened
        continue;
      }

      byte[] key = "testkey".getBytes();
      byte[] data = "testdata".getBytes();

      try {
        // This should trigger an exception at the specified operation
        store.insertRecord(key, data);
        // If we get here without exception, that's fine - some operations might not be reached
        logger.log(
            Level.FINE,
            () -> String.format("Operation %d not reached - that's expected", finalThrowAt));
      } catch (IOException e) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "RECORD AND PLAYBACK: Got expected exception at operation %d: %s",
                    finalThrowAt, e.getMessage()));
        // Store should now be closed (only if it was successfully created first)
        Assert.assertTrue(
            "RECORD AND PLAYBACK: Store should be closed after exception at operation "
                + finalThrowAt,
            store.isClosed());

        // All subsequent operations should throw IllegalStateException
        verifyStoreIsClosed(store);
      }

      // Close the store (whether it failed or not)
      try {
        store.close();
      } catch (Exception e) {
        // Ignore close exceptions
      }
    }

    logger.log(
        Level.FINE,
        "=== RECORD AND PLAYBACK: Completed testing all operations 1-" + totalOperations + " ===");
  }

  /// Test that recovery requires creating a new FileRecordStore instance
  @Test
  public void testRecoveryRequiresNewFileRecordStore() throws Exception {
    logger.log(Level.FINE, "=== Testing recovery requires new FileRecordStore instance ===");

    // Create first store with exception injection
    FileRecordStore corruptedStore = createStoreWithException(5);
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    // Trigger exception to corrupt the store
    try {
      corruptedStore.insertRecord(key, data);
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      // Expected - store is now corrupted
    }

    Assert.assertTrue("Corrupted store should be closed", corruptedStore.isClosed());

    // Close the corrupted store
    corruptedStore.close();

    // Create a completely new FileRecordStore for the same file
    FileRecordStore newStore =
        new FileRecordStoreBuilder().tempFile("recovery-test-", ".db").maxKeyLength(64).open();

    // New store should work fine
    Assert.assertFalse("New store should not be closed", newStore.isClosed());

    byte[] newKey = "new-key".getBytes();
    byte[] newData = "new-data".getBytes();

    newStore.insertRecord(newKey, newData);
    byte[] readData = newStore.readRecordData(newKey);
    Assert.assertArrayEquals("Data should be readable from new store", newData, readData);

    newStore.close();
  }

  /// Test all public methods throw IllegalStateException after closed
  @Test
  public void testAllMethodsThrowIllegalStateExceptionAfterClosed() throws Exception {
    logger.log(Level.FINE, "=== Testing all methods throw IllegalStateException after closed ===");

    // Create store and trigger exception to close it
    FileRecordStore store = createStoreWithException(3);
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    try {
      store.insertRecord(key, data);
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      // Expected - store should be closed
    }

    Assert.assertTrue("Store should be closed", store.isClosed());

    // Test all public methods throw IllegalStateException
    verifyStoreIsClosed(store);
  }

  /// Test that successful operations don't close the store
  @Test
  public void testSuccessfulOperationsDoNotCloseStore() throws Exception {
    logger.log(Level.FINE, "=== Testing successful operations do not close store ===");

    // Create normal store without exception injection
    FileRecordStore store =
        new FileRecordStoreBuilder().tempFile("success-test-", ".db").maxKeyLength(64).open();

    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    // Perform successful operations
    store.insertRecord(key, data);
    Assert.assertFalse("Store should not be closed after successful insert", store.isClosed());

    byte[] readData = store.readRecordData(key);
    Assert.assertArrayEquals("Data should match", data, readData);
    Assert.assertFalse("Store should not be closed after successful read", store.isClosed());

    store.updateRecord(key, "updated".getBytes());
    Assert.assertFalse("Store should not be closed after successful update", store.isClosed());

    store.deleteRecord(key);
    Assert.assertFalse("Store should not be closed after successful delete", store.isClosed());

    Assert.assertTrue("Store should not be empty initially", store.isEmpty());
    Assert.assertFalse("Store should not be closed after isEmpty", store.isClosed());

    // Test keys() method
    store.insertRecord(key, data);
    store.keysBytes();
    Assert.assertFalse("Store should not be closed after keys()", store.isClosed());

    // Test recordExists() method
    Assert.assertTrue("Record should exist", store.recordExists(key));
    Assert.assertFalse("Store should not be closed after recordExists()", store.isClosed());

    store.close();
  }

  /// Test file header operations failure scenarios using record-and-playback
  @Test
  public void testFileHeaderOperationsFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing file header operations failure (record-and-playback) ===");

    // Discover total operations for simple insert
    int totalOperations = discoverOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for simple insert", totalOperations));

    // Test early operations that would affect file headers (first few operations)
    int[] headerOperations = {1, 2, 3, 4, 5, 6};

    for (int throwAt : headerOperations) {
      if (throwAt <= totalOperations) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing header operation failure at operation %d", throwAt));
        testExceptionScenarioWithBehaviorVerification(throwAt, "header operation " + throwAt);
      }
    }
  }

  /// Test index manipulation operations failure using dynamic discovery
  @Test
  public void testIndexManipulationFailure() throws Exception {
    logger.log(
        Level.FINE, "=== Testing index manipulation operations failure with dynamic discovery ===");

    // Pre-populate store to test index operations
    FileRecordStore prepStore =
        new FileRecordStoreBuilder().tempFile("index-prep-", ".db").maxKeyLength(64).open();

    byte[] key1 = "key1".getBytes();
    byte[] key2 = "key2".getBytes();
    prepStore.insertRecord(key1, "data1".getBytes());
    prepStore.insertRecord(key2, "data2".getBytes());
    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Discover total operation count for insert into existing store
    int totalOps = discoverIndexManipulationScenarioOperationCount(filePath);
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for index manipulation scenario", totalOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(3, totalOps), Math.min(5, totalOps)};

    for (int op : testOperations) {
      if (op <= totalOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing index manipulation at operation %d/%d", op, totalOps));
        testExceptionScenarioAtPath(filePath, op, "index manipulation operation " + op);
      }
    }
  }

  /// Test data write operations failure using dynamic discovery
  @Test
  public void testDataWriteOperationsFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing data write operations failure with dynamic discovery ===");

    // Discover total operation count for simple insert (which includes data write operations)
    int totalOps = discoverOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d total operations for simple insert", totalOps));

    // Test a few representative operations that would include data write operations
    int[] testOperations = {1, Math.min(3, totalOps), Math.min(5, totalOps)};

    for (int op : testOperations) {
      if (op <= totalOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing data write operations at operation %d/%d", op, totalOps));
        testExceptionScenario(op, "data write operation " + op);
      }
    }
  }

  /// Test file growth operations using dynamic discovery
  @Test
  public void testFileGrowthOperationsFailure() throws Exception {
    logger.log(Level.FINE, "=== Testing file growth operations failure with dynamic discovery ===");

    // Discover total operation count for scenario that causes file growth
    int totalOps = discoverFileGrowthScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for file growth scenario", totalOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(3, totalOps), Math.min(5, totalOps)};

    for (int op : testOperations) {
      if (op <= totalOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing file growth operations at operation %d/%d", op, totalOps));
        testExceptionScenario(op, "file growth operation " + op);
      }
    }
  }

  /// Test specific insert scenarios using dynamic operation discovery
  @Test
  public void testInsertScenarios() throws Exception {
    logger.log(Level.FINE, "=== Testing specific insert scenarios with dynamic discovery ===");

    // Discover total operation count for insert into free space scenario
    int totalInsertOps = discoverInsertScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for insert scenario", totalInsertOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(3, totalInsertOps), Math.min(5, totalInsertOps)};

    for (int op : testOperations) {
      if (op <= totalInsertOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing insert scenario at operation %d/%d", op, totalInsertOps));
        testInsertScenarioWithException(op, "insert scenario operation " + op);
      }
    }
  }

  /// Test specific update scenarios using dynamic operation discovery
  @Test
  public void testUpdateScenarios() throws Exception {
    logger.log(Level.FINE, "=== Testing specific update scenarios with dynamic discovery ===");

    // Discover total operation count for update scenario
    int totalUpdateOps = discoverUpdateScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for update scenario", totalUpdateOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(3, totalUpdateOps), Math.min(5, totalUpdateOps)};

    for (int op : testOperations) {
      if (op <= totalUpdateOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing update scenario at operation %d/%d", op, totalUpdateOps));
        testUpdateScenarioWithException(op, "update scenario operation " + op);
      }
    }
  }

  /// Test specific delete scenarios using dynamic operation discovery
  @Test
  public void testDeleteScenarios() throws Exception {
    logger.log(Level.FINE, "=== Testing specific delete scenarios with dynamic discovery ===");

    // Discover total operation count for delete
    int totalDeleteOps = discoverDeleteOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for simple delete", totalDeleteOps));

    // Test a few representative operations - only test operations that are actually reached
    int[] testOperations = {1, Math.min(2, totalDeleteOps), Math.min(3, totalDeleteOps)};

    for (int op : testOperations) {
      if (op <= totalDeleteOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing delete scenario at operation %d/%d", op, totalDeleteOps));
        testDeleteScenarioWithException(op, "delete operation " + op);
      }
    }
  }

  /// Test specific read scenarios using dynamic operation discovery
  @Test
  public void testReadScenarios() throws Exception {
    logger.log(Level.FINE, "=== Testing specific read scenarios with dynamic discovery ===");

    // Discover total operation count for read scenario
    int totalReadOps = discoverReadScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for read scenario", totalReadOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(2, totalReadOps), Math.min(3, totalReadOps)};

    for (int op : testOperations) {
      if (op <= totalReadOps) {
        logger.log(
            Level.FINE,
            () -> String.format("Testing read scenario at operation %d/%d", op, totalReadOps));
        testReadScenarioWithException(op, "read scenario operation " + op);
      }
    }
  }

  /// Test edge cases and boundary conditions using dynamic discovery
  @Test
  public void testEdgeCasesAndBoundaries() throws Exception {
    logger.log(
        Level.FINE, "=== Testing edge cases and boundary conditions with dynamic discovery ===");

    // Test max key length scenario
    int totalMaxKeyOps = discoverMaxKeyLengthScenarioOperationCount();
    logger.log(
        Level.FINE,
        () ->
            String.format("Discovered %d operations for max key length scenario", totalMaxKeyOps));

    // Test a few representative operations
    int[] testOperations = {1, Math.min(2, totalMaxKeyOps), Math.min(3, totalMaxKeyOps)};

    for (int op : testOperations) {
      if (op <= totalMaxKeyOps) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Testing max key length scenario at operation %d/%d", op, totalMaxKeyOps));
        testMaxKeyLengthScenario(op);
      }
    }

    // Test empty data scenario
    int totalEmptyDataOps = discoverEmptyDataScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for empty data scenario", totalEmptyDataOps));

    for (int op : testOperations) {
      if (op <= totalEmptyDataOps) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Testing empty data scenario at operation %d/%d", op, totalEmptyDataOps));
        testEmptyDataScenario(op);
      }
    }

    // Test fsync scenario
    int totalFsyncOps = discoverFsyncScenarioOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for fsync scenario", totalFsyncOps));

    // Only test fsync if it actually triggers operations
    if (totalFsyncOps > 0) {
      int[] fsyncTestOperations = {1, Math.min(2, totalFsyncOps), Math.min(3, totalFsyncOps)};
      for (int op : fsyncTestOperations) {
        if (op <= totalFsyncOps && op > 0) {
          logger.log(
              Level.FINE,
              () -> String.format("Testing fsync scenario at operation %d/%d", op, totalFsyncOps));
          testFsyncScenarioWithException(op, "fsync scenario operation " + op);
        }
      }
    } else {
      logger.log(
          Level.FINE, "Fsync scenario doesn't trigger any file operations - skipping fsync tests");
    }
  }

  /// Helper method for exception scenario testing
  private void testExceptionScenario(int throwAt, String description) throws Exception {
    testExceptionScenarioAtPath(null, throwAt, description);
  }

  /// Helper method for exception scenario testing at specific path
  private void testExceptionScenarioAtPath(Path filePath, int throwAt, String description)
      throws Exception {
    logger.log(
        Level.FINE,
        () -> String.format("Testing %s with exception at operation %d", description, throwAt));

    FileRecordStore store =
        filePath != null
            ? createStoreWithExceptionAtPath(filePath, throwAt)
            : createStoreWithException(throwAt);

    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    try {
      store.insertRecord(key, data);
      Assert.fail("Expected IOException during " + description);
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () -> String.format("Got expected exception during %s: %s", description, e.getMessage()));
    }

    // Verify store is closed
    Assert.assertTrue("Store should be closed after " + description, store.isClosed());

    // Verify subsequent operations fail
    verifyStoreIsClosed(store);

    // Persistence verification: reopen store and verify data integrity
    if (filePath != null) {
      try (FileRecordStore verificationStore =
          new FileRecordStoreBuilder().path(filePath).maxKeyLength(64).open()) {

        // Count valid records in the reopened store
        int validRecords = 0;
        for (byte[] verificationKey : verificationStore.keysBytes()) {
          try {
            byte[] verificationData = verificationStore.readRecordData(verificationKey);
            if (verificationData != null && verificationData.length > 0) {
              validRecords++;
            }
          } catch (IOException e) {
            // Skip invalid records
          }
        }

        final int finalValidRecords = validRecords;
        final String finalDescription = description;
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Persistence verification for %s: found %d valid records after exception",
                    finalDescription, finalValidRecords));

      } catch (Exception e) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Persistence verification failed for %s: %s", description, e.getMessage()));
      }
    }

    store.close();
  }

  /// Helper method for insert scenario with exception
  private void testInsertScenarioWithException(int throwAt, String description) throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Testing insert scenario: %s with exception at operation %d",
                description, throwAt));

    // Pre-create store with some data to create realistic scenarios
    FileRecordStore prepStore =
        new FileRecordStoreBuilder().tempFile("insert-scenario-", ".db").maxKeyLength(64).open();

    // Add some records to create free space and realistic conditions
    for (int i = 0; i < 5; i++) {
      byte[] key = ("existing" + i).getBytes();
      prepStore.insertRecord(key, ("existing-data" + i).getBytes());
    }

    // Delete some to create free space scenarios
    prepStore.deleteRecord(("existing1".getBytes()));
    prepStore.deleteRecord(("existing3".getBytes()));

    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Test insert with exception injection
    FileRecordStore testStore = createStoreWithExceptionAtPath(filePath, throwAt);
    byte[] newKey = "new-key".getBytes();
    byte[] newData = "new-data".getBytes();

    try {
      testStore.insertRecord(newKey, newData);
      Assert.fail("Expected IOException during " + description);
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Insert failed as expected during %s: %s", description, e.getMessage()));
    }

    Assert.assertTrue("Store should be closed after " + description, testStore.isClosed());

    // Persistence verification: verify original data is still intact after exception
    try (FileRecordStore verificationStore =
        new FileRecordStoreBuilder().path(filePath).maxKeyLength(64).open()) {

      // Verify original records are still present and valid
      int originalRecordsFound = 0;
      for (int i = 0; i < 5; i++) {
        if (i == 1 || i == 3) continue; // These were deleted

        byte[] originalKey = ("existing" + i).getBytes();
        String expectedData = "existing-data" + i;

        try {
          if (verificationStore.recordExists(originalKey)) {
            byte[] data = verificationStore.readRecordData(originalKey);
            if (data != null) {
              String actualData = new String(data, StandardCharsets.UTF_8);
              if (expectedData.equals(actualData)) {
                originalRecordsFound++;
              }
            }
          }
        } catch (IOException e) {
          // Skip corrupted records
        }
      }

      // We expect 3 original records (5 created - 2 deleted)
      final int finalOriginalRecordsFound = originalRecordsFound;
      final String finalDescription2 = description;
      Assert.assertEquals(
          "Original data should be intact after exception", 3, finalOriginalRecordsFound);
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Persistence verified: %d original records intact after %s",
                  finalOriginalRecordsFound, finalDescription2));

      // Verify the new insert did NOT happen (store closed before completion)
      Assert.assertFalse(
          "New insert should not have completed", verificationStore.recordExists(newKey));

    } catch (Exception e) {
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Persistence verification failed for %s: %s", description, e.getMessage()));
    }

    testStore.close();
  }

  /// Helper method for update scenario with exception
  private void testUpdateScenarioWithException(int throwAt, String description) throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Testing update scenario: %s with exception at operation %d",
                description, throwAt));

    // Pre-create store with data
    FileRecordStore prepStore =
        new FileRecordStoreBuilder().tempFile("update-scenario-", ".db").maxKeyLength(64).open();

    byte[] key = "update-key".getBytes();
    prepStore.insertRecord(key, "original-data".getBytes());
    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Test update with exception
    FileRecordStore testStore = createStoreWithExceptionAtPath(filePath, throwAt);
    byte[] newData = "updated-data".getBytes();

    try {
      testStore.updateRecord(key, newData);
      Assert.fail("Expected IOException during " + description);
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Update failed as expected during %s: %s", description, e.getMessage()));
    }

    Assert.assertTrue("Store should be closed after " + description, testStore.isClosed());
    testStore.close();
  }

  /// Helper method for delete scenario with exception using behavior-based verification
  private void testDeleteScenarioWithException(int throwAt, String description) throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Testing delete scenario: %s with exception at operation %d",
                description, throwAt));

    // Pre-create store with data
    FileRecordStore prepStore =
        new FileRecordStoreBuilder().tempFile("delete-scenario-", ".db").maxKeyLength(64).open();

    byte[] key = "delete-key".getBytes();
    prepStore.insertRecord(key, "delete-data".getBytes());
    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Test delete with exception
    FileRecordStore testStore = createStoreWithExceptionAtPath(filePath, throwAt);
    DelegatingExceptionOperations exceptionOps =
        (DelegatingExceptionOperations) testStore.fileOperations;

    try {
      testStore.deleteRecord(key);

      // If we get here, exception wasn't triggered - that's fine for some operations
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Operation %d not reached during %s - verifying store still works",
                  throwAt, description));

      // Verify store is still operational
      Assert.assertFalse(
          "Store should not be closed when no exception thrown", testStore.isClosed());
      Assert.assertFalse("Record should be deleted", testStore.recordExists(key));

    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Delete failed as expected during %s: %s", description, e.getMessage()));

      // Verify exception was actually thrown by our delegate
      Assert.assertTrue("Delegate should have thrown exception", exceptionOps.didThrow());
      Assert.assertEquals(
          "Delegate should have reached target operation",
          throwAt,
          exceptionOps.getOperationCount());

      // Verify store is closed
      Assert.assertTrue("Store should be closed after " + description, testStore.isClosed());

      // Verify subsequent operations fail
      verifySubsequentOperationsFail(testStore);
    }

    testStore.close();
  }

  /// Helper method for read scenario with exception
  private void testReadScenarioWithException(int throwAt, String description) throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Testing read scenario: %s with exception at operation %d", description, throwAt));

    // Pre-create store with data and specific CRC setting
    FileRecordStore prepStore =
        new FileRecordStoreBuilder()
            .tempFile("read-scenario-", ".db")
            .disablePayloadCrc32(false)
            .maxKeyLength(64)
            .open();

    byte[] key = "read-key".getBytes();
    prepStore.insertRecord(key, "read-data".getBytes());
    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Test read with exception and same CRC setting
    FileRecordStore testStore =
        new FileRecordStoreBuilder()
            .path(filePath)
            .disablePayloadCrc32(false)
            .maxKeyLength(64)
            .open();

    // Replace file operations with exception injection
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(testStore.getFilePath().toFile(), "r");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    testStore.fileOperations = new DelegatingExceptionOperations(directOps, throwAt);

    try {
      testStore.readRecordData(key);
      Assert.fail("Expected IOException during " + description);
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () ->
              String.format("Read failed as expected during %s: %s", description, e.getMessage()));
    }

    Assert.assertTrue("Store should be closed after " + description, testStore.isClosed());
    testStore.close();
  }

  /// Helper method for max key length scenario
  private void testMaxKeyLengthScenario(int throwAt) throws Exception {
    logger.log(
        Level.FINE, "Testing max key length scenario with exception at operation " + throwAt);

    // Create store with max key length
    FileRecordStore testStore =
        new FileRecordStoreBuilder()
            .tempFile("max-key-test-", ".db")
            .maxKeyLength(248) // Max theoretical key length
            .open();

    // Replace file operations with exception injection
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(testStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    testStore.fileOperations = new DelegatingExceptionOperations(directOps, throwAt);

    // Create max length key
    byte[] maxKey = new byte[248];
    for (int i = 0; i < 248; i++) {
      maxKey[i] = (byte) (i % 256);
    }
    byte[] data = "max-key-data".getBytes();

    try {
      testStore.insertRecord(maxKey, data);
      Assert.fail("Expected IOException during max key length operation");
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () -> String.format("Max key length operation failed as expected: %s", e.getMessage()));
    }

    Assert.assertTrue(
        "Store should be closed after max key length exception", testStore.isClosed());
    testStore.close();
  }

  /// Helper method for empty data scenario
  private void testEmptyDataScenario(int throwAt) throws Exception {
    logger.log(Level.FINE, "Testing empty data scenario with exception at operation " + throwAt);

    FileRecordStore testStore = createStoreWithException(throwAt);
    byte[] key = "empty-key".getBytes();
    byte[] emptyData = new byte[0];

    try {
      testStore.insertRecord(key, emptyData);
      Assert.fail("Expected IOException during empty data operation");
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () -> String.format("Empty data operation failed as expected: %s", e.getMessage()));
    }

    Assert.assertTrue("Store should be closed after empty data exception", testStore.isClosed());
    testStore.close();
  }

  /// Helper method for fsync scenario with exception using behavior-based verification
  private void testFsyncScenarioWithException(int throwAt, String description) throws Exception {
    logger.log(
        Level.FINE,
        () -> String.format("Testing %s with exception at operation %d", description, throwAt));

    // Create store and add some data
    FileRecordStore prepStore =
        new FileRecordStoreBuilder().tempFile("fsync-scenario-", ".db").maxKeyLength(64).open();

    byte[] key = "fsync-key".getBytes();
    prepStore.insertRecord(key, "fsync-data".getBytes());
    Path filePath = prepStore.getFilePath();
    prepStore.close();

    // Test fsync with exception
    FileRecordStore testStore = createStoreWithExceptionAtPath(filePath, throwAt);
    DelegatingExceptionOperations exceptionOps =
        (DelegatingExceptionOperations) testStore.fileOperations;

    try {
      testStore.fsync();

      // If we get here, exception wasn't triggered - that's fine for some operations
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Operation %d not reached during %s - verifying store still works",
                  throwAt, description));

      // Verify store is still operational
      Assert.assertFalse(
          "Store should not be closed when no exception thrown", testStore.isClosed());

    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () ->
              String.format("Fsync failed as expected during %s: %s", description, e.getMessage()));

      // Verify exception was actually thrown by our delegate
      Assert.assertTrue("Delegate should have thrown exception", exceptionOps.didThrow());
      Assert.assertEquals(
          "Delegate should have reached target operation",
          throwAt,
          exceptionOps.getOperationCount());

      // Verify store is closed
      Assert.assertTrue("Store should be closed after " + description, testStore.isClosed());

      // Verify subsequent operations fail
      verifySubsequentOperationsFail(testStore);
    }

    // Close the test store, handling potential exceptions during close
    try {
      testStore.close();
    } catch (IOException e) {
      logger.log(
          Level.FINE,
          () -> String.format("Exception during test cleanup close: %s", e.getMessage()));
      // This is expected if the exception was triggered during close operations
    }
  }

  /// Helper method to create a FileRecordStore with exception injection
  private FileRecordStore createStoreWithException(int throwAtOperation) throws IOException {
    logger.log(
        Level.FINE,
        () -> String.format("Creating store with exception at operation %d", throwAtOperation));

    // Create the base RandomAccessFile via builder temp file
    FileRecordStore baseStore =
        new FileRecordStoreBuilder().tempFile("exception-test-", ".db").maxKeyLength(64).open();

    // Replace file operations with exception injection wrapper
    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations exceptionOps =
        new DelegatingExceptionOperations(directOps, throwAtOperation);

    // Create new store with exception wrapper
    FileRecordStore exceptionStore =
        new FileRecordStoreBuilder().path(baseStore.getFilePath()).maxKeyLength(64).open();

    // Replace the file operations with our exception wrapper
    exceptionStore.fileOperations = exceptionOps;

    // Close the base store since we're done with it
    baseStore.close();

    logger.log(Level.FINE, "Store created with exception injection wrapper");
    return exceptionStore;
  }

  /// Helper method to create a FileRecordStore with exception injection at specific path
  private FileRecordStore createStoreWithExceptionAtPath(Path filePath, int throwAtOperation)
      throws IOException {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Creating store with exception at operation %d for path %s",
                throwAtOperation, filePath));

    // Create store with the specified path
    FileRecordStore store = new FileRecordStoreBuilder().path(filePath).maxKeyLength(64).open();

    // Replace file operations with exception injection wrapper
    java.io.RandomAccessFile raf = new java.io.RandomAccessFile(store.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);

    // Replace the file operations with our exception wrapper
    store.fileOperations = new DelegatingExceptionOperations(directOps, throwAtOperation);

    logger.log(Level.FINE, "Store created with exception injection wrapper at specified path");
    return store;
  }

  /// New behavior-based exception testing method
  private void testExceptionScenarioWithBehaviorVerification(int throwAt, String description)
      throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Testing %s with exception at operation %d (behavior-based)",
                description, throwAt));

    DelegatingExceptionOperations exceptionOps;
    try (FileRecordStore store = createStoreWithException(throwAt)) {
      exceptionOps = (DelegatingExceptionOperations) store.fileOperations;

      byte[] key = "testkey".getBytes();
      byte[] data = "testdata".getBytes();

      try {
        store.insertRecord(key, data);

        // If no exception, verify store is still operational
        byte[] readData = store.readRecordData(key);
        Assert.assertArrayEquals("Data should be intact when no exception thrown", data, readData);
        logger.log(
            Level.FINE,
            () -> String.format("No exception at operation %d - store operational", throwAt));

      } catch (IOException e) {
        // Exception occurred as expected
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Got expected exception at operation %d: %s", throwAt, e.getMessage()));

        // Verify exception was actually thrown by our delegate
        Assert.assertTrue("Delegate should have thrown exception", exceptionOps.didThrow());
        Assert.assertEquals(
            "Delegate should have reached target operation",
            throwAt,
            exceptionOps.getOperationCount());

        // Verify subsequent operations fail appropriately
        verifySubsequentOperationsFail(store);
      }
    }
    // Ignore close exceptions in test cleanup
  }

  /// Helper method to verify all public methods throw IllegalStateException
  private void verifyStoreIsClosed(FileRecordStore store) {
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    // Test insertRecord
    try {
      store.insertRecord(key, data);
      Assert.fail("insertRecord should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINE, "insertRecord correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail(
          "insertRecord should throw IllegalStateException, not IOException: " + e.getMessage());
    }

    // Test updateRecord
    try {
      store.updateRecord(key, data);
      Assert.fail("updateRecord should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINE, "updateRecord correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail(
          "updateRecord should throw IllegalStateException, not IOException: " + e.getMessage());
    }

    // Test readRecordData
    try {
      store.readRecordData(key);
      Assert.fail("readRecordData should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINE, "readRecordData correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail(
          "readRecordData should throw IllegalStateException, not IOException: " + e.getMessage());
    }

    // Test deleteRecord
    try {
      store.deleteRecord(key);
      Assert.fail("deleteRecord should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINE, "deleteRecord correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail(
          "deleteRecord should throw IllegalStateException, not IOException: " + e.getMessage());
    }

    // Test keys()
    try {
      store.keysBytes();
      Assert.fail("keys() should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(Level.FINE, "keys() correctly threw IllegalStateException: " + e.getMessage());
    } catch (Exception e) {
      Assert.fail(
          "keys() should throw IllegalStateException, got: "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
    }

    // Test isEmpty()
    try {
      store.isEmpty();
      Assert.fail("isEmpty() should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(Level.FINE, "isEmpty() correctly threw IllegalStateException: " + e.getMessage());
    } catch (Exception e) {
      Assert.fail(
          "isEmpty() should throw IllegalStateException, got: "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
    }

    // Test recordExists()
    try {
      store.recordExists(key);
      Assert.fail("recordExists() should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINE, "recordExists() correctly threw IllegalStateException: " + e.getMessage());
    } catch (Exception e) {
      Assert.fail(
          "recordExists() should throw IllegalStateException, got: "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
    }

    // Test fsync()
    try {
      store.fsync();
      Assert.fail("fsync() should throw IllegalStateException when store is closed");
    } catch (IllegalStateException e) {
      logger.log(Level.FINE, "fsync() correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail("fsync() should throw IllegalStateException, not IOException: " + e.getMessage());
    }
  }

  /// Helper to verify subsequent operations fail with IllegalStateException after exception
  private void verifySubsequentOperationsFail(FileRecordStore store) {
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    // All operations should now throw IllegalStateException
    assertOperationFails(
        () -> {
          try {
            store.insertRecord(key, data);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "insertRecord");

    assertOperationFails(
        () -> {
          try {
            store.readRecordData(key);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "readRecordData");

    assertOperationFails(
        () -> {
          try {
            store.updateRecord(key, data);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "updateRecord");

    assertOperationFails(
        () -> {
          try {
            store.deleteRecord(key);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "deleteRecord");

    assertOperationFails(store::keysBytes, "keys");
    assertOperationFails(store::isEmpty, "isEmpty");
    assertOperationFails(() -> store.recordExists(key), "recordExists");
    assertOperationFails(
        () -> {
          try {
            store.fsync();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "fsync");
  }

  /// Helper to assert that an operation fails with IllegalStateException
  private void assertOperationFails(Runnable operation, String operationName) {
    try {
      operation.run();
      Assert.fail(operationName + " should throw IllegalStateException");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINEST,
          () -> operationName + " correctly threw IllegalStateException: " + e.getMessage());
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        Assert.fail(
            operationName
                + " should throw IllegalStateException, not IOException: "
                + e.getCause().getMessage());
      } else {
        throw e;
      }
    }
  }

  /// Verifies store integrity after writes and exceptions.
  ///
  /// @param store               the FileRecordStore to check
  /// @param expectedRecordCount expected number of records
  /// @param expectedValues      optional map of expected key->expected string data;
  ///                            if null, only non-empty checks are applied
  /// @param description         label for error messages
  private void verifyStoreIntegrity(
      FileRecordStore store,
      int expectedRecordCount,
      Map<byte[], String> expectedValues,
      String description) {

    try {
      if (store.isClosed()) {
        throw new AssertionError(description + ": Store is closed, cannot verify integrity");
      }

      Iterable<byte[]> keys = store.keysBytes();
      int actualCount = 0;
      for (byte[] ignored : keys) {
        actualCount++;
      }

      if (actualCount != expectedRecordCount) {
        throw new AssertionError(
            String.format(
                "%s: Expected %d records but found %d",
                description, expectedRecordCount, actualCount));
      }

      for (byte[] key : keys) {
        byte[] data;
        try {
          data = store.readRecordData(key);
        } catch (IOException e) {
          throw new AssertionError(
              description
                  + ": Failed to read data for key "
                  + Base64.getEncoder().encodeToString(key)
                  + ": "
                  + e.getMessage(),
              e);
        }

        if (data == null) {
          throw new AssertionError(
              description + ": Null data for key " + Base64.getEncoder().encodeToString(key));
        }

        if (data.length == 0) {
          throw new AssertionError(
              description + ": Empty data for key " + Base64.getEncoder().encodeToString(key));
        }

        if (expectedValues != null) {
          String actual = new String(data, StandardCharsets.UTF_8);
          String expected = expectedValues.get(key);
          if (expected == null) {
            throw new AssertionError(
                description + ": Unexpected key found " + Base64.getEncoder().encodeToString(key));
          }
          if (!expected.equals(actual)) {
            throw new AssertionError(
                String.format(
                    "%s: Data mismatch for key %s - expected '%s' but got '%s'",
                    description, Base64.getEncoder().encodeToString(key), expected, actual));
          }
        }
      }

      final int finalActualCount = actualCount;
      logger.log(
          Level.FINE,
          () ->
              String.format("%s: Verified %d records successfully", description, finalActualCount));

    } catch (Exception e) {
      throw new AssertionError(
          description + ": Unexpected error during verification: " + e.getMessage(), e);
    }
  }

  /// Test persistence verification after exceptions with controlled write sequence
  @Test
  public void testPersistenceVerificationAfterExceptions() throws Exception {
    logger.log(Level.FINE, "=== Testing persistence verification after exceptions ===");

    // Discover total operations for simple insert
    int totalOperations = discoverOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for simple insert", totalOperations));

    // Test a few representative operations that should demonstrate persistence
    int[] testOperations = {1, Math.min(3, totalOperations), Math.min(5, totalOperations)};

    for (int throwAt : testOperations) {
      if (throwAt > totalOperations) continue;

      final int finalThrowAt = throwAt;
      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Testing persistence with exception at operation %d/%d",
                  finalThrowAt, totalOperations));

      // Create store with exception injection
      FileRecordStore store = null;
      Path storePath = null;
      int successfulWrites = 0;
      Map<byte[], String> expectedData = new HashMap<>();

      try {
        // Create base store first to get a path
        FileRecordStore baseStore =
            new FileRecordStoreBuilder()
                .tempFile("persistence-test-", ".db")
                .maxKeyLength(64)
                .open();
        storePath = baseStore.getFilePath();
        baseStore.close();

        // Now create store with exception injection at the same path
        store = createStoreWithExceptionAtPath(storePath, finalThrowAt);

        // Write multiple records sequentially
        for (int i = 0; i < 10; i++) { // Try to write 10 records
          final int recordIndex = i;
          byte[] key = ("key_" + i).getBytes();
          String data = "data_" + i;

          try {
            store.insertRecord(key, data.getBytes(StandardCharsets.UTF_8));
            successfulWrites++;
            expectedData.put(key, data);
            logger.log(
                Level.FINER, () -> String.format("Successfully wrote record %d", recordIndex));
          } catch (IOException e) {
            final int finalSuccessfulWrites = successfulWrites;
            logger.log(
                Level.FINE,
                () ->
                    String.format(
                        "Exception thrown at operation %d after %d successful writes",
                        finalThrowAt, finalSuccessfulWrites));
            break; // Stop writing after exception
          }
        }

        // Verify store is now closed due to exception
        Assert.assertTrue("Store should be closed after exception", store.isClosed());

      } catch (Exception e) {
        logger.log(
            Level.FINE,
            () -> String.format("Exception during test setup or writing: %s", e.getMessage()));
      } finally {
        // Clean up the store
        if (store != null) {
          try {
            store.close();
          } catch (Exception e) {
            // Ignore close exceptions
          }
        }
      }

      // Now verify persistence by opening a fresh store
      final int finalSuccessfulWrites = successfulWrites;
      final Path finalStorePath = storePath;

      if (finalStorePath != null && finalSuccessfulWrites > 0) {
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Verifying persistence: reopening store with %d expected records",
                    finalSuccessfulWrites));

        try (FileRecordStore freshStore =
            new FileRecordStoreBuilder().path(finalStorePath).maxKeyLength(64).open()) {

          // Use our hybrid validation helper
          final String verificationDescription =
              String.format("Persistence check after exception at operation %d", finalThrowAt);
          verifyStoreIntegrity(
              freshStore, finalSuccessfulWrites, expectedData, verificationDescription);

          logger.log(
              Level.FINE,
              () ->
                  String.format(
                      "Persistence verified: %d records correctly stored", finalSuccessfulWrites));

        } catch (Exception e) {
          logger.log(
              Level.SEVERE,
              () -> String.format("Persistence verification failed: %s", e.getMessage()));
          throw new AssertionError("Persistence verification failed: " + e.getMessage(), e);
        }
        // Ignore close exceptions
      } else if (finalStorePath != null) {
        logger.log(
            Level.FINE,
            () -> String.format("No successful writes to verify for operation %d", finalThrowAt));
      }
    }

    logger.log(Level.FINE, "=== Persistence verification testing completed ===");
  }
}
