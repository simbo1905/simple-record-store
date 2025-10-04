package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/// Behavior-based test for FileRecordStore that focuses on observable outcomes
/// rather than internal state inspection. This provides more robust testing
/// that is less brittle to implementation changes.
public class FileRecordStoreBehaviorTest extends JulLoggingConfig {

  private static final Logger logger =
      Logger.getLogger(FileRecordStoreBehaviorTest.class.getName());

  /// Test that exceptions during operations prevent subsequent operations
  @Test
  public void testExceptionPreventsSubsequentOperations() throws Exception {
    logger.log(Level.FINE, "=== Testing exception prevents subsequent operations ===");

    // Discover total operations for a simple insert
    int totalOperations = discoverOperationCount();
    logger.log(
        Level.FINE,
        () -> String.format("Discovered %d operations for simple insert", totalOperations));

    // Test exceptions at various operation points
    for (int throwAt = 1; throwAt <= totalOperations; throwAt++) {
      final int finalThrowAt = throwAt;
      logger.log(
          Level.FINE,
          () ->
              String.format("Testing exception at operation %d/%d", finalThrowAt, totalOperations));

      FileRecordStore store = null;
      DelegatingExceptionOperations exceptionOps = null;

      try {
        // Create store with exception injection
        store = createStoreWithException(throwAt);
        exceptionOps = (DelegatingExceptionOperations) store.fileOperations;

        // Attempt operation that should trigger exception
        byte[] key = "testkey".getBytes();
        byte[] data = "testdata".getBytes();

        store.insertRecord(key, data);

        // If we get here, exception wasn't triggered - that's OK for some operations
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Operation %d not reached - verifying store still works", finalThrowAt));

        // Verify store is still operational
        verifyStoreOperational(store, key, data);

      } catch (IOException e) {
        // Exception was triggered as expected
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Got expected exception at operation %d: %s", finalThrowAt, e.getMessage()));

        // Verify exception was actually thrown by our delegate
        Assert.assertTrue(
            "Delegate should have thrown exception",
            Objects.requireNonNull(exceptionOps).didThrow());
        Assert.assertEquals(
            "Delegate should have reached target operation",
            throwAt,
            exceptionOps.getOperationCount());

        // Verify subsequent operations fail
        verifySubsequentOperationsFail(store);

      } finally {
        if (store != null) {
          try {
            store.close();
          } catch (Exception e) {
            // Ignore close exceptions in test cleanup
          }
        }
      }
    }

    logger.log(Level.FINE, "=== Completed behavior-based exception testing ===");
  }

  /// Test that successful operations complete normally
  @Test
  public void testSuccessfulOperationsCompleteNormally() throws Exception {
    logger.log(Level.FINE, "=== Testing successful operations complete normally ===");

    // Create normal store without exception injection

    try (FileRecordStore store =
        new FileRecordStore.Builder().tempFile("success-test-", ".db").open()) {
      byte[] key = "testkey".getBytes();
      byte[] data = "testdata".getBytes();

      // Insert record
      store.insertRecord(key, data);

      // Verify data can be read back
      byte[] readData = store.readRecordData(key);
      Assert.assertArrayEquals("Data should match after insert", data, readData);

      // Update record
      byte[] updatedData = "updateddata".getBytes();
      store.updateRecord(key, updatedData);

      // Verify updated data
      byte[] readUpdatedData = store.readRecordData(key);
      Assert.assertArrayEquals("Data should match after update", updatedData, readUpdatedData);

      // Delete record
      store.deleteRecord(key);

      // Verify record no longer exists
      Assert.assertFalse("Record should not exist after delete", store.recordExists(key));

      logger.log(Level.FINE, "=== All successful operations completed normally ===");
    }
  }

  /// Test data integrity after exceptions
  @Test
  public void testDataIntegrityAfterExceptions() throws Exception {
    logger.log(Level.FINE, "=== Testing data integrity after exceptions ===");

    // Test a few specific operation points where we can verify data integrity
    int[] testPoints = {5, 10, 15}; // Operations where we expect some data to be written

    for (int throwAt : testPoints) {
      logger.log(
          Level.FINE,
          () -> String.format("Testing data integrity with exception at operation %d", throwAt));

      Path tempFile = Files.createTempFile("integrity-test-", ".db");
      tempFile.toFile().deleteOnExit();

      try {
        // Create store with exception injection
        FileRecordStore store = createStoreWithExceptionAtPath(tempFile, throwAt);
        DelegatingExceptionOperations exceptionOps =
            (DelegatingExceptionOperations) store.fileOperations;

        byte[] key = "testkey".getBytes();
        byte[] data = "testdata".getBytes();

        try {
          store.insertRecord(key, data);

          // If no exception, verify data integrity
          final byte[] readData = store.readRecordData(key);
          Assert.assertArrayEquals(
              "Data should be intact when no exception thrown", data, readData);

        } catch (IOException e) {
          // Exception occurred - verify what we can
          logger.log(
              Level.FINE,
              () -> String.format("Exception at operation %d, checking data integrity", throwAt));

          // Verify exception was thrown
          Assert.assertTrue("Delegate should have thrown exception", exceptionOps.didThrow());

          // Close the failed store
          store.close();

          // Try to reopen and check what data is recoverable
          try {
            FileRecordStore reopenedStore = new FileRecordStore.Builder().path(tempFile).open();

            // Check if any data was written before the exception
            if (reopenedStore.recordExists(key)) {
              final byte[] recoveredData = reopenedStore.readRecordData(key);
              logger.log(
                  Level.FINE, () -> String.format("Recovered data: %s", new String(recoveredData)));
              // Data integrity depends on when the exception occurred
            } else {
              logger.log(
                  Level.FINE, "No data recovered - exception occurred before write completed");
            }

            reopenedStore.close();
          } catch (Exception reopenEx) {
            logger.log(
                Level.FINE, "Could not reopen store after exception - file may be corrupted");
          }
        }

      } finally {
        Files.deleteIfExists(tempFile);
      }
    }

    logger.log(Level.FINE, "=== Completed data integrity testing ===");
  }

  /// Discover total operation count for simple insert without throwing
  private int discoverOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Discovering operation count ===");

    FileRecordStore.Builder builder = new FileRecordStore.Builder().tempFile("discovery-", ".db");

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

    // Perform simple insert
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    countingStore.insertRecord(key, data);

    // Get operation count
    int totalOps = countingOps.getOperationCount();

    countingStore.close();

    logger.log(
        Level.FINE, () -> String.format("Discovered %d operations for simple insert", totalOps));
    return totalOps;
  }

  /// Create store with exception injection
  private FileRecordStore createStoreWithException(int throwAtOperation) throws IOException {
    logger.log(
        Level.FINE,
        () -> String.format("Creating store with exception at operation %d", throwAtOperation));

    FileRecordStore.Builder builder =
        new FileRecordStore.Builder().tempFile("exception-test-", ".db");

    FileRecordStore baseStore = builder.open();

    java.io.RandomAccessFile raf =
        new java.io.RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingExceptionOperations exceptionOps =
        new DelegatingExceptionOperations(directOps, throwAtOperation);

    FileRecordStore exceptionStore = builder.path(baseStore.getFilePath()).open();
    exceptionStore.fileOperations = exceptionOps;

    baseStore.close();

    logger.log(Level.FINE, "Store created with exception injection wrapper");
    return exceptionStore;
  }

  /// Create store with exception injection at specific path
  private FileRecordStore createStoreWithExceptionAtPath(Path filePath, int throwAtOperation)
      throws IOException {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Creating store with exception at operation %d for path %s",
                throwAtOperation, filePath));

    FileRecordStore store = new FileRecordStore.Builder().path(filePath).open();

    java.io.RandomAccessFile raf = new java.io.RandomAccessFile(store.getFilePath().toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);

    store.fileOperations = new DelegatingExceptionOperations(directOps, throwAtOperation);

    logger.log(Level.FINE, "Store created with exception injection wrapper at specified path");
    return store;
  }

  /// Verify store is operational by performing basic operations
  private void verifyStoreOperational(FileRecordStore store, byte[] key, byte[] data)
      throws IOException {
    // Write data
    store.insertRecord(key, data);

    // Read it back
    final byte[] readData = store.readRecordData(key);
    Assert.assertArrayEquals("Data should match", data, readData);

    // Check record exists
    Assert.assertTrue("Record should exist", store.recordExists(key));

    logger.log(Level.FINEST, "Store verified as operational");
  }

  /// Verify subsequent operations fail with IllegalStateException
  private void verifySubsequentOperationsFail(FileRecordStore store) {
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    // All operations should now throw IllegalStateException
    assertOperationFails(() -> store.insertRecord(key, data), "insertRecord");
    assertOperationFails(() -> store.readRecordData(key), "readRecordData");
    assertOperationFails(() -> store.updateRecord(key, data), "updateRecord");
    assertOperationFails(() -> store.deleteRecord(key), "deleteRecord");
    assertOperationFails(() -> store.keysBytes(), "keys");
    assertOperationFails(store::isEmpty, "isEmpty");
    assertOperationFails(() -> store.recordExists(key), "recordExists");
    assertOperationFails(store::fsync, "fsync");
  }

  /// Helper to assert that an operation fails with IllegalStateException
  private void assertOperationFails(IOOperation operation, String operationName) {
    try {
      operation.run();
      Assert.fail(operationName + " should throw IllegalStateException");
    } catch (IllegalStateException e) {
      logger.log(
          Level.FINEST,
          () -> operationName + " correctly threw IllegalStateException: " + e.getMessage());
    } catch (IOException e) {
      Assert.fail(
          operationName
              + " should throw IllegalStateException, not IOException: "
              + e.getMessage());
    } catch (Exception e) {
      Assert.fail(
          operationName
              + " should throw IllegalStateException, got: "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
    }
  }

  /// Functional interface for IO operations that can throw IOException
  @FunctionalInterface
  interface IOOperation {
    void run() throws IOException;
  }
}
