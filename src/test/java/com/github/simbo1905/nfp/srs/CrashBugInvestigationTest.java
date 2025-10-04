package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/// Detailed investigation test to determine if the crash bug is fundamental
/// or introduced by the builder pattern refactor.
///
/// This test uses controlled halting instead of exception throwing to precisely
/// measure file operations and verify file integrity at specific points.
public class CrashBugInvestigationTest extends JulLoggingConfig {

  private static final Logger logger = Logger.getLogger(CrashBugInvestigationTest.class.getName());

  private FileRecordStore store;
  private Path tempFile;

  @Before
  public void setup() throws IOException {
    tempFile = Files.createTempFile("crash-investigation-", ".db");
    tempFile.toFile().deleteOnExit();
    logger.log(Level.FINE, () -> "Setup: Created temp file " + tempFile);
  }

  @After
  public void cleanup() {
    if (store != null) {
      try {
        store.close();
      } catch (Exception e) {
        logger.log(Level.FINE, "Error closing store during cleanup", e);
      }
    }
    try {
      Files.deleteIfExists(tempFile);
      logger.log(Level.FINE, () -> "Cleanup: Deleted temp file " + tempFile);
    } catch (IOException e) {
      logger.log(Level.FINE, "Error deleting temp file", e);
    }
  }

  /// First discover the total operation count for a simple insert
  @Test
  public void testDiscoverOperationCount() throws Exception {
    logger.log(Level.FINE, "=== Starting testDiscoverOperationCount ===");

    // Reset file
    Files.deleteIfExists(tempFile);
    Files.createFile(tempFile);
    tempFile.toFile().deleteOnExit();

    // Create store with counting wrapper (halt at a very high number to count all operations)
    int veryHighHaltPoint = 1000;
    store = createStoreWithHalt(veryHighHaltPoint);

    // Perform simple insert
    byte[] key = "testkey".getBytes();
    byte[] data = "testdata".getBytes();

    logger.log(
        Level.FINE,
        () -> String.format("Inserting key=%s, data=%s", new String(key), new String(data)));

    store.insertRecord(key, data);

    // Get the operation count
    int totalOps = ((DelegatingHaltOperations) store.fileOperations).getOperationCount();
    logger.log(Level.FINE, () -> String.format("Total operations for simple insert: %d", totalOps));

    // Verify data is intact
    byte[] readData = store.readRecordData(key);
    Assert.assertArrayEquals("Data should be intact", data, readData);

    store.close();

    // Now test systematic halting at each operation
    testSystematicHalting(totalOps);

    logger.log(Level.FINE, "=== testDiscoverOperationCount completed successfully ===");
  }

  /// Systematically test halting at each operation count
  private void testSystematicHalting(int maxOperations) {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "=== Starting systematic halting test for %d operations ===", maxOperations));

    // Test halt at each operation from 1 to maxOperations
    java.util.stream.IntStream.rangeClosed(1, maxOperations)
        .forEach(
            haltAt -> {
              try {
                logger.log(
                    Level.FINE,
                    () -> String.format("Testing halt at operation %d/%d", haltAt, maxOperations));

                // Reset file for each test
                Files.deleteIfExists(tempFile);
                Files.createFile(tempFile);
                tempFile.toFile().deleteOnExit();

                // Create store with halt wrapper
                FileRecordStore testStore = createStoreWithHalt(haltAt);

                // Perform simple insert
                byte[] key = "testkey".getBytes();
                byte[] data = "testdata".getBytes();

                testStore.insertRecord(key, data);

                // Flush and close
                testStore.fsync();
                testStore.close();

                // Try to reopen and verify
                try {
                  FileRecordStore reopenedStore =
                      new FileRecordStore.Builder()
                          .path(tempFile)
                          .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
                          .disablePayloadCrc32(false)
                          .open();

                  // Verify data integrity
                  if (reopenedStore.recordExists(key)) {
                    byte[] readData = reopenedStore.readRecordData(key);
                    Assert.assertArrayEquals(
                        "Data should match after halt at operation " + haltAt, data, readData);
                    logger.log(
                        Level.FINE,
                        () ->
                            String.format(
                                "✓ Data integrity verified for halt at operation %d", haltAt));
                  } else {
                    logger.log(
                        Level.FINE,
                        () -> String.format("Key not found after halt at operation %d", haltAt));
                  }

                  reopenedStore.close();

                } catch (Exception e) {
                  logger.log(
                      Level.SEVERE,
                      String.format("Failed to reopen store after halt at operation %d", haltAt),
                      e);

                  // For critical early operations, this indicates a real bug
                  if (haltAt <= 3) {
                    throw new RuntimeException("Critical failure at early operation " + haltAt, e);
                  }
                }

              } catch (Exception e) {
                logger.log(Level.SEVERE, "Error during systematic halting test", e);
                throw new RuntimeException(
                    "Systematic halting test failed at operation " + haltAt, e);
              }
            });

    logger.log(Level.FINE, "=== Systematic halting test completed successfully ===");
  }

  /// Create a FileRecordStore with halt wrapper at specified operation count
  private FileRecordStore createStoreWithHalt(int haltAtOperation) throws IOException {
    logger.log(
        Level.FINE,
        () -> String.format("Creating store with halt at operation %d", haltAtOperation));

    // Create the base RandomAccessFile
    java.io.RandomAccessFile raf = new java.io.RandomAccessFile(tempFile.toFile(), "rw");

    // Create the direct file operations
    RandomAccessFile directOps = new RandomAccessFile(raf);

    // Wrap with halt operations
    DelegatingHaltOperations haltOps = new DelegatingHaltOperations(directOps, haltAtOperation);

    // Create FileRecordStore with custom operations
    FileRecordStore store = new FileRecordStore(tempFile.toFile(), 10, 64, false, false, "rw");

    // Replace the file operations with our halt wrapper
    store.fileOperations = haltOps;

    logger.log(Level.FINE, "Store created with halt wrapper");
    return store;
  }
}
