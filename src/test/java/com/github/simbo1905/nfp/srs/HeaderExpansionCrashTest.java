package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/// Critical missing test coverage: crash safety during header expansion
/// Tests crashes at various points during record relocation to verify recovery
///
/// DESIGN NOTE: This test models JVM termination (real crash), not gradual shutdown.
/// When operations are halted mid-transaction, the store instance enters UNKNOWN state
/// (expected behavior - instance is "zombie"). The test validates that:
/// 1. Disk state remains consistent (atomic writes completed or not)
/// 2. Fresh JVM can reopen the file successfully
/// 3. Committed data is intact after reopen
///
/// State consistency errors in the halted instance are EXPECTED and CORRECT - they
/// indicate the in-memory state is corrupt (which is fine, JVM would terminate).
/// The test proves crash safety by showing reopen succeeds with consistent disk state.
public class HeaderExpansionCrashTest extends JulLoggingConfig {

  private Path tempFile;

  @Before
  public void setup() throws IOException {
    tempFile = Files.createTempFile("header-expansion-crash-", ".db");
    tempFile.toFile().deleteOnExit();
    logger.log(Level.FINE, () -> "Setup: Created temp file " + tempFile);
  }

  @After
  public void cleanup() {
    try {
      Files.deleteIfExists(tempFile);
      logger.log(Level.FINE, () -> "Cleanup: Deleted temp file " + tempFile);
    } catch (IOException e) {
      logger.log(Level.FINE, "Error deleting temp file", e);
    }
  }

  /// Test crash during header expansion - discovers operation count then tests systematic halts
  @Test
  public void testCrashDuringHeaderExpansion() throws Exception {
    logger.log(Level.FINE, "=== Testing crash safety during header expansion ===");

    // Phase 1: Discover operation count for expansion scenario
    Files.deleteIfExists(tempFile);
    Files.createFile(tempFile);

    FileRecordStore discoveryStore = createStoreWithHalt(10000); // High count to avoid halt

    // Fill pre-allocated space (2 records)
    byte[] key1 = createKey(1);
    byte[] key2 = createKey(2);
    byte[] data = createData(100);

    discoveryStore.insertRecord(key1, data);
    discoveryStore.insertRecord(key2, data);

    // Reset operation counter
    if (discoveryStore.fileOperations instanceof DelegatingHaltOperations haltOps) {
      int opsBeforeExpansion = haltOps.getOperationCount();
      logger.log(
          Level.FINE, () -> String.format("Operations before expansion: %d", opsBeforeExpansion));
    }

    // Trigger expansion with 3rd record
    byte[] key3 = createKey(3);
    discoveryStore.insertRecord(key3, data);

    int totalOps = 0;
    if (discoveryStore.fileOperations instanceof DelegatingHaltOperations haltOps) {
      totalOps = haltOps.getOperationCount();
      final int finalTotalOps = totalOps;
      logger.log(
          Level.FINE, () -> String.format("Total operations with expansion: %d", finalTotalOps));
    }

    discoveryStore.close();

    // Phase 2: Systematic halting at each operation
    if (totalOps > 0) {
      testSystematicHaltingDuringExpansion(totalOps);
    }

    logger.log(Level.FINE, "=== Crash during header expansion test completed ===");
  }

  /// Test systematic halting at each operation during expansion
  ///
  /// NOTE: You will see SEVERE "State consistency error" logs during this test.
  /// These are EXPECTED and CORRECT - they indicate zombie instances detecting
  /// in-memory corruption (which is fine, real crash would terminate JVM).
  /// The test PASSES if reopen succeeds, proving disk state is consistent.
  private void testSystematicHaltingDuringExpansion(int maxOperations) throws Exception {
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "=== Testing systematic halting during expansion (%d operations) ===",
                maxOperations));

    byte[] key1 = createKey(1);
    byte[] key2 = createKey(2);
    byte[] key3 = createKey(3);
    byte[] data = createData(100);

    // Create file once at start of test
    Files.deleteIfExists(tempFile);
    Files.createFile(tempFile);

    // Test halting at each operation
    // IMPORTANT: File persists across iterations to test recovery from partial writes
    for (int haltAt = 1; haltAt <= maxOperations; haltAt++) {
      final int currentHalt = haltAt;

      try {
        FileRecordStore testStore = createStoreWithHalt(haltAt);

        try {
          // Fill space then trigger expansion
          testStore.insertRecord(key1, data);
          testStore.insertRecord(key2, data);
          testStore.insertRecord(key3, data); // Triggers expansion
        } catch (Exception e) {
          // Expected: operation may halt or throw
          // Instance may now be in UNKNOWN state (zombie) - this is correct behavior
          logger.log(
              Level.FINEST,
              () -> String.format("Expected halt/exception at operation %d", currentHalt));
        }

        // CRITICAL: Do NOT use close() - it may flush buffers and alter state
        testStore.terminate();

        if (testStore.getState() != FileRecordStore.StoreState.UNKNOWN) {
          throw new AssertionError("Illegal state of test store it must be marked as unusable.");
        }

        // VALIDATION: Fresh JVM reopen (models crash recovery)
        // This is the ONLY meaningful crash safety check
        try {
          FileRecordStore reopened =
              new FileRecordStoreBuilder()
                  .path(tempFile)
                  .preallocatedRecords(2)
                  .maxKeyLength(64)
                  .useMemoryMapping(false)
                  .open();

          logger.log(
              Level.FINEST,
              () ->
                  String.format(
                      "✓ Reopen successful after halt at op %d, records=%d",
                      currentHalt, reopened.getNumRecords()));

          // Verify data integrity for committed records ONLY
          // Uncommitted records (e.g., key3 if insert interrupted) should not exist
          if (reopened.recordExists(key1)) {
            Assert.assertArrayEquals(
                "Key1 data intact after crash at op " + currentHalt,
                data,
                reopened.readRecordData(key1));
          }

          if (reopened.recordExists(key2)) {
            Assert.assertArrayEquals(
                "Key2 data intact after crash at op " + currentHalt,
                data,
                reopened.readRecordData(key2));
          }

          // Note: key3 may or may not exist depending on halt point
          // If it exists, it must be intact (atomic commit)
          if (reopened.recordExists(key3)) {
            Assert.assertArrayEquals(
                "Key3 data intact if committed at op " + currentHalt,
                data,
                reopened.readRecordData(key3));
          }

          reopened.close();

        } catch (Exception e) {
          Assert.fail("Failed to reopen store after halt at operation " + currentHalt + ": " + e);
        }

      } catch (Exception e) {
        logger.log(Level.SEVERE, "Error during systematic halting at operation " + currentHalt, e);
        throw new RuntimeException("Test failed at operation " + currentHalt, e);
      }
    }

    logger.log(Level.FINE, "=== Systematic halting test completed successfully ===");
  }

  /// Create FileRecordStore with halt operations wrapper
  private FileRecordStore createStoreWithHalt(int haltAtOperation) throws IOException {
    java.io.RandomAccessFile raf = new java.io.RandomAccessFile(tempFile.toFile(), "rw");
    RandomAccessFile directOps = new RandomAccessFile(raf);
    DelegatingHaltOperations haltOps = new DelegatingHaltOperations(directOps, haltAtOperation);

    FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(tempFile)
            .preallocatedRecords(2) // Force expansion on 3rd record
            .maxKeyLength(64)
            .disablePayloadCrc32(false)
            .useMemoryMapping(false)
            .accessMode(FileRecordStoreBuilder.AccessMode.READ_WRITE)
            .open();

    store.fileOperations = haltOps;
    return store;
  }

  /// Helper to create test keys
  private byte[] createKey(int id) {
    byte[] key = new byte[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (byte) (id + i);
    }
    return key;
  }

  /// Helper to create test data
  private byte[] createData(@SuppressWarnings("SameParameterValue") int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (i % 256);
    }
    return data;
  }
}
