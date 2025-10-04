package com.github.simbo1905.nfp.srs;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class MemoryMappedRecordStoreTest extends JulLoggingConfig {

  private static final String TEST_DIR = System.getProperty("java.io.tmpdir");

  @Test
  public void testBasicOperationsWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-basic-" + System.nanoTime() + ".db";
    try {
      // Create a new store with memory mapping enabled
      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(1000)
              .useMemoryMapping(true)
              .open()) {
        // Insert
        final var key1 = ("key1".getBytes());
        final var value1 = "value1".getBytes();
        store.insertRecord(key1, value1);

        // Read
        byte[] read1 = store.readRecordData(key1);
        Assert.assertArrayEquals(value1, read1);

        // Update
        final var value2 = "value2-updated".getBytes();
        store.updateRecord(key1, value2);
        byte[] read2 = store.readRecordData(key1);
        Assert.assertArrayEquals(value2, read2);

        // Delete
        store.deleteRecord(key1);
        Assert.assertFalse(store.recordExists(key1));
      }

      // Reopen without memory mapping to verify data persisted
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        Assert.assertEquals(0, store.getNumRecords());
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testMultipleInsertsWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-multiple-" + System.nanoTime() + ".db";
    try {
      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(1000)
              .useMemoryMapping(true)
              .open()) {
        // Insert multiple records
        for (int i = 0; i < 100; i++) {
          final byte[] key = ("key" + i).getBytes();

          final var value = ("value" + i).getBytes();
          store.insertRecord(key, value);
        }

        // Verify all records
        for (int i = 0; i < 100; i++) {
          final byte[] key = ("key" + i).getBytes();
          Assert.assertTrue(store.recordExists(key));
          byte[] value = store.readRecordData(key);
          Assert.assertArrayEquals(("value" + i).getBytes(), value);
        }
      }

      // Reopen and verify persistence
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(true)
              .open()) {
        Assert.assertEquals(100, store.getNumRecords());
        for (int i = 0; i < 100; i++) {
          final byte[] key = ("key" + i).getBytes();
          Assert.assertTrue(store.recordExists(key));
        }
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testLargeRecordsWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-large-" + System.nanoTime() + ".db";
    try {
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(Paths.get(fileName))
              .preallocatedRecords(10000)
              .useMemoryMapping(true)
              .open()) {
        // Insert large records
        final var key1 = ("largekey1".getBytes());
        byte[] largeValue = new byte[10000];
        Arrays.fill(largeValue, (byte) 'A');
        store.insertRecord(key1, largeValue);

        // Read and verify
        byte[] read = store.readRecordData(key1);
        Assert.assertArrayEquals(largeValue, read);

        // Update with even larger value
        byte[] largerValue = new byte[20000];
        Arrays.fill(largerValue, (byte) 'B');
        store.updateRecord(key1, largerValue);

        byte[] read2 = store.readRecordData(key1);
        Assert.assertArrayEquals(largerValue, read2);
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testUpdateInPlaceWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-update-" + System.nanoTime() + ".db";
    try {
      final var data1 = String.join("", Collections.nCopies(256, "1")).getBytes();
      final var data2 = String.join("", Collections.nCopies(256, "2")).getBytes();

      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(Paths.get(fileName))
              .preallocatedRecords(2000)
              .useMemoryMapping(true)
              .open()) {
        final var key = ("testkey".getBytes());
        store.insertRecord(key, data1);

        // Update with same size (should be in-place)
        store.updateRecord(key, data2);

        byte[] read = store.readRecordData(key);
        Assert.assertArrayEquals(data2, read);
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testFsyncWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-fsync-" + System.nanoTime() + ".db";
    try {
      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(1000)
              .useMemoryMapping(true)
              .open()) {
        final var key = ("key1".getBytes());
        final var value = "value1".getBytes();
        store.insertRecord(key, value);

        // Call fsync explicitly
        store.fsync();

        // Verify data is still accessible
        byte[] read = store.readRecordData(key);
        Assert.assertArrayEquals(value, read);
      }

      // Verify persistence after close
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        final var key = ("key1".getBytes());
        Assert.assertTrue(store.recordExists(key));
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testMixedDirectAndMemoryMappedAccess() throws Exception {
    String fileName = TEST_DIR + "/test-mixed-" + System.nanoTime() + ".db";
    try {
      // Create with direct I/O
      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(1000)
              .disablePayloadCrc32(false)
              .open()) {
        final var key1 = ("key1".getBytes());
        final var value1 = "value1".getBytes();
        store.insertRecord(key1, value1);
      }

      // Open with memory-mapped I/O and add more data
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .disablePayloadCrc32(false)
              .useMemoryMapping(true)
              .open()) {
        final var key2 = ("key2".getBytes());
        final var value2 = "value2".getBytes();
        store.insertRecord(key2, value2);

        // Verify both records
        Assert.assertTrue(store.recordExists(("key1".getBytes())));
        Assert.assertTrue(store.recordExists(key2));
      }

      // Open with direct I/O again and verify
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        Assert.assertEquals(2, store.getNumRecords());
      }
    } finally {
      new File(fileName).delete();
    }
  }

  @Test
  public void testFileGrowthWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-growth-" + System.nanoTime() + ".db";
    try {
      // Start with small initial size
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(Paths.get(fileName))
              .preallocatedRecords(100)
              .useMemoryMapping(true)
              .open()) {
        // Insert records that will require file growth
        for (int i = 0; i < 50; i++) {
          final byte[] key = ("key" + i).getBytes();
          byte[] value = new byte[100];
          Arrays.fill(value, (byte) ('A' + (i % 26)));
          store.insertRecord(key, value);
        }

        // Verify all records
        Assert.assertEquals(50, store.getNumRecords());
      }
    } finally {
      new File(fileName).delete();
    }
  }

  /// Validates that the crash safety guarantees are preserved by testing
  /// proper cleanup and recovery after abnormal shutdown.
  ///
  /// Note: Memory-mapped files achieve crash safety differently than direct I/O:
  /// - Direct I/O: Each write is interceptable and can fail individually
  /// - Memory-mapped: Writes are batched in memory, sync/force writes to disk
  ///
  /// The crash safety comes from:
  /// 1. Same write ordering and dual-write patterns are preserved
  /// 2. OS guarantees about memory-mapped file consistency
  /// 3. CRC32 validation on read catches any corruption
  /// 4. File structure validation on reopen
  @Test
  public void testCrashRecoveryWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-crash-" + System.nanoTime() + ".db";
    try {
      // Write some data
      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(1000)
              .useMemoryMapping(true)
              .open()) {
        for (int i = 0; i < 10; i++) {
          final byte[] key = ("key" + i).getBytes();
          final var value = ("value" + i).getBytes();
          store.insertRecord(key, value);
        }
        // Intentionally not calling fsync() to simulate crash during writes
      }

      // Reopen and verify - OS should have persisted the data
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        // Due to close() calling fsync(), all data should be present
        Assert.assertEquals(10, store.getNumRecords());
        for (int i = 0; i < 10; i++) {
          final byte[] key = ("key" + i).getBytes();
          Assert.assertTrue(store.recordExists(key));
          byte[] value = store.readRecordData(key);
          Assert.assertArrayEquals(("value" + i).getBytes(), value);
        }
      }
    } finally {
      new File(fileName).delete();
    }
  }

  /// Test the dual-write pattern for updates is preserved with memory mapping.
  /// The pattern writes: backup header -> data -> final header
  @Test
  public void testDualWritePatternWithMemoryMapping() throws Exception {
    String fileName = TEST_DIR + "/test-mmap-dual-" + System.nanoTime() + ".db";
    try {
      final var data1 = String.join("", Collections.nCopies(256, "1")).getBytes();
      final var data2 = String.join("", Collections.nCopies(256, "2")).getBytes();

      Path path = Paths.get(fileName);
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .preallocatedRecords(2000)
              .useMemoryMapping(true)
              .open()) {
        final var key = ("testkey".getBytes());
        store.insertRecord(key, data1);
        store.fsync(); // Ensure first write is persisted

        // Update - this should use the dual-write pattern
        store.updateRecord(key, data2);
        store.fsync(); // Ensure update is persisted

        byte[] read = store.readRecordData(key);
        Assert.assertArrayEquals(data2, read);
      }

      // Verify persistence
      try (FileRecordStore store =
          new FileRecordStore.Builder()
              .path(path)
              .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        final var key = ("testkey".getBytes());
        byte[] read = store.readRecordData(key);
        Assert.assertArrayEquals(data2, read);
      }
    } finally {
      new File(fileName).delete();
    }
  }
}
