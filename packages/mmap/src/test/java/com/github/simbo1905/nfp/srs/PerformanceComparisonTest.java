package com.github.simbo1905.nfp.srs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Level;
import org.junit.Test;

public class PerformanceComparisonTest extends JulLoggingConfig {

  private static final int RECORD_COUNT = 1000;
  private static final int RECORD_SIZE = 256;

  @Test
  public void compareInsertPerformance() throws Exception {
    Path directFile = Files.createTempFile("perf-direct-", ".db");
    Path mmapFile = Files.createTempFile("perf-mmap-", ".db");
    directFile.toFile().deleteOnExit();
    mmapFile.toFile().deleteOnExit();

    // Test direct I/O
    long directStart = System.nanoTime();
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(directFile)
            .preallocatedRecords(10000)
            .maxKeyLength(64)
            .disablePayloadCrc32(false)
            .open()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final byte[] key = ("key" + i).getBytes();
        byte[] value = new byte[RECORD_SIZE];
        Arrays.fill(value, (byte) ('A' + (i % 26)));
        store.insertRecord(key, value);
      }
    }
    long directTime = System.nanoTime() - directStart;

    // Test memory-mapped I/O
    long mmapStart = System.nanoTime();
    try (FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(mmapFile)
            .preallocatedRecords(10000)
            .maxKeyLength(64)
            .useMemoryMapping(true)
            .open()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final byte[] key = ("key" + i).getBytes();
        byte[] value = new byte[RECORD_SIZE];
        Arrays.fill(value, (byte) ('A' + (i % 26)));
        store.insertRecord(key, value);
      }
    }
    long mmapTime = System.nanoTime() - mmapStart;

    // Report results
    logger.log(Level.FINE, "\n=== Insert Performance Comparison ===");
    logger.log(Level.FINE, "Records: " + RECORD_COUNT + ", Size: " + RECORD_SIZE + " bytes each");
    logger.log(Level.FINE, "Direct I/O:        " + (directTime / 1_000_000) + " ms");
    logger.log(Level.FINE, "Memory-Mapped I/O: " + (mmapTime / 1_000_000) + " ms");

    double speedup = (double) directTime / mmapTime;
    logger.log(Level.FINE, "Speedup: " + String.format("%.2f", speedup) + "x");

    if (mmapTime < directTime) {
      double reduction = (1.0 - (double) mmapTime / directTime) * 100;
      logger.log(Level.FINE, "Time reduction: " + String.format("%.1f", reduction) + "%");
    }
    logger.log(Level.FINE, "=====================================\n");
  }

  @Test
  public void compareUpdatePerformance() throws Exception {
    Path directPath = Files.createTempFile("perf-update-direct-", ".db");
    Path mmapPath = Files.createTempFile("perf-update-mmap-", ".db");
    directPath.toFile().deleteOnExit();
    mmapPath.toFile().deleteOnExit();

    try {
      // Prepare test data
      byte[] value1 = new byte[RECORD_SIZE];
      byte[] value2 = new byte[RECORD_SIZE];
      Arrays.fill(value1, (byte) 'A');
      Arrays.fill(value2, (byte) 'B');

      // Test direct I/O updates
      try (FileRecordStore store =
          new FileRecordStoreBuilder()
              .path(directPath)
              .preallocatedRecords(10000)
              .maxKeyLength(64)
              .disablePayloadCrc32(false)
              .open()) {
        for (int i = 0; i < RECORD_COUNT / 2; i++) {
          final byte[] key = ("key" + i).getBytes();
          store.insertRecord(key, value1);
        }
      }

      long directStart = System.nanoTime();
      try (FileRecordStore store =
          new FileRecordStoreBuilder()
              .path(directPath)
              .maxKeyLength(64)
              .disablePayloadCrc32(false)
              .useMemoryMapping(false)
              .open()) {
        for (int i = 0; i < RECORD_COUNT / 2; i++) {
          final byte[] key = ("key" + i).getBytes();
          store.updateRecord(key, value2);
        }
      }
      long directTime = System.nanoTime() - directStart;

      // Test memory-mapped I/O updates
      try (FileRecordStore store =
          new FileRecordStoreBuilder()
              .path(mmapPath)
              .preallocatedRecords(10000)
              .maxKeyLength(64)
              .useMemoryMapping(true)
              .open()) {
        for (int i = 0; i < RECORD_COUNT / 2; i++) {
          final byte[] key = ("key" + i).getBytes();
          store.insertRecord(key, value1);
        }
      }

      long mmapStart = System.nanoTime();
      try (FileRecordStore store =
          new FileRecordStoreBuilder()
              .path(mmapPath)
              .maxKeyLength(64)
              .disablePayloadCrc32(false)
              .useMemoryMapping(true)
              .open()) {
        for (int i = 0; i < RECORD_COUNT / 2; i++) {
          final byte[] key = ("key" + i).getBytes();
          store.updateRecord(key, value2);
        }
      }
      long mmapTime = System.nanoTime() - mmapStart;

      // Report results
      logger.log(Level.FINE, "\n=== Update Performance Comparison ===");
      logger.log(
          Level.FINE, "Records: " + (RECORD_COUNT / 2) + ", Size: " + RECORD_SIZE + " bytes each");
      logger.log(Level.FINE, "Direct I/O:        " + (directTime / 1_000_000) + " ms");
      logger.log(Level.FINE, "Memory-Mapped I/O: " + (mmapTime / 1_000_000) + " ms");

      double speedup = (double) directTime / mmapTime;
      logger.log(Level.FINE, "Speedup: " + String.format("%.2f", speedup) + "x");

      if (mmapTime < directTime) {
        double reduction = (1.0 - (double) mmapTime / directTime) * 100;
        logger.log(Level.FINE, "Time reduction: " + String.format("%.1f", reduction) + "%");
      }
      logger.log(Level.FINE, "====================================\n");

    } finally {
      Files.deleteIfExists(directPath);
      Files.deleteIfExists(mmapPath);
    }
  }

  @Test
  public void compareWriteAmplification() {
    logger.log(Level.FINE, "\n=== Write Amplification Analysis ===");
    logger.log(Level.FINE, "Based on code analysis:");
    logger.log(Level.FINE, "");
    logger.log(Level.FINE, "Direct I/O - INSERT (gap available):");
    logger.log(Level.FINE, "  5 disk writes per operation");
    logger.log(
        Level.FINE,
        "  (dataStartPtr header + record data + key to index + record header + numRecords header)");
    logger.log(Level.FINE, "");
    logger.log(Level.FINE, "Direct I/O - UPDATE (in-place):");
    logger.log(Level.FINE, "  3 disk writes per operation");
    logger.log(Level.FINE, "  (backup header + data + final header)");
    logger.log(Level.FINE, "");
    logger.log(Level.FINE, "Memory-Mapped - INSERT:");
    logger.log(Level.FINE, "  5 memory writes batched, 1 sync on close/fsync");
    logger.log(Level.FINE, "  Effective: 1 disk flush per batch of operations");
    logger.log(Level.FINE, "");
    logger.log(Level.FINE, "Memory-Mapped - UPDATE:");
    logger.log(Level.FINE, "  3 memory writes batched, 1 sync on close/fsync");
    logger.log(Level.FINE, "  Effective: 1 disk flush per batch of operations");
    logger.log(Level.FINE, "");
    logger.log(Level.FINE, "For " + RECORD_COUNT + " operations:");
    logger.log(Level.FINE, "  Direct I/O:        ~" + (RECORD_COUNT * 5) + " write operations");
    logger.log(Level.FINE, "  Memory-Mapped I/O: ~" + RECORD_COUNT + " memory writes + 1 sync");
    logger.log(Level.FINE, "  Write Amplification Reduction: ~" + (5) + "x");
    logger.log(Level.FINE, "====================================\n");
  }
}
