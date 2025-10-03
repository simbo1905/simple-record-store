package com.github.trex_paxos.srs;

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * Performance comparison tests demonstrating the write amplification reduction
 * achieved by memory-mapped files.
 * 
 * These tests are informational and demonstrate the performance characteristics
 * rather than asserting specific performance metrics.
 */
public class PerformanceComparisonTest extends JulLoggingConfig {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir");
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
      try (FileRecordStore store = new FileRecordStore.Builder().path(directFile).preallocatedRecords(10000).disablePayloadCrc32(false).open()) {
          for (int i = 0; i < RECORD_COUNT; i++) {
              final var key = ByteSequence.of(("key" + i).getBytes());
              byte[] value = new byte[RECORD_SIZE];
              Arrays.fill(value, (byte) ('A' + (i % 26)));
              store.insertRecord(key, value);
          }
      }
      long directTime = System.nanoTime() - directStart;

      // Test memory-mapped I/O
      long mmapStart = System.nanoTime();
      try (FileRecordStore store = new FileRecordStore.Builder().path(mmapFile).preallocatedRecords(10000).useMemoryMapping(true).open()) {
          for (int i = 0; i < RECORD_COUNT; i++) {
              final var key = ByteSequence.of(("key" + i).getBytes());
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void compareUpdatePerformance() throws Exception {
        String directFile = TEST_DIR + "/perf-update-direct-" + System.nanoTime() + ".db";
        String mmapFile = TEST_DIR + "/perf-update-mmap-" + System.nanoTime() + ".db";
        
        try {
            // Prepare test data
            byte[] value1 = new byte[RECORD_SIZE];
            byte[] value2 = new byte[RECORD_SIZE];
            Arrays.fill(value1, (byte) 'A');
            Arrays.fill(value2, (byte) 'B');
            
            // Test direct I/O updates
          Path path = Paths.get(directFile);
          try (FileRecordStore store = new FileRecordStore.Builder().path(path).preallocatedRecords(10000).disablePayloadCrc32(false).open()) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    final var key = ByteSequence.of(("key" + i).getBytes());
                    store.insertRecord(key, value1);
                }
            }
            
            long directStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore.Builder().path(path).disablePayloadCrc32(false).useMemoryMapping(false).open()) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    final var key = ByteSequence.of(("key" + i).getBytes());
                    store.updateRecord(key, value2);
                }
            }
            long directTime = System.nanoTime() - directStart;
            
            // Test memory-mapped I/O updates
          Path path1 = Paths.get(mmapFile);
          try (FileRecordStore store = new FileRecordStore.Builder().path(path1).preallocatedRecords(10000).useMemoryMapping(true).open()) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    final var key = ByteSequence.of(("key" + i).getBytes());
                    store.insertRecord(key, value1);
                }
            }
            
            long mmapStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore.Builder().path(path1).disablePayloadCrc32(false).useMemoryMapping(true).open()) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    final var key = ByteSequence.of(("key" + i).getBytes());
                    store.updateRecord(key, value2);
                }
            }
            long mmapTime = System.nanoTime() - mmapStart;
            
            // Report results
            logger.log(Level.FINE, "\n=== Update Performance Comparison ===");
            logger.log(Level.FINE, "Records: " + (RECORD_COUNT / 2) + ", Size: " + RECORD_SIZE + " bytes each");
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
            new File(directFile).delete();
            new File(mmapFile).delete();
        }
    }

    @Test
    public void compareWriteAmplification() {
        logger.log(Level.FINE, "\n=== Write Amplification Analysis ===");
        logger.log(Level.FINE, "Based on code analysis:");
        logger.log(Level.FINE, "");
        logger.log(Level.FINE, "Direct I/O - INSERT (gap available):");
        logger.log(Level.FINE, "  5 disk writes per operation");
        logger.log(Level.FINE, "  (dataStartPtr header + record data + key to index + record header + numRecords header)");
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
