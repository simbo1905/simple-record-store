package com.github.trex_paxos.srs;

import lombok.val;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

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
        String directFile = TEST_DIR + "/perf-direct-" + System.nanoTime() + ".db";
        String mmapFile = TEST_DIR + "/perf-mmap-" + System.nanoTime() + ".db";
        
        try {
            // Test direct I/O
            long directStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore(directFile, 10000, false)) {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    byte[] value = new byte[RECORD_SIZE];
                    Arrays.fill(value, (byte) ('A' + (i % 26)));
                    store.insertRecord(key, value);
                }
            }
            long directTime = System.nanoTime() - directStart;
            
            // Test memory-mapped I/O
            long mmapStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore(mmapFile, 10000, true)) {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    byte[] value = new byte[RECORD_SIZE];
                    Arrays.fill(value, (byte) ('A' + (i % 26)));
                    store.insertRecord(key, value);
                }
            }
            long mmapTime = System.nanoTime() - mmapStart;
            
            // Report results
            System.out.println("\n=== Insert Performance Comparison ===");
            System.out.println("Records: " + RECORD_COUNT + ", Size: " + RECORD_SIZE + " bytes each");
            System.out.println("Direct I/O:        " + (directTime / 1_000_000) + " ms");
            System.out.println("Memory-Mapped I/O: " + (mmapTime / 1_000_000) + " ms");
            
            double speedup = (double) directTime / mmapTime;
            System.out.println("Speedup: " + String.format("%.2f", speedup) + "x");
            
            if (mmapTime < directTime) {
                double reduction = (1.0 - (double) mmapTime / directTime) * 100;
                System.out.println("Time reduction: " + String.format("%.1f", reduction) + "%");
            }
            System.out.println("=====================================\n");
            
        } finally {
            new File(directFile).delete();
            new File(mmapFile).delete();
        }
    }

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
            try (FileRecordStore store = new FileRecordStore(directFile, 10000, false)) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    store.insertRecord(key, value1);
                }
            }
            
            long directStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore(directFile, "rw", false, false)) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    store.updateRecord(key, value2);
                }
            }
            long directTime = System.nanoTime() - directStart;
            
            // Test memory-mapped I/O updates
            try (FileRecordStore store = new FileRecordStore(mmapFile, 10000, true)) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    store.insertRecord(key, value1);
                }
            }
            
            long mmapStart = System.nanoTime();
            try (FileRecordStore store = new FileRecordStore(mmapFile, "rw", false, true)) {
                for (int i = 0; i < RECORD_COUNT / 2; i++) {
                    val key = ByteSequence.of(("key" + i).getBytes());
                    store.updateRecord(key, value2);
                }
            }
            long mmapTime = System.nanoTime() - mmapStart;
            
            // Report results
            System.out.println("\n=== Update Performance Comparison ===");
            System.out.println("Records: " + (RECORD_COUNT / 2) + ", Size: " + RECORD_SIZE + " bytes each");
            System.out.println("Direct I/O:        " + (directTime / 1_000_000) + " ms");
            System.out.println("Memory-Mapped I/O: " + (mmapTime / 1_000_000) + " ms");
            
            double speedup = (double) directTime / mmapTime;
            System.out.println("Speedup: " + String.format("%.2f", speedup) + "x");
            
            if (mmapTime < directTime) {
                double reduction = (1.0 - (double) mmapTime / directTime) * 100;
                System.out.println("Time reduction: " + String.format("%.1f", reduction) + "%");
            }
            System.out.println("====================================\n");
            
        } finally {
            new File(directFile).delete();
            new File(mmapFile).delete();
        }
    }

    @Test
    public void compareWriteAmplification() throws Exception {
        System.out.println("\n=== Write Amplification Analysis ===");
        System.out.println("Based on code analysis:");
        System.out.println("");
        System.out.println("Direct I/O - INSERT (gap available):");
        System.out.println("  5 disk writes per operation");
        System.out.println("  (dataStartPtr header + record data + key to index + record header + numRecords header)");
        System.out.println("");
        System.out.println("Direct I/O - UPDATE (in-place):");
        System.out.println("  3 disk writes per operation");
        System.out.println("  (backup header + data + final header)");
        System.out.println("");
        System.out.println("Memory-Mapped - INSERT:");
        System.out.println("  5 memory writes batched, 1 msync on close/fsync");
        System.out.println("  Effective: 1 disk flush per batch of operations");
        System.out.println("");
        System.out.println("Memory-Mapped - UPDATE:");
        System.out.println("  3 memory writes batched, 1 msync on close/fsync");
        System.out.println("  Effective: 1 disk flush per batch of operations");
        System.out.println("");
        System.out.println("For " + RECORD_COUNT + " operations:");
        System.out.println("  Direct I/O:        ~" + (RECORD_COUNT * 5) + " write operations");
        System.out.println("  Memory-Mapped I/O: ~" + RECORD_COUNT + " memory writes + 1 msync");
        System.out.println("  Write Amplification Reduction: ~" + (5) + "x");
        System.out.println("====================================\n");
    }
}
