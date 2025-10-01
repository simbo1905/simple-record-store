package com.github.trex_paxos.srs.api;

import com.github.trex_paxos.srs.ByteSequence;
import com.github.trex_paxos.srs.FileRecordStore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Examples demonstrating how to use memory-mapped file mode for
 * reduced write amplification while maintaining crash safety.
 */
public class MemoryMappedUsageExample {

    @Test
    public void basicMemoryMappedUsage() throws IOException {
        String dbPath = "/tmp/example-mmap.db";
        
        try {
            // Create a new store with memory-mapping enabled
            // The true parameter enables memory-mapped mode
            try (FileRecordStore store = new FileRecordStore(dbPath, 10000, true)) {
                
                // Insert records - writes go to memory-mapped buffers
                ByteSequence key1 = ByteSequence.of("user:1".getBytes());
                byte[] userData = "John Doe".getBytes();
                store.insertRecord(key1, userData);
                
                // Multiple operations are batched in memory
                ByteSequence key2 = ByteSequence.of("user:2".getBytes());
                store.insertRecord(key2, "Jane Smith".getBytes());
                
                // Read works immediately from memory-mapped buffers
                byte[] retrieved = store.readRecordData(key1);
                System.out.println("Retrieved: " + new String(retrieved));
                
                // Update also goes to memory
                store.updateRecord(key1, "John Doe Updated".getBytes());
                
                // Optional: Force sync to disk before close
                store.fsync();
                
                // On close(), all buffered writes are automatically synced to disk
            }
            
            // Data persists - reopen and verify
            try (FileRecordStore store = new FileRecordStore(dbPath, "r", false, false)) {
                ByteSequence key1 = ByteSequence.of("user:1".getBytes());
                byte[] data = store.readRecordData(key1);
                System.out.println("After reopen: " + new String(data));
            }
            
        } finally {
            new File(dbPath).delete();
        }
    }

    @Test
    public void updateHeavyWorkload() throws IOException {
        String dbPath = "/tmp/example-updates.db";
        
        try {
            // For update-heavy workloads, memory-mapping provides significant benefits
            try (FileRecordStore store = new FileRecordStore(dbPath, 100000, true)) {
                
                // Insert initial data
                for (int i = 0; i < 1000; i++) {
                    ByteSequence key = ByteSequence.of(("key" + i).getBytes());
                    store.insertRecord(key, ("initial-value-" + i).getBytes());
                }
                
                // Perform many updates - this is where memory-mapping shines
                // Each update would normally be 3 disk writes, now they're batched
                for (int i = 0; i < 1000; i++) {
                    ByteSequence key = ByteSequence.of(("key" + i).getBytes());
                    store.updateRecord(key, ("updated-value-" + i).getBytes());
                }
                
                // All updates are synced on close
            }
            
            System.out.println("Update-heavy workload completed with memory-mapping");
            
        } finally {
            new File(dbPath).delete();
        }
    }

    @Test
    public void batchOperationsWithControlledSync() throws IOException {
        String dbPath = "/tmp/example-batch.db";
        
        try {
            try (FileRecordStore store = new FileRecordStore(
                    dbPath,      // path
                    50000,       // pre-allocate 50KB to reduce remapping
                    64,          // max key length
                    false,       // enable CRC32
                    true         // enable memory-mapping
            )) {
                
                // Process records in batches
                for (int batch = 0; batch < 5; batch++) {
                    // Insert batch of records
                    for (int i = 0; i < 100; i++) {
                        int recordId = batch * 100 + i;
                        ByteSequence key = ByteSequence.of(("record" + recordId).getBytes());
                        store.insertRecord(key, ("data-" + recordId).getBytes());
                    }
                    
                    // Optionally sync after each batch for durability checkpoint
                    store.fsync();
                    System.out.println("Batch " + batch + " synced to disk");
                }
            }
            
        } finally {
            new File(dbPath).delete();
        }
    }

    @Test
    public void mixedDirectAndMemoryMappedAccess() throws IOException {
        String dbPath = "/tmp/example-mixed.db";
        
        try {
            // Create initial data with direct I/O
            try (FileRecordStore store = new FileRecordStore(dbPath, 10000, false)) {
                store.insertRecord(ByteSequence.of("key1".getBytes()), "value1".getBytes());
            }
            
            // Open with memory-mapping for batch updates
            try (FileRecordStore store = new FileRecordStore(dbPath, "rw", false, true)) {
                store.updateRecord(ByteSequence.of("key1".getBytes()), "updated".getBytes());
                store.insertRecord(ByteSequence.of("key2".getBytes()), "value2".getBytes());
            }
            
            // Open with direct I/O again for verification
            try (FileRecordStore store = new FileRecordStore(dbPath, "r", false, false)) {
                byte[] data = store.readRecordData(ByteSequence.of("key1".getBytes()));
                System.out.println("Final value: " + new String(data));
            }
            
        } finally {
            new File(dbPath).delete();
        }
    }
}
