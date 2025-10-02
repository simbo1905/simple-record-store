package com.github.trex_paxos.srs.api;

import com.github.trex_paxos.srs.ByteSequence;
import com.github.trex_paxos.srs.FileRecordStore;
import com.github.trex_paxos.srs.JulLoggingConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Examples demonstrating how to use memory-mapped file mode for
 * reduced write amplification while maintaining crash safety.
 */
public class MemoryMappedTests extends JulLoggingConfig {

  // Constant for maxKeyLength to ensure consistency across test operations
  // Use a reasonable value that fits in the file format (max is 252)
  private static final int MAX_KEY_LENGTH = 64;

  @Test
  public void testBasicUsage() throws IOException {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger(MemoryMappedTests.class.getName());
    logger.fine("Starting testBasicUsage with temp file");
    
    Path tempFilePath;

    // Create a new store with memory-mapping enabled
    // The true parameter enables memory-mapped mode
    logger.fine("Creating FileRecordStore with parameters: tempFile, preallocatedRecords=128, maxKeyLength=" + MAX_KEY_LENGTH + ", disablePayloadCrc32=false, useMemoryMapping=true");
    try (FileRecordStore store = new FileRecordStore.Builder().tempFile("mydb-", ".tmp").preallocatedRecords(128).maxKeyLength(MAX_KEY_LENGTH).disablePayloadCrc32(false).useMemoryMapping(true).open()) {
      logger.fine("FileRecordStore created successfully");
      tempFilePath = store.getFilePath(); // Capture the path for reopening

      // Insert records - writes go to memory-mapped buffers
      ByteSequence key1 = ByteSequence.of("user:1".getBytes());
      byte[] userData = "John Doe".getBytes();
      logger.fine("Inserting record with key: " + new String("user:1".getBytes()) + ", data: " + new String(userData));
      store.insertRecord(key1, userData);

      // Multiple operations are batched in memory
      ByteSequence key2 = ByteSequence.of("user:2".getBytes());
      logger.fine("Inserting record with key: " + new String("user:2".getBytes()) + ", data: Jane Smith");
      store.insertRecord(key2, "Jane Smith".getBytes());

      // Read works immediately from memory-mapped buffers
      logger.fine("Reading record with key: " + new String("user:1".getBytes()));
      byte[] retrieved = store.readRecordData(key1);
      System.out.println("Retrieved: " + new String(retrieved));

      // Update also goes to memory
      logger.fine("Updating record with key: " + new String("user:1".getBytes()) + ", data: John Doe Updated");
      store.updateRecord(key1, "John Doe Updated".getBytes());

      // Optional: Force sync to disk before close
      logger.fine("Calling fsync()");
      store.fsync();

      // On close(), all buffered writes are automatically synced to disk
      logger.fine("Closing store");
    }
    
    logger.fine("Store closed, reopening for verification");

    // Data persists - reopen and verify
    logger.fine("Reopening FileRecordStore with parameters: tempFile, maxKeyLength=" + MAX_KEY_LENGTH + ", disablePayloadCrc32=false, useMemoryMapping=true");
    try (FileRecordStore store = new FileRecordStore.Builder().path(tempFilePath).maxKeyLength(MAX_KEY_LENGTH).disablePayloadCrc32(false).useMemoryMapping(true).open()) {
      logger.fine("FileRecordStore reopened successfully");
      ByteSequence key1 = ByteSequence.of("user:1".getBytes());
      logger.fine("Reading record with key: " + new String("user:1".getBytes()));
      byte[] data = store.readRecordData(key1);
      System.out.println("After reopen: " + new String(data));
    }
  }

  @Test
  public void batchOperationsWithControlledSync() throws IOException {

    try (FileRecordStore store = new FileRecordStore.Builder()
        .tempFile("mydb-batch-", ".tmp")
        .preallocatedRecords(50000)    // pre-allocate 50KB to reduce remapping
        .maxKeyLength(64)              // max key length
        .disablePayloadCrc32(false)    // enable CRC32
        .useMemoryMapping(true)        // enable memory-mapping
        .open()) {
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
  }

  @Test
  public void updateHeavyWorkload() throws IOException {

    // For update-heavy workloads, memory-mapping provides significant benefits
    try (FileRecordStore store = new FileRecordStore.Builder().tempFile("mydb-update-", ".tmp").preallocatedRecords(128).maxKeyLength(MAX_KEY_LENGTH).disablePayloadCrc32(false).useMemoryMapping(true).open()) {

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
  }

  @Test
  public void mixedDirectAndMemoryMappedAccess() throws IOException {
    Path tempFile;
    
    // Create initial data with direct I/O
    try (FileRecordStore store = new FileRecordStore.Builder().tempFile("example-mixed-", ".db").preallocatedRecords(10000).disablePayloadCrc32(false).open()) {
      tempFile = store.getFilePath(); // Get the path from the created temp file
      store.insertRecord(ByteSequence.of("key1".getBytes()), "value1".getBytes());
    }

    // Open with memory-mapping for batch updates
    try (FileRecordStore store = new FileRecordStore.Builder().path(tempFile).disablePayloadCrc32(false).useMemoryMapping(true).open()) {
      store.updateRecord(ByteSequence.of("key1".getBytes()), "updated".getBytes());
      store.insertRecord(ByteSequence.of("key2".getBytes()), "value2".getBytes());
    }

    // Open with direct I/O again for verification
    try (FileRecordStore store = new FileRecordStore.Builder().path(tempFile).disablePayloadCrc32(false).useMemoryMapping(false).open()) {
      byte[] data = store.readRecordData(ByteSequence.of("key1".getBytes()));
      System.out.println("Final value: " + new String(data));
    }
  }
}

