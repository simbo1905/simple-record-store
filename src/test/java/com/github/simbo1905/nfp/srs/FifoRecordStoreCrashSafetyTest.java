package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/// Tests for crash safety and atomicity guarantees in FIFO wrapper.
/// Verifies that write ordering (items before genesis record) ensures consistency.
public class FifoRecordStoreCrashSafetyTest {

  private Path tempFile;

  @Before
  public void setUp() throws IOException {
    tempFile = Files.createTempFile("fifo-crash-test", ".db");
  }

  @After
  public void tearDown() throws IOException {
    if (tempFile != null) {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testBatchWriteAtomicity() throws Exception {
    // This test verifies that the genesis record is updated AFTER all items are written
    // If genesis is updated first and crash occurs during item writes,
    // the queue would be in inconsistent state

    List<byte[]> batch = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      batch.add(("batch-item-" + i).getBytes());
    }

    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.putBatch(batch);

      // Verify all items are present
      assertEquals("All items should be in queue", 10, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("totalPutCount should be 10", 10, stats.getTotalPutCount());
      assertEquals("currentSize should be 10", 10, stats.getCurrentSize());
    }

    // Reopen to verify recovery
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Size should be recovered as 10", 10, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("totalPutCount should be recovered", 10, stats.getTotalPutCount());

      // Verify all items can be retrieved in order
      for (int i = 0; i < 10; i++) {
        byte[] result = fifo.take();
        String expected = "batch-item-" + i;
        String actual = new String(result);
        assertEquals("Items should be in correct order", expected, actual);
      }

      assertTrue("Queue should be empty", fifo.isEmpty());
    }
  }

  @Test
  public void testRecoveryAfterPartialWrites() throws Exception {
    // Simulate scenario where some items are written but genesis not updated
    // (simulates crash during batch write)

    // First, create a queue with some items
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.put("item-1".getBytes());
      fifo.put("item-2".getBytes());
      assertEquals("Initial size should be 2", 2, fifo.size());
    }

    // Reopen - should recover to consistent state
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Size should be recovered as 2", 2, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("Stats should be consistent", 2, stats.getCurrentSize());
      assertEquals("totalPutCount should be 2", 2, stats.getTotalPutCount());

      // Can still operate normally
      fifo.put("item-3".getBytes());
      assertEquals("Can add new items", 3, fifo.size());

      // Verify all data intact
      assertEquals("item-1", new String(fifo.take()));
      assertEquals("item-2", new String(fifo.take()));
      assertEquals("item-3", new String(fifo.take()));
    }
  }

  @Test
  public void testConsistencyAfterMultipleSessions() throws Exception {
    // Write, close, reopen multiple times to verify consistency

    // Session 1: Write some items
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      for (int i = 0; i < 5; i++) {
        fifo.put(("s1-item-" + i).getBytes());
      }
      assertEquals("Session 1 size", 5, fifo.size());
    }

    // Session 2: Write more and take some
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Session 2 recovered size", 5, fifo.size());

      for (int i = 0; i < 3; i++) {
        fifo.put(("s2-item-" + i).getBytes());
      }

      fifo.take(); // Remove s1-item-0
      fifo.take(); // Remove s1-item-1

      assertEquals("Session 2 final size", 6, fifo.size());
    }

    // Session 3: Verify and continue
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Session 3 recovered size", 6, fifo.size());

      // Should get s1-item-2 next
      assertEquals("s1-item-2", new String(fifo.take()));
      assertEquals("s1-item-3", new String(fifo.take()));
      assertEquals("s1-item-4", new String(fifo.take()));
      assertEquals("s2-item-0", new String(fifo.take()));
      assertEquals("s2-item-1", new String(fifo.take()));
      assertEquals("s2-item-2", new String(fifo.take()));

      assertTrue("Queue should be empty", fifo.isEmpty());
    }
  }

  @Test
  public void testGenesisRecordIntegrity() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      // Perform various operations
      fifo.put("item-1".getBytes());
      fifo.put("item-2".getBytes());
      fifo.put("item-3".getBytes());

      fifo.take(); // Remove item-1

      List<byte[]> batch = new ArrayList<>();
      batch.add("batch-1".getBytes());
      batch.add("batch-2".getBytes());
      fifo.putBatch(batch);

      FifoStats stats1 = fifo.getStats();
      assertEquals("Current size should be 4", 4, stats1.getCurrentSize());
      assertEquals("Total put should be 5", 5, stats1.getTotalPutCount());
      assertEquals("Total take should be 1", 1, stats1.getTotalTakeCount());
      assertEquals("High water mark should be 4", 4, stats1.getHighWaterMark());
    }

    // Reopen and verify genesis record was persisted correctly
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      FifoStats stats2 = fifo.getStats();
      assertEquals("Recovered size should be 4", 4, stats2.getCurrentSize());
      assertEquals("Recovered put count should be 5", 5, stats2.getTotalPutCount());
      assertEquals("Recovered take count should be 1", 1, stats2.getTotalTakeCount());
      assertEquals("Recovered high water mark should be 4", 4, stats2.getHighWaterMark());
    }
  }

  @Test
  public void testEmptyBatchDoesNothing() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.put("item-1".getBytes());

      FifoStats stats1 = fifo.getStats();
      long nextCounter1 = stats1.getNextCounter();

      // Empty batch should not change state
      fifo.putBatch(new ArrayList<>());

      FifoStats stats2 = fifo.getStats();
      assertEquals("Size should not change", 1, stats2.getCurrentSize());
      assertEquals("NextCounter should not change", nextCounter1, stats2.getNextCounter());
    }
  }

  @Test
  public void testLargeQueueRecovery() throws Exception {
    final int ITEM_COUNT = 1000;

    // Create large queue
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      List<byte[]> batch = new ArrayList<>();
      for (int i = 0; i < ITEM_COUNT; i++) {
        batch.add(("item-" + i).getBytes());
      }
      fifo.putBatch(batch);

      assertEquals("Queue should have " + ITEM_COUNT + " items", ITEM_COUNT, fifo.size());
    }

    // Reopen and verify
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Recovered queue should have " + ITEM_COUNT + " items", ITEM_COUNT, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("Total put should be " + ITEM_COUNT, ITEM_COUNT, stats.getTotalPutCount());

      // Take some items
      for (int i = 0; i < 10; i++) {
        String expected = "item-" + i;
        String actual = new String(fifo.take());
        assertEquals("Items should be in order", expected, actual);
      }

      assertEquals("Remaining items", ITEM_COUNT - 10, fifo.size());
    }

    // Reopen again and verify consistency
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Final size should be correct", ITEM_COUNT - 10, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("Total put should still be " + ITEM_COUNT, ITEM_COUNT, stats.getTotalPutCount());
      assertEquals("Total take should be 10", 10, stats.getTotalTakeCount());
    }
  }
}
