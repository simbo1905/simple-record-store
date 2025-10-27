package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/// Tests for basic FIFO queue operations
public class FifoRecordStoreTest {

  private Path tempFile;

  @Before
  public void setUp() throws IOException {
    tempFile = Files.createTempFile("fifo-test", ".db");
  }

  @After
  public void tearDown() throws IOException {
    if (tempFile != null) {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void testCreateNewQueue() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertTrue("New queue should be empty", fifo.isEmpty());
      assertEquals("New queue size should be 0", 0, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("nextCounter should start at 1", 1, stats.getNextCounter());
      assertEquals("totalPutCount should be 0", 0, stats.getTotalPutCount());
      assertEquals("totalTakeCount should be 0", 0, stats.getTotalTakeCount());
    }
  }

  @Test
  public void testPutAndTake() throws Exception {
    byte[] data1 = "test data 1".getBytes();
    byte[] data2 = "test data 2".getBytes();

    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      // Put items
      fifo.put(data1);
      assertEquals("Size after first put", 1, fifo.size());

      fifo.put(data2);
      assertEquals("Size after second put", 2, fifo.size());

      // Take items in FIFO order
      byte[] result1 = fifo.take();
      assertArrayEquals("First item should match", data1, result1);
      assertEquals("Size after first take", 1, fifo.size());

      byte[] result2 = fifo.take();
      assertArrayEquals("Second item should match", data2, result2);
      assertEquals("Size after second take", 0, fifo.size());
      assertTrue("Queue should be empty", fifo.isEmpty());
    }
  }

  @Test
  public void testFifoOrdering() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      // Put multiple items
      for (int i = 0; i < 10; i++) {
        fifo.put(("item-" + i).getBytes());
      }

      assertEquals("Queue should have 10 items", 10, fifo.size());

      // Take and verify FIFO order
      for (int i = 0; i < 10; i++) {
        byte[] result = fifo.take();
        String expected = "item-" + i;
        String actual = new String(result);
        assertEquals("Items should come out in FIFO order", expected, actual);
      }

      assertTrue("Queue should be empty after taking all items", fifo.isEmpty());
    }
  }

  @Test
  public void testPeek() throws Exception {
    byte[] data = "peek test".getBytes();

    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.put(data);

      // Peek should return data without removing
      byte[] peeked1 = fifo.peek();
      assertArrayEquals("Peek should return correct data", data, peeked1);
      assertEquals("Size should not change after peek", 1, fifo.size());

      // Peek again
      byte[] peeked2 = fifo.peek();
      assertArrayEquals("Peek should return same data", data, peeked2);
      assertEquals("Size should still not change", 1, fifo.size());

      // Take should return same data
      byte[] taken = fifo.take();
      assertArrayEquals("Take should return peeked data", data, taken);
      assertTrue("Queue should be empty after take", fifo.isEmpty());
    }
  }

  @Test
  public void testPeekEmpty() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      byte[] result = fifo.peek();
      assertNull("Peek on empty queue should return null", result);
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testTakeEmpty() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.take(); // Should throw NoSuchElementException
    }
  }

  @Test
  public void testBatchPut() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      List<byte[]> items = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        items.add(("batch-item-" + i).getBytes());
      }

      fifo.putBatch(items);

      assertEquals("Queue should have 5 items", 5, fifo.size());

      // Verify FIFO order
      for (int i = 0; i < 5; i++) {
        byte[] result = fifo.take();
        String expected = "batch-item-" + i;
        String actual = new String(result);
        assertEquals("Batch items should come out in order", expected, actual);
      }
    }
  }

  @Test
  public void testStatistics() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      // Put 3 items
      for (int i = 0; i < 3; i++) {
        fifo.put(("item-" + i).getBytes());
      }

      FifoStats stats1 = fifo.getStats();
      assertEquals("totalPutCount should be 3", 3, stats1.getTotalPutCount());
      assertEquals("currentSize should be 3", 3, stats1.getCurrentSize());
      assertEquals("highWaterMark should be 3", 3, stats1.getHighWaterMark());

      // Take 2 items
      fifo.take();
      fifo.take();

      FifoStats stats2 = fifo.getStats();
      assertEquals("totalPutCount should still be 3", 3, stats2.getTotalPutCount());
      assertEquals("totalTakeCount should be 2", 2, stats2.getTotalTakeCount());
      assertEquals("currentSize should be 1", 1, stats2.getCurrentSize());
      assertEquals("highWaterMark should still be 3", 3, stats2.getHighWaterMark());
      assertEquals("lowWaterMark should be 1", 1, stats2.getLowWaterMark());
    }
  }

  @Test
  public void testRecovery() throws Exception {
    byte[] data1 = "recovery test 1".getBytes();
    byte[] data2 = "recovery test 2".getBytes();

    // Create queue and add items
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      fifo.put(data1);
      fifo.put(data2);
      assertEquals("Size should be 2", 2, fifo.size());
    }

    // Reopen and verify state is recovered
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      assertEquals("Size should be recovered as 2", 2, fifo.size());

      FifoStats stats = fifo.getStats();
      assertEquals("totalPutCount should be recovered", 2, stats.getTotalPutCount());
      assertEquals("currentSize should be recovered", 2, stats.getCurrentSize());

      // Verify data
      byte[] result1 = fifo.take();
      assertArrayEquals("First item should be recovered", data1, result1);

      byte[] result2 = fifo.take();
      assertArrayEquals("Second item should be recovered", data2, result2);

      assertTrue("Queue should be empty", fifo.isEmpty());
    }
  }

  @Test
  public void testHighWaterMark() throws Exception {
    try (FifoRecordStore fifo = FifoRecordStore.open(tempFile)) {
      // Add 5 items
      for (int i = 0; i < 5; i++) {
        fifo.put(("item-" + i).getBytes());
      }

      assertEquals("High water mark should be 5", 5, fifo.getStats().getHighWaterMark());

      // Remove 3 items
      for (int i = 0; i < 3; i++) {
        fifo.take();
      }

      assertEquals("High water mark should still be 5", 5, fifo.getStats().getHighWaterMark());
      assertEquals("Current size should be 2", 2, fifo.size());

      // Add 4 more items
      for (int i = 0; i < 4; i++) {
        fifo.put(("new-item-" + i).getBytes());
      }

      assertEquals("High water mark should be 6", 6, fifo.getStats().getHighWaterMark());
      assertEquals("Current size should be 6", 6, fifo.size());
    }
  }
}
