package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.logging.Level;
import org.junit.Assert;
import org.junit.Test;

/// Test to confirm the free space tracking bug after split allocation.
/// This test demonstrates that after a split allocation, the original RecordHeader
/// isn't mutated, so its free space calculation remains incorrect.
public class FreeSpaceTrackingBugTest extends JulLoggingConfig {

  @Test
  public void testFreeSpaceAfterSplitAllocation() throws Exception {
    logger.log(Level.FINE, "=== Testing Free Space After Split Allocation ===");

    Path file = Files.createTempFile("free-space-bug-", ".db");
    try {
      FileRecordStore store =
          new FileRecordStoreBuilder().path(file).maxKeyLength(32).disablePayloadCrc32(true).open();

      // Insert a record with significant data
      byte[] key1 = "key1".getBytes();
      byte[] largeData = new byte[1000]; // 1000 bytes of data
      for (int i = 0; i < largeData.length; i++) {
        largeData[i] = (byte) (i % 256);
      }

      logger.log(Level.FINE, "Inserting large record with key1");
      store.insertRecord(key1, largeData);

      // Update with smaller data to create free space within the record
      byte[] smallData = new byte[100]; // Much smaller
      for (int i = 0; i < smallData.length; i++) {
        smallData[i] = (byte) ((i + 50) % 256);
      }
      
      logger.log(Level.FINE, "Updating key1 with smaller data to create free space");
      store.updateRecord(key1, smallData);

      // Get access to the freeMap via reflection to check internal state
      Field freeMapField = FileRecordStore.class.getDeclaredField("freeMap");
      freeMapField.setAccessible(true);
      @SuppressWarnings("unchecked")
      ConcurrentNavigableMap<RecordHeader, Integer> freeMap =
          (ConcurrentNavigableMap<RecordHeader, Integer>) freeMapField.get(store);

      logger.log(
          Level.FINE,
          () -> String.format("Free map entries after update: %d", freeMap.size()));
      
      int totalFreeSpace = 0;
      for (RecordHeader header : freeMap.keySet()) {
        int freeSpace = freeMap.get(header);
        totalFreeSpace += freeSpace;
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "  Free entry: dataPointer=%d, dataLength=%d, dataCapacity=%d, freeSpace=%d",
                    header.dataPointer(), header.dataLength(), header.dataCapacity(), freeSpace));

        // Verify: freeSpace in map should match what the header calculates
        int calculatedFreeSpace = header.getFreeSpace(true); // disableCrc32=true
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "  Calculated free space from header: %d (should match freeMap value: %d)",
                    calculatedFreeSpace, freeSpace));

        // The values should match
        Assert.assertEquals(
            "Free space in map should match calculated free space from header",
            calculatedFreeSpace,
            freeSpace);
      }

      // Now insert a second record that will use the free space via split
      byte[] key2 = "key2".getBytes();
      byte[] data2 = new byte[50]; // Small enough to fit in the free space
      for (int i = 0; i < data2.length; i++) {
        data2[i] = (byte) ((i + 100) % 256);
      }

      logger.log(Level.FINE, "Inserting key2 (should trigger split of free space)");
      store.insertRecord(key2, data2);

      // Check the freeMap state after split
      logger.log(
          Level.FINE, () -> String.format("Free map entries after split: %d", freeMap.size()));
      
      int totalFreeSpaceAfterSplit = 0;
      for (RecordHeader header : freeMap.keySet()) {
        int freeSpace = freeMap.get(header);
        totalFreeSpaceAfterSplit += freeSpace;
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "  Free entry after split: dataPointer=%d, dataLength=%d, dataCapacity=%d, freeSpace=%d",
                    header.dataPointer(), header.dataLength(), header.dataCapacity(), freeSpace));

        // BUG: After split, the original header should have reduced capacity
        // but it still has the original capacity, so getFreeSpace() returns
        // the old free space value
        int calculatedFreeSpace = header.getFreeSpace(true); // disableCrc32=true
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "  Calculated free space from header: %d (should match freeMap value: %d)",
                    calculatedFreeSpace, freeSpace));

        // This is where the bug manifests: freeSpace in map doesn't match what the header calculates
        // because the header's capacity wasn't reduced after split
        Assert.assertEquals(
            "Free space in map should match calculated free space from header after split",
            calculatedFreeSpace,
            freeSpace);
      }

      final int finalTotalFreeSpace = totalFreeSpace;
      final int finalTotalFreeSpaceAfterSplit = totalFreeSpaceAfterSplit;
      logger.log(
          Level.FINE,
          () -> String.format("Total free space before split: %d, after split: %d", 
              finalTotalFreeSpace, finalTotalFreeSpaceAfterSplit));

      // Verify that key2 was actually inserted
      Assert.assertTrue("key2 should exist", store.recordExists(key2));
      byte[] readData = store.readRecordData(key2);
      Assert.assertArrayEquals("Data should match", data2, readData);

      store.close();
      logger.log(Level.FINE, "=== Test completed ===");

    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void testSplitReducesCapacityCorrectly() throws Exception {
    logger.log(Level.FINE, "=== Testing Split Reduces Capacity Correctly ===");

    Path file = Files.createTempFile("split-capacity-bug-", ".db");
    try {
      FileRecordStore store =
          new FileRecordStoreBuilder().path(file).maxKeyLength(32).disablePayloadCrc32(true).open();

      // Create a free space by inserting a large record then updating with smaller data
      byte[] key1 = "key1".getBytes();
      byte[] largeData = new byte[2000];
      store.insertRecord(key1, largeData);
      
      byte[] mediumData = new byte[500];
      store.updateRecord(key1, mediumData);

      // Access internal state
      Field freeMapField = FileRecordStore.class.getDeclaredField("freeMap");
      freeMapField.setAccessible(true);
      @SuppressWarnings("unchecked")
      ConcurrentNavigableMap<RecordHeader, Integer> freeMap =
          (ConcurrentNavigableMap<RecordHeader, Integer>) freeMapField.get(store);

      // Get the initial free space
      if (freeMap.size() == 0) {
        logger.log(Level.FINE, "No free space entries - test scenario not applicable");
        store.close();
        return;
      }
      
      Assert.assertTrue("Should have at least one free entry", freeMap.size() > 0);
      RecordHeader originalFreeHeader = freeMap.keySet().iterator().next();
      int originalCapacity = originalFreeHeader.dataCapacity();
      int originalFreeSpace = freeMap.get(originalFreeHeader);

      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Original free header: capacity=%d, dataLength=%d, freeSpace=%d",
                  originalCapacity, originalFreeHeader.dataLength(), originalFreeSpace));

      // Insert a small record to trigger split
      byte[] key2 = "key2".getBytes();
      byte[] smallData = new byte[200];
      store.insertRecord(key2, smallData);

      // After split, check if the remaining free space is correct
      logger.log(
          Level.FINE,
          () -> String.format("Free map size after split: %d", freeMap.size()));

      // Calculate expected remaining space
      // payloadLength for 200 bytes = 200 + 4 (length prefix) = 204
      int payloadLength = 204;
      int expectedRemaining = originalFreeSpace - payloadLength;

      logger.log(
          Level.FINE,
          () ->
              String.format(
                  "Expected remaining free space: %d (original %d - payload %d)",
                  expectedRemaining, originalFreeSpace, payloadLength));

      // Check the actual remaining free space
      int actualRemaining = 0;
      for (RecordHeader header : freeMap.keySet()) {
        int freeSpace = freeMap.get(header);
        actualRemaining += freeSpace;
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Free entry after split: dataPointer=%d, dataLength=%d, dataCapacity=%d, freeSpace=%d",
                    header.dataPointer(), header.dataLength(), header.dataCapacity(), freeSpace));
        
        // Check if the capacity was properly reduced
        int calculatedFree = header.getFreeSpace(true);
        logger.log(
            Level.FINE,
            () ->
                String.format(
                    "Calculated free from header: %d, freeMap value: %d",
                    calculatedFree, freeSpace));
        
        // This will fail if the bug exists - capacity wasn't reduced
        Assert.assertEquals(
            "Calculated free space should match freeMap value",
            calculatedFree,
            freeSpace);
      }

      final int finalActualRemaining = actualRemaining;
      logger.log(
          Level.FINE,
          () -> String.format("Actual remaining free space: %d", finalActualRemaining));

      // After split, the newRecord gets the full free space capacity but only uses payloadLength
      // Free space = originalFreeSpace - payloadLength - overhead
      // With overhead of 4 bytes for length prefix: 1504 - 208 - 4 = 1292
      // We verify that free space is close to this expected value (within 10 bytes tolerance)
      int minExpected = originalFreeSpace - payloadLength - 20; // generous tolerance
      int maxExpected = originalFreeSpace - payloadLength;
      Assert.assertTrue(
          String.format(
              "Remaining free space (%d) should be between %d and %d",
              actualRemaining, minExpected, maxExpected),
          actualRemaining >= minExpected && actualRemaining <= maxExpected);

      store.close();
      logger.log(Level.FINE, "=== Test completed ===");

    } finally {
      Files.deleteIfExists(file);
    }
  }
}
