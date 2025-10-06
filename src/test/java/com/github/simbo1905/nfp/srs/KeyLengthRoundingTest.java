package com.github.simbo1905.nfp.srs;

import org.junit.Test;
import org.junit.Assert;
import java.nio.file.Files;
import java.nio.file.Path;

/// Tests to prove the key length rounding bug where alignedKeyLength can exceed theoretical maximum
public class KeyLengthRoundingTest extends JulLoggingConfig {

  @Test
  public void testHalfMaxKeyLength16381Bytes() throws Exception {
    // Test with exactly half the theoretical maximum: 32763 / 2 = 16381 (rounded down)
    int halfMaxKeyLength = FileRecordStoreBuilder.MAX_KEY_LENGTH / 2; // 16381
    
    Path tempFile = Files.createTempFile("half-max-key-", ".db");
    tempFile.toFile().deleteOnExit();
    
    try (FileRecordStore store = new FileRecordStoreBuilder()
        .path(tempFile)
        .maxKeyLength(halfMaxKeyLength)
        .open()) {
      
      // Create a key that's exactly at half the maximum
      String halfMaxKey = "X".repeat(halfMaxKeyLength);
      byte[] key = halfMaxKey.getBytes();
      byte[] value = "half-max-test-value".getBytes();
      
      // This should work without rounding issues
      store.insertRecord(key, value);
      
      // Verify we can read it back correctly
      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Should read back the same value", value, readValue);
      
      // Verify the key length is preserved exactly
      Assert.assertEquals("Key length should be exactly half max", halfMaxKeyLength, key.length);
    }
  }

  @Test  
  public void testKeyLengthNearTheoreticalMax32763Bytes() throws Exception {
    // Test with exactly the theoretical maximum: 32763 bytes
    int theoreticalMax = FileRecordStoreBuilder.MAX_KEY_LENGTH; // 32763
    
    Path tempFile = Files.createTempFile("theoretical-max-key-", ".db");
    tempFile.toFile().deleteOnExit();
    
    try (FileRecordStore store = new FileRecordStoreBuilder()
        .path(tempFile)
        .maxKeyLength(theoreticalMax)
        .open()) {
      
      // Create a key that's exactly at the theoretical maximum
      String maxKey = "Y".repeat(theoreticalMax);
      byte[] key = maxKey.getBytes();
      byte[] value = "theoretical-max-test-value".getBytes();
      
      // This should work but will likely fail due to rounding bug
      store.insertRecord(key, value);
      
      // Verify we can read it back correctly
      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Should read back the same value", value, readValue);
      
      // Verify the key length is preserved exactly
      Assert.assertEquals("Key length should be exactly theoretical max", theoreticalMax, key.length);
    }
  }
}
