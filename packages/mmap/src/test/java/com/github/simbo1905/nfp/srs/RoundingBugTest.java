package com.github.simbo1905.nfp.srs;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

/// Test to prove the rounding bug where alignedKeyLength can exceed theoretical maximum
public class RoundingBugTest extends JulLoggingConfig {

  @Test
  public void testRoundingBugAtTheoreticalMax() throws Exception {
    // Use a key length that's exactly at the theoretical maximum (32763)
    // The current rounding logic would try to round this up, exceeding the limit
    int theoreticalMax = FileRecordStoreBuilder.MAX_KEY_LENGTH; // 32763

    Path tempFile = Files.createTempFile("rounding-bug-", ".db");
    tempFile.toFile().deleteOnExit();

    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(tempFile).maxKeyLength(theoreticalMax).open()) {

      // Create a key that's exactly at the theoretical maximum
      String maxKey = "1".repeat(theoreticalMax);
      byte[] key = maxKey.getBytes();
      byte[] value = "test-value".getBytes();

      // This should work without issues
      store.insertRecord(key, value);

      // Verify we can read it back
      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Should read back the same value", value, readValue);
    }
  }

  @Test
  public void testRoundingBugWith8ByteAlignment() throws Exception {
    // Use a key length that's 8 bytes less than theoretical max to test alignment
    int keyLength = FileRecordStoreBuilder.MAX_KEY_LENGTH - 8; // 32755

    Path tempFile = Files.createTempFile("rounding-alignment-", ".db");
    tempFile.toFile().deleteOnExit();

    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(tempFile).maxKeyLength(keyLength).open()) {

      // Create a key that's exactly at our chosen length
      String testKey = "A".repeat(keyLength);
      byte[] key = testKey.getBytes();
      byte[] value = "alignment-test-value".getBytes();

      // This should work without the rounding logic pushing us over the limit
      store.insertRecord(key, value);

      // Verify we can read it back
      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Should read back the same value", value, readValue);

      // Verify the key length is correct
      Assert.assertEquals("Key length should match", keyLength, key.length);
    }
  }

  @Test
  public void testRoundingBugWithExact8ByteMultiple() throws Exception {
    // Use a key length that's exactly an 8-byte multiple near the max
    int keyLength = (FileRecordStoreBuilder.MAX_KEY_LENGTH / 8) * 8; // 32760

    Path tempFile = Files.createTempFile("rounding-exact-", ".db");
    tempFile.toFile().deleteOnExit();

    try (FileRecordStore store =
        new FileRecordStoreBuilder().path(tempFile).maxKeyLength(keyLength).open()) {

      // Create a key that's exactly at our chosen length
      String testKey = "B".repeat(keyLength);
      byte[] key = testKey.getBytes();
      byte[] value = "exact-multiple-test-value".getBytes();

      // This should work perfectly since it's already aligned
      store.insertRecord(key, value);

      // Verify we can read it back
      byte[] readValue = store.readRecordData(key);
      Assert.assertArrayEquals("Should read back the same value", value, readValue);
    }
  }
}
