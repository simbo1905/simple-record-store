package com.github.simbo1905.nfp.srs;

import java.nio.file.Files;
import java.security.MessageDigest;
import org.junit.Assert;
import org.junit.Test;

/// Test to verify header size calculation works correctly with different key lengths including
// SHA256
public class HeaderSizeTest {

  @Test
  public void testHeaderSizeWithSHA256Keys() throws Exception {
    // SHA256 produces 32-byte hashes
    byte[] sha256Key = MessageDigest.getInstance("SHA-256").digest("test".getBytes());
    Assert.assertEquals("SHA256 should be 32 bytes", 32, sha256Key.length);

    // Create store with SHA256-sized keys
    var tempFile = Files.createTempFile("sha256-test", ".db");
    try (var store =
        new FileRecordStoreBuilder()
            .path(tempFile)
            .maxKeyLength(32) // SHA256 size
            .open()) {

      // Should be able to insert SHA256 key
      store.insertRecord(sha256Key, "test data".getBytes());

      // Should be able to read it back
      byte[] readData = store.readRecordData(sha256Key);
      Assert.assertArrayEquals("Data should match", "test data".getBytes(), readData);
    }
    Files.delete(tempFile);
  }

  @Test
  public void testHeaderSizeWithLargerKeys() throws Exception {
    // Test with 64-byte keys (SHA512 size)
    byte[] largeKey = new byte[64];
    for (int i = 0; i < 64; i++) {
      largeKey[i] = (byte) i;
    }

    var tempFile = Files.createTempFile("large-key-test", ".db");
    try (var store = new FileRecordStoreBuilder().path(tempFile).maxKeyLength(64).open()) {

      store.insertRecord(largeKey, "large key test data".getBytes());
      byte[] readData = store.readRecordData(largeKey);
      Assert.assertArrayEquals("Data should match", "large key test data".getBytes(), readData);
    }
    Files.delete(tempFile);
  }
}
