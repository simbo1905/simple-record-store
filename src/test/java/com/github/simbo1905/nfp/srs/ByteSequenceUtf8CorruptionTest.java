package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;

/// Test verifying ByteSequence correctly handles arbitrary binary data.
/// These tests verify the fix for UTF-8 corruption bug.
public class ByteSequenceUtf8CorruptionTest {

  /// Test that verifies toString() preserves arbitrary bytes through hex formatting.
  /// After fix: toString() returns hex format, not corrupted UTF-8.
  @Test
  public void testArbitraryBytesPreservedByHexToString() {
    // Create a byte array with invalid UTF-8 sequences
    byte[] original = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0x80, (byte) 0x00, 0x42};

    // Convert to ByteSequence
    ByteSequence seq = ByteSequence.of(original);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it contains hex representation
    assertTrue("toString() should contain hex format", hexString.contains("0xFF"));
    assertTrue("toString() should contain hex format", hexString.contains("0xFE"));
    assertTrue("toString() should contain hex format", hexString.contains("0x80"));
    assertTrue("toString() should contain hex format", hexString.contains("0x00"));
    assertTrue("toString() should contain hex format", hexString.contains("0x42"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies UUID bytes are safely represented via hex toString().
  /// After fix: toString() returns hex format showing all UUID bytes.
  @Test
  public void testUuidBytesPreservedByHexToString() {
    // Generate a random UUID
    UUID uuid = UUID.randomUUID();

    // Convert to 16-byte array
    byte[] uuidBytes = new byte[16];
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      uuidBytes[i] = (byte) (msb >>> (8 * (7 - i)));
      uuidBytes[8 + i] = (byte) (lsb >>> (8 * (7 - i)));
    }

    ByteSequence seq = ByteSequence.of(uuidBytes);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it's in hex format (contains 0x prefix)
    assertTrue("toString() should use hex format", hexString.contains("0x"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies SHA-256 hashes are safely represented via hex toString().
  /// After fix: toString() returns hex format showing all hash bytes.
  @Test
  public void testSha256HashPreservedByHexToString() throws NoSuchAlgorithmException {
    // Generate a SHA-256 hash
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest("test data".getBytes(StandardCharsets.UTF_8));

    ByteSequence seq = ByteSequence.of(hash);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it's in hex format
    assertTrue("toString() should use hex format", hexString.contains("0x"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies random bytes are safely represented via hex toString().
  /// After fix: toString() returns hex format showing all random bytes.
  @Test
  public void testRandomBytesPreservedByHexToString() {
    Random random = new Random(12345); // fixed seed for reproducibility
    byte[] randomBytes = new byte[32];
    random.nextBytes(randomBytes);

    ByteSequence seq = ByteSequence.of(randomBytes);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it's in hex format
    assertTrue("toString() should use hex format", hexString.contains("0x"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies utf8ToString() works correctly for valid UTF-8 data.
  /// utf8ToString() is specifically for decoding UTF-8 encoded strings.
  @Test
  public void testUtf8ToStringWorksForValidUtf8() {
    // Create valid UTF-8 data
    String original = "Hello, 世界!";
    byte[] utf8Bytes = original.getBytes(StandardCharsets.UTF_8);

    ByteSequence seq = ByteSequence.of(utf8Bytes);

    // utf8ToString() should correctly decode valid UTF-8
    String decoded = ByteSequence.utf8ToString(seq);

    assertEquals("utf8ToString() should decode valid UTF-8", original, decoded);
  }

  /// Test that verifies toString() shows hex representation instead of replacement characters.
  /// After fix: toString() uses hex format for reliable debugging.
  @Test
  public void testToStringShowsHexInsteadOfReplacementCharacters() {
    // Create bytes with invalid UTF-8
    byte[] bytes = new byte[] {(byte) 0xFF, (byte) 0xFE};

    ByteSequence seq = ByteSequence.of(bytes);

    // FIXED: toString() returns hex representation
    String result = seq.toString();

    // Should contain hex values
    assertTrue("toString() should contain hex FF", result.contains("0xFF"));
    assertTrue("toString() should contain hex FE", result.contains("0xFE"));

    // Should NOT contain replacement character
    assertFalse("toString() should not contain replacement chars", result.contains("\uFFFD"));
  }

  /// Test that verifies encrypted key material is safely represented via hex toString().
  /// After fix: toString() returns hex format showing all encrypted bytes.
  @Test
  public void testEncryptedKeyMaterialPreservedByHexToString() {
    // Simulate encrypted key material (arbitrary bytes)
    byte[] encryptedKey =
        new byte[] {
          0x3F,
          (byte) 0xA2,
          0x7E,
          (byte) 0xC1,
          (byte) 0x89,
          0x4F,
          (byte) 0xBE,
          0x23,
          (byte) 0xD7,
          0x56,
          (byte) 0xF0,
          0x12,
          (byte) 0x9A,
          0x6C,
          (byte) 0xE4,
          0x01
        };

    ByteSequence seq = ByteSequence.of(encryptedKey);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it's in hex format
    assertTrue("toString() should use hex format", hexString.contains("0x"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies even a single byte is safely represented via hex toString().
  /// After fix: toString() returns hex format for all bytes.
  @Test
  public void testSingleBytePreservedByHexToString() {
    // A single byte that's not valid UTF-8
    byte[] singleByte = new byte[] {(byte) 0xFF};

    ByteSequence seq = ByteSequence.of(singleByte);

    // FIXED: toString() now returns hex format
    String hexString = seq.toString();

    // Verify it contains hex representation
    assertTrue("toString() should contain 0xFF", hexString.contains("0xFF"));

    // Verify no replacement characters
    assertFalse("toString() should not contain replacement chars", hexString.contains("\uFFFD"));
  }

  /// Test that verifies hex format enables data inspection and debugging.
  /// After fix: toString() returns hex that contains all original byte information.
  @Test
  public void testHexFormatPreservesAllByteInformation() {
    byte[] original = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};

    ByteSequence seq = ByteSequence.of(original);
    String hexString = seq.toString();

    // FIXED: toString() now returns hex format with all byte information
    assertTrue("Hex format should contain 0xFF", hexString.contains("0xFF"));
    assertTrue("Hex format should contain 0xFE", hexString.contains("0xFE"));
    assertTrue("Hex format should contain 0xFD", hexString.contains("0xFD"));

    // Hex format is not meant for round-trip conversion - it's for debugging
    // The original byte information is visible and not corrupted
    assertFalse("Hex format should not contain replacement chars", hexString.contains("\uFFFD"));
  }
}
