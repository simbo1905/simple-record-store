package com.github.simbo1905.nfp.srs;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;

/// Test demonstrating UTF-8 conversion bugs in ByteSequence.
/// These tests SHOULD FAIL to prove the bug exists.
public class ByteSequenceUtf8CorruptionTest {

  /// Test that demonstrates data corruption when converting arbitrary bytes to string and back.
  /// This test SHOULD FAIL because the round-trip through String corrupts the data.
  @Test
  public void testArbitraryBytesCorruptedByStringConversion() {
    // Create a byte array with invalid UTF-8 sequences
    byte[] original = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0x80, (byte) 0x00, 0x42};

    // Convert to ByteSequence
    ByteSequence seq = ByteSequence.of(original);

    // BUG: toString() corrupts the data
    String corrupted = seq.toString();

    // BUG: Try to convert back - this will NOT give us the original bytes
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL - proving the bug exists
    assertArrayEquals(
        "Round-trip through String should preserve bytes but doesn't!", original, roundtrip);
  }

  /// Test that demonstrates UUID bytes being corrupted by toString().
  /// UUIDs contain arbitrary byte values that are not valid UTF-8.
  /// This test SHOULD FAIL.
  @Test
  public void testUuidBytesCorruptedByToString() {
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

    // BUG: toString() corrupts the UUID bytes
    String corrupted = seq.toString();
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL for most UUIDs
    assertArrayEquals(
        "UUID bytes should survive toString() but are corrupted!", uuidBytes, roundtrip);
  }

  /// Test that demonstrates SHA-256 hash corruption.
  /// Cryptographic hashes contain arbitrary bytes that are not valid UTF-8.
  /// This test SHOULD FAIL.
  @Test
  public void testSha256HashCorruptedByToString() throws NoSuchAlgorithmException {
    // Generate a SHA-256 hash
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest("test data".getBytes(StandardCharsets.UTF_8));

    ByteSequence seq = ByteSequence.of(hash);

    // BUG: toString() corrupts the hash
    String corrupted = seq.toString();
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL - hash bytes are corrupted
    assertArrayEquals("SHA-256 hash should survive toString() but is corrupted!", hash, roundtrip);
  }

  /// Test that demonstrates random byte corruption.
  /// Random bytes are statistically likely to contain invalid UTF-8.
  /// This test SHOULD FAIL.
  @Test
  public void testRandomBytesCorruptedByToString() {
    Random random = new Random(12345); // fixed seed for reproducibility
    byte[] randomBytes = new byte[32];
    random.nextBytes(randomBytes);

    ByteSequence seq = ByteSequence.of(randomBytes);

    // BUG: toString() corrupts random bytes
    String corrupted = seq.toString();
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL - random bytes contain invalid UTF-8
    assertArrayEquals(
        "Random bytes should survive toString() but are corrupted!", randomBytes, roundtrip);
  }

  /// Test that demonstrates utf8ToString() corruption.
  /// When arbitrary bytes are passed to utf8ToString(), they get corrupted.
  /// This test SHOULD FAIL.
  @Test
  public void testUtf8ToStringCorruptsArbitraryBytes() {
    // Create bytes that are NOT valid UTF-8
    byte[] invalidUtf8 = new byte[] {(byte) 0x80, (byte) 0xFF, (byte) 0xC0};

    ByteSequence seq = ByteSequence.of(invalidUtf8);

    // BUG: utf8ToString() silently corrupts invalid UTF-8
    String result = ByteSequence.utf8ToString(seq);

    // Convert back to bytes
    byte[] roundtrip = result.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL - data is corrupted
    assertArrayEquals(
        "utf8ToString() should preserve bytes but corrupts them!", invalidUtf8, roundtrip);
  }

  /// Test that demonstrates the debug/logging problem.
  /// When trying to debug issues, toString() shows replacement characters instead of actual bytes.
  /// This test documents the expected BROKEN behavior.
  @Test
  public void testToStringShowsReplacementCharactersInsteadOfActualBytes() {
    // Create bytes with invalid UTF-8
    byte[] bytes = new byte[] {(byte) 0xFF, (byte) 0xFE};

    ByteSequence seq = ByteSequence.of(bytes);

    // BUG: toString() shows replacement character � (U+FFFD) instead of actual hex values
    String result = seq.toString();

    // The string will contain replacement characters, NOT hex representation
    // This makes debugging impossible
    assertTrue(
        "toString() should contain replacement character showing the bug",
        result.contains("\uFFFD"));

    // What we WANT is something like "[0xFF, 0xFE]" or "FF FE"
    // But we DON'T get that - this test proves the bug
    assertFalse(
        "toString() incorrectly contains replacement chars instead of hex", result.contains("FF"));
  }

  /// Test demonstrating that encrypted key material gets corrupted.
  /// Encrypted data is arbitrary bytes that will be corrupted by UTF-8 conversion.
  /// This test SHOULD FAIL.
  @Test
  public void testEncryptedKeyMaterialCorrupted() {
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

    // BUG: toString() corrupts encrypted material
    String corrupted = seq.toString();
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL
    assertArrayEquals(
        "Encrypted key material should survive toString() but is corrupted!",
        encryptedKey,
        roundtrip);
  }

  /// Test showing that even a single invalid byte corrupts the data.
  /// This test SHOULD FAIL.
  @Test
  public void testSingleInvalidByteCorruptsData() {
    // A single byte that's not valid UTF-8
    byte[] singleByte = new byte[] {(byte) 0xFF};

    ByteSequence seq = ByteSequence.of(singleByte);

    // BUG: toString() corrupts even a single byte
    String corrupted = seq.toString();
    byte[] roundtrip = corrupted.getBytes(StandardCharsets.UTF_8);

    // This assertion SHOULD FAIL
    assertArrayEquals(
        "Single invalid byte should be preserved but is corrupted!", singleByte, roundtrip);
  }

  /// Test demonstrating data loss is irreversible.
  /// Once corrupted through string conversion, original data cannot be recovered.
  /// This test documents the severity of the bug.
  @Test
  public void testDataLossIsIrreversible() {
    byte[] original = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};

    ByteSequence seq = ByteSequence.of(original);
    String corrupted = seq.toString();

    // Try multiple approaches to recover - none work
    byte[] attempt1 = corrupted.getBytes(StandardCharsets.UTF_8);
    byte[] attempt2 = corrupted.getBytes(StandardCharsets.ISO_8859_1);
    byte[] attempt3 = corrupted.getBytes();

    // All attempts SHOULD FAIL to recover original data
    assertFalse("UTF-8 roundtrip fails to preserve data", Arrays.equals(original, attempt1));
    assertFalse("ISO-8859-1 also fails to recover data", Arrays.equals(original, attempt2));
    assertFalse("Platform default encoding also fails", Arrays.equals(original, attempt3));

    // This proves the data loss is PERMANENT and IRREVERSIBLE
  }
}
