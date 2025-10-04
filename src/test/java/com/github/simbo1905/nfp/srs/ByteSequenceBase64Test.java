package com.github.simbo1905.nfp.srs;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests for Base64 encoding/decoding in ByteSequence to ensure safe handling
 * of arbitrary binary data including UUIDs, hashes, encrypted data, and random bytes.
 */
public class ByteSequenceBase64Test {

    @Test
    public void testBase64RoundTrip() {
        byte[] data = "Hello, World!".getBytes();
        ByteSequence seq = ByteSequence.of(data);
        
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Round-trip should preserve data", data, decoded.bytes);
    }

    @Test
    public void testToStringUsesBase64() {
        byte[] data = "Test Data".getBytes();
        ByteSequence seq = ByteSequence.of(data);
        
        String toString = seq.toString();
        String toBase64 = seq.toBase64();
        
        assertEquals("toString() should return Base64 encoding", toBase64, toString);
    }

    @Test
    public void testRandomBytesRoundTrip() {
        Random random = new Random(42);
        byte[] randomBytes = new byte[256];
        random.nextBytes(randomBytes);
        
        ByteSequence seq = ByteSequence.of(randomBytes);
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Random bytes must round-trip perfectly", randomBytes, decoded.bytes);
    }

    @Test
    public void testUUIDRoundTrip() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        byte[] uuidBytes = bb.array();
        
        ByteSequence seq = ByteSequence.of(uuidBytes);
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("UUID bytes must round-trip perfectly", uuidBytes, decoded.bytes);
    }

    @Test
    public void testHashOutputRoundTrip() throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] hash = sha256.digest("test data".getBytes());
        
        ByteSequence seq = ByteSequence.of(hash);
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Hash output must round-trip perfectly", hash, decoded.bytes);
    }

    @Test
    public void testEmptyByteArray() {
        byte[] empty = new byte[0];
        ByteSequence seq = ByteSequence.of(empty);
        
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Empty array must round-trip", empty, decoded.bytes);
        assertEquals("Empty array should encode to empty string", "", base64);
    }

    @Test
    public void testSingleByte() {
        for (int i = 0; i < 256; i++) {
            byte[] single = new byte[]{(byte) i};
            ByteSequence seq = ByteSequence.of(single);
            
            String base64 = seq.toBase64();
            ByteSequence decoded = ByteSequence.fromBase64(base64);
            
            assertArrayEquals("Single byte value " + i + " must round-trip", single, decoded.bytes);
        }
    }

    @Test
    public void testInvalidUTF8Sequences() {
        // These byte sequences are invalid UTF-8 but should encode/decode perfectly via Base64
        byte[][] invalidUtf8 = {
            {(byte) 0xFF, (byte) 0xFE},  // Invalid UTF-8 start bytes
            {(byte) 0x80, (byte) 0x81},  // Continuation bytes without starter
            {(byte) 0xC0, (byte) 0x00},  // Overlong encoding
            {(byte) 0xED, (byte) 0xA0, (byte) 0x80},  // UTF-16 surrogate
        };
        
        for (byte[] invalid : invalidUtf8) {
            ByteSequence seq = ByteSequence.of(invalid);
            String base64 = seq.toBase64();
            ByteSequence decoded = ByteSequence.fromBase64(base64);
            
            assertArrayEquals("Invalid UTF-8 must round-trip via Base64", invalid, decoded.bytes);
        }
    }

    @Test
    public void testLargeDataRoundTrip() {
        // Test with 1MB of random data
        Random random = new Random(12345);
        byte[] largeData = new byte[1024 * 1024];
        random.nextBytes(largeData);
        
        ByteSequence seq = ByteSequence.of(largeData);
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Large data must round-trip perfectly", largeData, decoded.bytes);
    }

    @Test
    public void testBase64UsesStandardEncoding() {
        byte[] data = {0, 1, 2, 3, 4, 5};
        ByteSequence seq = ByteSequence.of(data);
        
        String base64 = seq.toBase64();
        String expected = java.util.Base64.getEncoder().encodeToString(data);
        
        assertEquals("Should use standard Base64 encoding", expected, base64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromBase64WithInvalidInput() {
        // This is not valid Base64
        ByteSequence.fromBase64("not-valid-base64!!!");
    }

    @Test
    public void testHashCodeConsistency() {
        byte[] data = "test".getBytes();
        ByteSequence seq1 = ByteSequence.of(data);
        
        String base64 = seq1.toBase64();
        ByteSequence seq2 = ByteSequence.fromBase64(base64);
        
        assertEquals("Hash codes must match for equal sequences", seq1.hashCode(), seq2.hashCode());
        assertEquals("Sequences must be equal", seq1, seq2);
    }

    @Test
    public void testNullByteHandling() {
        byte[] withNulls = {0, 1, 0, 2, 0, 3};
        ByteSequence seq = ByteSequence.of(withNulls);
        
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Null bytes must be preserved", withNulls, decoded.bytes);
    }

    @Test
    public void testControlCharacters() {
        // Test with various control characters
        byte[] controls = new byte[32];
        for (int i = 0; i < 32; i++) {
            controls[i] = (byte) i;
        }
        
        ByteSequence seq = ByteSequence.of(controls);
        String base64 = seq.toBase64();
        ByteSequence decoded = ByteSequence.fromBase64(base64);
        
        assertArrayEquals("Control characters must round-trip", controls, decoded.bytes);
    }
}
