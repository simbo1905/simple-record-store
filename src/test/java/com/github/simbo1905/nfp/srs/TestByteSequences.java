package com.github.simbo1905.nfp.srs;

import java.nio.charset.StandardCharsets;

/**
 * Test-only helpers for working with {@link ByteSequence} using UTF-8 encoding.
 */
public final class TestByteSequences {
    private TestByteSequences() {
    }

    public static ByteSequence fromUtf8(String value) {
        return ByteSequence.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String toUtf8String(byte[] utf8Bytes) {
        return new String(utf8Bytes, StandardCharsets.UTF_8);
    }

    public static String toUtf8String(ByteSequence utf8) {
        return toUtf8String(utf8.bytes);
    }
}
