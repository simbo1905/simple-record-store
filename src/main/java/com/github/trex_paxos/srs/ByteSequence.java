package com.github.trex_paxos.srs;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Base64;

/// A ByteSequence is a wrapper to a byte array that adds equals and hashcode so
/// that it can be used as the key in a map. As we intend for it to be used as
/// the key in a map it should be immutable. This means you should construct it
/// using the static copyOf method. If you are sure the byte array you are
/// wrapping will never be mutated you can use the static `of` method.
@RequiredArgsConstructor
public class ByteSequence {
    final byte[] bytes;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteSequence that = (ByteSequence) o;
        return Arrays.equals(bytes, that.bytes);
    }

    // here we memoize the hashcode upon first use. when the map is resized the cached value will be reused.
    @Getter(lazy=true)
    private final int hashCode = Arrays.hashCode(bytes);

    @Override
    public int hashCode() {
        return this.getHashCode();
    }

    /**
     * Returns a Base64-encoded string representation of the byte array.
     * This is safe for arbitrary binary data including UUIDs, hashes, and encrypted data.
     * The string can be decoded back to the original bytes using fromBase64().
     * 
     * @return Base64-encoded string representation
     */
    @Override
    public String toString() {
        return toBase64();
    }

    /**
     * Encodes this ByteSequence as a Base64 string.
     * This is safe for any binary data and guarantees perfect round-trip conversion.
     * 
     * @return Base64-encoded string representation
     */
    public String toBase64() {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Decodes a Base64 string into a ByteSequence.
     * 
     * @param base64 Base64-encoded string
     * @return ByteSequence containing the decoded bytes
     * @throws IllegalArgumentException if the string is not valid Base64
     */
    public static ByteSequence fromBase64(String base64) {
        return of(Base64.getDecoder().decode(base64));
    }

    /// This takes a defensive copy of the passed bytes. This should be used if
    /// the array can be recycled by the caller.
    /// @param bytes The bytes to copy.
    public static ByteSequence copyOf(byte[] bytes) {
        return new ByteSequence(bytes.clone());
    }

    /// This does not take a defensive copy of the passed bytes. This should be
    /// used only if you know that the array cannot be recycled.
    /// @param bytes The bytes to copy.
    public static ByteSequence of(byte[] bytes) {
        return new ByteSequence(bytes);
    }

    /// This returns a defensive copy.:x
    /// @return A deep copy of this sequence.
    public ByteSequence copy() {
        return ByteSequence.copyOf(bytes);
    }

    /// This returns a defensive copy of the internal byte array.
    /// TODO why is this unused
    /// @return A copy of the wrapped bytes.
    @SuppressWarnings("unused")
    public byte[] bytes() {
        return bytes.clone();
    }

    public long length() {
        return bytes.length;
    }
}
