package com.github.simbo1905.srs;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
A ByteSequence is a wrapper to a byte array that adds equals and hashcode so that it can be used as the key in a map.
As we intend for it to be used as the key in a map it should be immutable. This means you should construct it using
 the static copyOf method. If you are sure the the byte array you are wrapping will never be mutated you can use the
 static "of" method.
 */
@ToString
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

    // here we memoize the hashcode upon first use. when the map is resized the cached value will reused.
    @Getter(lazy=true)
    private final int hashCode = Arrays.hashCode(bytes);

    @Override
    public int hashCode() {
        return this.getHashCode();
    }

    /**
     * This encodes a string into a fresh UTF8 byte array wrapped as a ByteString. Note that this copies data.
     * @param string A string
     * @return ByteString wrapping a UTF8 byte array generated from the input string.
     */
    public static ByteSequence stringToUtf8(String string){
        return of(string.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * This decodes a UTF8 byte array wrapped in a ByteString into a string. Note that this copies data.
     * @param utf8 A ByteString wrapping a UTF8 encoded string.
     * @return A String that has decoded and copied the data into its internal state.
     */
    public static String utf8ToString(ByteSequence utf8){
        return utf8ToString(utf8.bytes);
    }

    /**
     * This decodes a UTF8 byte array into a string. Note that this copies data.
     * @param utf8 A ByteString wrapping a UTF8 encoded string.
     * @return A String that has decoded and copied the data into its internal state.
     */
    public static String utf8ToString(byte[] utf8){
        return new String(utf8, StandardCharsets.UTF_8);
    }

    /**
     * This takes a defensive copy of the passed bytes. This should be used if the array can be recycled by the caller.
     * @param bytes The bytes to copy.
     */
    public static ByteSequence copyOf(byte[] bytes) {
        return new ByteSequence(bytes.clone());
    }

    /**
     * This does not take a defensive copy of the passed bytes. This should be used only if you know that the array cannot be recycled.
     * @param bytes The bytes to copy.
     */
    public static ByteSequence of(byte[] bytes) {
        return new ByteSequence(bytes);
    }

    /**
     * This returns a defensive copy.:x
     * @return A deep copy of this sequence.
     */
    public ByteSequence copy() {
        return ByteSequence.copyOf(bytes);
    }

    /**
     * This returns a defensive copy of the internal byte array.
     * @return A copy of the wrapped bytes.
     */
    public byte[] bytes() {
        return bytes.clone();
    }

    public long length() {
        return bytes.length;
    }
}
