package com.github.simbo1905.srs;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.charset.StandardCharsets;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ByteSequence {
    final byte[] bytes;

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
     * This takes a defensive copy of the passed bytes. This should be used if the bytes may be recycled by the caller.
     */
    public static ByteSequence copyOf(byte[] bytes) {
        return new ByteSequence(bytes.clone());
    }

    /**
     * This does not take a defensive copy of the passed bytes. This should be used only if you know that the byte array cannot be recycled.
     */
    public static ByteSequence of(byte[] bytes) {
        return new ByteSequence(bytes);
    }

    /**
     * This returns a defensive copy.
     */
    public ByteSequence copy() {
        return ByteSequence.copyOf(bytes);
    }

    /**
     * This returns a defensive copy of the internal byte array.
     */
    public byte[] bytes() {
        return bytes.clone();
    }

    public long length() {
        return bytes.length;
    }
}
