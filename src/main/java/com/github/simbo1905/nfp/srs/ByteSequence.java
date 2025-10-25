package com.github.simbo1905.nfp.srs;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// Immutable wrapper for byte sequences used as keys in the record store.
/// Provides efficient hash code caching and defensive copying options.
/// Supports safe string representation for arbitrary binary data using hex encoding.
public record ByteSequence(byte[] bytes, int cachedHashCode) {

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ByteSequence that = (ByteSequence) obj;
    return Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return cachedHashCode;
  }

  /// Creates a ByteSequence with defensive copy of the byte array.
  /// This should be used if the caller can recycle the array.
  ///
  /// @param bytes the byte array to copy
  /// @return a new ByteSequence instance with copied bytes
  /// @throws IllegalArgumentException if bytes is null
  public static ByteSequence copyOf(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes cannot be null");
    }
    byte[] copy = bytes.clone();
    int cachedHashCode = Arrays.hashCode(copy);
    return new ByteSequence(copy, cachedHashCode);
  }

  /// Creates a ByteSequence without defensive copy of the byte array.
  /// This should be used only if you know that the array cannot be recycled.
  ///
  /// @param bytes the byte array to wrap
  /// @return a new ByteSequence instance wrapping the bytes
  /// @throws IllegalArgumentException if bytes is null
  public static ByteSequence of(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes cannot be null");
    }
    int cachedHashCode = Arrays.hashCode(bytes);
    return new ByteSequence(bytes, cachedHashCode);
  }

  /// Encodes a string into UTF8 byte array wrapped as a ByteSequence.
  /// This encodes a string into a fresh UTF8 byte array wrapped as a ByteSequence.
  /// Note that this copies data.
  /// Use this only when you have a valid UTF-8 string, not arbitrary binary data.
  ///
  /// @param string the string to encode
  /// @return a new ByteSequence with UTF-8 encoded bytes
  /// @throws IllegalArgumentException if string is null
  public static ByteSequence stringToUtf8(String string) {
    if (string == null) {
      throw new IllegalArgumentException("String cannot be null");
    }
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return of(bytes);
  }

  /// Decodes UTF8 byte array wrapped in a ByteSequence into a string.
  /// This decodes a UTF8 byte array wrapped in a ByteSequence into a string.
  /// Note that this copies data.
  /// Use this only when the ByteSequence contains valid UTF-8 encoded text data.
  /// For arbitrary binary data, use toString() which provides hex representation.
  ///
  /// @param utf8 the ByteSequence containing UTF-8 bytes
  /// @return the decoded string
  /// @throws IllegalArgumentException if utf8 is null
  public static String utf8ToString(ByteSequence utf8) {
    if (utf8 == null) {
      throw new IllegalArgumentException("ByteSequence cannot be null");
    }
    return new String(utf8.bytes, StandardCharsets.UTF_8);
  }

  /// Returns a hex string representation of this ByteSequence.
  /// Safe for arbitrary binary data - preserves all byte values without corruption.
  /// Format: "[ 0xAB 0xCD 0xEF ... ]" for debugging and logging.
  ///
  /// @return hex string representation preserving all byte information
  @Override
  public String toString() {
    return toHexString(bytes);
  }

  /// Converts byte array to hex string format for safe display of arbitrary binary data.
  /// Uses format "[ 0xAB 0xCD ... ]" consistent with FileRecordStore.print().
  ///
  /// @param bytes the byte array to format
  /// @return hex string representation
  private static String toHexString(byte[] bytes) {
    if (bytes.length == 0) {
      return "[ ]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (byte b : bytes) {
      sb.append(String.format("0x%02X ", b));
    }
    sb.append("]");
    return sb.toString();
  }

  /// Returns the length of the byte sequence.
  ///
  /// @return the number of bytes
  public int length() {
    return bytes.length;
  }

  /// Creates a defensive copy of the internal byte array.
  ///
  /// @return a clone of the wrapped bytes
  public byte[] copyBytes() {
    return bytes.clone();
  }
}
