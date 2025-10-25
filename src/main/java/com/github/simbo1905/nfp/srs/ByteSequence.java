package com.github.simbo1905.nfp.srs;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// Immutable wrapper for byte sequences used as keys in the record store.
/// Provides efficient hash code caching and defensive copying options.
/// BUGGY VERSION: Contains unsafe UTF-8 string conversions that corrupt non-textual data.
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

  /// BUGGY: Encodes a string into UTF8 byte array wrapped as a ByteSequence.
  /// This encodes a string into a fresh UTF8 byte array wrapped as a ByteString.
  /// Note that this copies data.
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

  /// BUGGY: Decodes UTF8 byte array wrapped in a ByteSequence into a string.
  /// This decodes a UTF8 byte array wrapped in a ByteString into a string.
  /// Note that this copies data.
  /// WARNING: This method silently corrupts invalid UTF-8 sequences!
  ///
  /// @param utf8 the ByteSequence containing UTF-8 bytes
  /// @return the decoded string
  /// @throws IllegalArgumentException if utf8 is null
  public static String utf8ToString(ByteSequence utf8) {
    if (utf8 == null) {
      throw new IllegalArgumentException("ByteSequence cannot be null");
    }
    // BUG: new String(bytes) silently replaces invalid UTF-8 with U+FFFD
    return new String(utf8.bytes, StandardCharsets.UTF_8);
  }

  /// BUGGY: Returns a string representation of this ByteSequence.
  /// WARNING: This corrupts non-UTF-8 data!
  ///
  /// @return string representation (CORRUPTED for binary data)
  @Override
  public String toString() {
    // BUG: new String(bytes) corrupts arbitrary byte data
    return new String(bytes, StandardCharsets.UTF_8);
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
