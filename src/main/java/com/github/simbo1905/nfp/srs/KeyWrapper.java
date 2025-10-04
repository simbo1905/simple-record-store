package com.github.simbo1905.nfp.srs;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/// Immutable wrapper for keys used internally by FileRecordStore.
/// Provides efficient hash code caching and optional defensive copying for byte arrays.
/// Supports both byte array and UUID keys with optimized storage.
///
/// @see FileRecordStore
/// @see KeyType
record KeyWrapper(byte[] bytes, int cachedHashCode) {
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        KeyWrapper that = (KeyWrapper) obj;
        return java.util.Arrays.equals(bytes, that.bytes);
    }
    
    @Override
    public int hashCode() {
        return cachedHashCode;
    }
    
    /// Creates a KeyWrapper from a byte array with optional defensive copying.
    /// When defensive copying is enabled, the byte array is cloned to prevent
    /// external mutation. The hash code is precomputed for efficient HashMap usage.
    ///
    /// @param bytes the byte array key
    /// @param copy whether to create a defensive copy of the byte array
    /// @return a new KeyWrapper instance
    /// @throws IllegalArgumentException if bytes is null
    static KeyWrapper of(byte[] bytes, boolean copy) {
        if (bytes == null) {
            throw new IllegalArgumentException("Key bytes cannot be null");
        }
        byte[] keyBytes = copy ? bytes.clone() : bytes;
        int cachedHashCode = Arrays.hashCode(keyBytes);
        return new KeyWrapper(keyBytes, cachedHashCode);
    }
    
    /// Creates a KeyWrapper from a UUID by converting it to a 16-byte array.
    /// This method provides efficient UUID storage without additional allocations.
    ///
    /// @param uuid the UUID key
    /// @return a new KeyWrapper instance
    /// @throws IllegalArgumentException if uuid is null
    static KeyWrapper of(UUID uuid) {
        if (uuid == null) {
            throw new IllegalArgumentException("UUID key cannot be null");
        }
        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits());
        int cachedHashCode = Arrays.hashCode(bytes);
        return new KeyWrapper(bytes, cachedHashCode);
    }
    
    /// Converts this KeyWrapper back to a UUID.
    /// Only valid if this wrapper was created from a 16-byte UUID.
    ///
    /// @return the UUID representation
    /// @throws IllegalStateException if bytes length is not 16
    UUID toUUID() {
        if (bytes.length != 16) {
            throw new IllegalStateException("Cannot convert " + bytes.length + " bytes to UUID (expected 16)");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }
    
    /// Returns the length of the key in bytes.
    ///
    /// @return the number of bytes in this key
    int length() {
        return bytes.length;
    }
    
    /// Creates a defensive copy of the internal byte array.
    ///
    /// @return a clone of the wrapped bytes
    byte[] copyBytes() {
        return bytes.clone();
    }
}
