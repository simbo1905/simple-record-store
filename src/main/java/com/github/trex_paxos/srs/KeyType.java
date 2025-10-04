package com.github.trex_paxos.srs;

/// Key type enumeration for optimized key handling in FileRecordStore.
/// This enum enables JIT optimization by providing constant key type after construction.
///
/// @see FileRecordStore
/// @see KeyWrapper
enum KeyType {
    /// Standard byte array keys - general purpose binary key support
    BYTE_ARRAY,
    
    /// UUID keys - optimized for 16-byte UUID storage and retrieval
    UUID
}