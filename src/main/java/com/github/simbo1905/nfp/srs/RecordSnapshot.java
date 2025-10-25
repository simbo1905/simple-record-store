package com.github.simbo1905.nfp.srs;

/// Captures the on-disk index ordering so crash tests can validate
/// structural invariants without exposing mutable headers.
record RecordSnapshot(
        int indexPosition,
        KeyWrapper key,
        long dataPointer,
        int dataCapacity,
        int dataLength,
        long crc32
) {
}
