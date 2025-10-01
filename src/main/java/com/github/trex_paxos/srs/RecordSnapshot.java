package com.github.trex_paxos.srs;

/// Captures the on-disk index ordering so crash tests can validate
/// structural invariants without exposing mutable headers.
record RecordSnapshot(
        int indexPosition,
        ByteSequence key,
        long dataPointer,
        int dataCapacity,
        int dataCount,
        long crc32
) {
}
