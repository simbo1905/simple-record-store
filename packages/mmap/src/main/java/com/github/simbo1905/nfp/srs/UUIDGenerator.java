// SPDX-FileCopyrightText: 2024 - 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package com.github.simbo1905.nfp.srs;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/// Generator for time-based UUIDs with sub-millisecond ordering within a single JVM.
/// Provides good time-based ordering while maintaining global uniqueness.
///
/// Supports multiple UUID generation strategies:
/// 1. UUIDv7 (default): Standards-compliant time-ordered UUIDs per RFC 4122
/// 2. uniqueThenTime: Custom strategy with unique MSB and time+random in LSB
/// 3. Legacy: Original time+counter in MSB, random in LSB
///
/// The default strategy can be configured via system property:
/// - com.github.simbo1905.nfp.srs.UUIDGenerator.strategy
/// - Values: "uuidv7" (default), "uniqueThenTime", "legacy"
///
/// The Java UUID library lets us create a UUID from two longs.
/// In the most significant long we put the time in milliseconds.
/// We then bit shift the time left by 20 bits and mask in a counter.
/// This gives us good time based ordering within a single JVM.
/// The ordering across servers will naturally be subject to clock drift between hosts.
/// For the last significant bits we use a pure random long to makes the UUIDs globally unique.
/// The RFC for time based UUIDs suggest that 10M UUIDs per second can be generated. On an M1 Mac
/// the Java core Type 4
/// pure random UUID generation gives me about 0.6M per second. This class gets about 0.5M per
/// second.
/// High-performance UUID generator that creates time-ordered UUIDs suitable for database keys.
public class UUIDGenerator {
  /// A trick from the core UUID class is to use holder class to defer initialization until needed.
  private static class LazyRandom {
    static final SecureRandom RANDOM = new SecureRandom();
  }

  private static final AtomicLong sequence = new AtomicLong();

  /// Strategy for UUID generation
  public enum Strategy {
    UUIDV7,
    UNIQUE_THEN_TIME,
    LEGACY
  }

  private static final Strategy DEFAULT_STRATEGY;
  private static final long UNIQUE_MSB; // For UNIQUE_THEN_TIME strategy

  static {
    String strategyProp =
        System.getProperty("com.github.simbo1905.nfp.srs.UUIDGenerator.strategy", "uuidv7");
    Strategy strategy;
    try {
      strategy = Strategy.valueOf(strategyProp.toUpperCase().replace("THENTIME", "_THEN_TIME"));
    } catch (IllegalArgumentException e) {
      strategy = Strategy.UUIDV7;
    }
    DEFAULT_STRATEGY = strategy;

    // For UNIQUE_THEN_TIME strategy, generate a stable unique MSB at startup
    // This could be based on MAC address, hostname hash, or random value
    UNIQUE_MSB = LazyRandom.RANDOM.nextLong();
  }

  /// Gets the configured default strategy
  public static Strategy getDefaultStrategy() {
    return DEFAULT_STRATEGY;
  }

  private static SecureRandom getRandom() {
    return LazyRandom.RANDOM;
  }

  /// This takes the Unix/Java epoch time in milliseconds, bit shifts it left by 20 bits, and then
  // masks in the least
  /// significant 20 bits of the local counter. That gives us a million unique values per
  // millisecond.
  static long epochTimeThenCounterMsb() {
    long currentMillis = System.currentTimeMillis();
    // Take the least significant 20 bits from our atomic sequence
    long counter20bits = sequence.incrementAndGet() & 0xFFFFF;
    return (currentMillis << 20) | counter20bits;
  }

  /// Returns a 64-bit value combining timestamp and counter for time-ordering
  static long timeCounterBits() {
    long currentMillis = System.currentTimeMillis();
    long counter = sequence.incrementAndGet();
    // Combine timestamp and counter: timestamp in upper bits, counter in lower bits
    return (currentMillis << 20) | (counter & 0xFFFFF);
  }

  /// Creates a type 7 UUID (UUIDv7) {@code UUID} from the given Unix Epoch timestamp.
  ///
  /// The returned {@code UUID} will have the given {@code timestamp} in
  /// the first 6 bytes, followed by the version and variant bits representing {@code UUIDv7},
  /// and the remaining bytes will contain random data from a cryptographically strong
  /// pseudo-random number generator.
  ///
  /// @apiNote {@code UUIDv7} values are created by allocating a Unix timestamp in milliseconds
  /// in the most significant 48 bits, allocating the required version (4 bits) and variant
  /// (2-bits)
  /// and filling the remaining 74 bits with random bits. As such, this method rejects {@code
  /// timestamp}
  /// values that do not fit into 48 bits.
  ///
  /// Monotonicity (each subsequent value being greater than the last) is a primary characteristic
  /// of {@code UUIDv7} values. This is due to the {@code timestamp} value being part of the
  /// {@code UUID}.
  /// Callers of this method that wish to generate monotonic {@code UUIDv7} values are expected to
  /// ensure that the given {@code timestamp} value is monotonic.
  ///
  /// @param timestamp the number of milliseconds since midnight 1 Jan 1970 UTC,
  ///     leap seconds excluded.
  /// @return a {@code UUID} constructed using the given {@code timestamp}
  /// @throws IllegalArgumentException if the timestamp is negative or greater than {@code (1L <<
  ///     48) - 1}
  /// @since Backport from Java 26 (JDK-8334015)
  public static UUID ofEpochMillis(long timestamp) {
    if ((timestamp >> 48) != 0) {
      throw new IllegalArgumentException(
          "Supplied timestamp: " + timestamp + " does not fit within 48 bits");
    }

    byte[] randomBytes = new byte[16];
    LazyRandom.RANDOM.nextBytes(randomBytes);

    // Embed the timestamp into the first 6 bytes
    randomBytes[0] = (byte) (timestamp >> 40);
    randomBytes[1] = (byte) (timestamp >> 32);
    randomBytes[2] = (byte) (timestamp >> 24);
    randomBytes[3] = (byte) (timestamp >> 16);
    randomBytes[4] = (byte) (timestamp >> 8);
    randomBytes[5] = (byte) (timestamp);

    // Set version to 7
    randomBytes[6] &= 0x0f;
    randomBytes[6] |= 0x70;

    // Set variant to IETF
    randomBytes[8] &= 0x3f;
    randomBytes[8] |= (byte) 0x80;

    // Convert byte array to UUID using ByteBuffer
    ByteBuffer buffer = ByteBuffer.wrap(randomBytes);
    long msb = buffer.getLong();
    long lsb = buffer.getLong();
    return new UUID(msb, lsb);
  }

  /// Creates a UUID with a unique MSB and time+counter in LSB.
  /// Layout:
  /// ┌──────────────────────────────────────────────────────────────────────────────┐
  /// │  unique  (64 bits)  │  time+counter  (44 bits)  │  random  (20 bits)        │
  /// └──────────────────────────────────────────────────────────────────────────────┘
  ///
  /// @param uniqueMsb the unique most significant 64 bits
  /// @return a {@code UUID} with the given MSB and time-ordered LSB
  public static UUID uniqueThenTime(long uniqueMsb) {
    final int timeBits = 44;
    final int randomBits = 20;
    final int randomMask = (1 << randomBits) - 1;

    long currentMillis = System.currentTimeMillis();
    long counter = sequence.incrementAndGet();

    // Create a 44-bit value combining timestamp and counter
    // Use upper 32 bits for timestamp, lower 12 bits for counter
    // This ensures uniqueness even within the same millisecond
    long timeComponent = ((currentMillis & 0xFFFFFFFFL) << 12) | (counter & 0xFFF);
    // Take only the lower 44 bits
    timeComponent = timeComponent & ((1L << timeBits) - 1);

    long lsb = (timeComponent << randomBits) | (getRandom().nextInt() & randomMask);
    return new UUID(uniqueMsb, lsb);
  }

  /// Generates a time-based UUID using the configured strategy.
  ///
  /// The default strategy is UUIDv7 (standards-compliant RFC 4122), but can be configured
  /// via system property: com.github.simbo1905.nfp.srs.UUIDGenerator.strategy
  ///
  /// For UUIDv7: Timestamp in first 48 bits, version 7, variant IETF, random in remaining bits
  /// For uniqueThenTime: Stable unique MSB (generated at startup), time+counter (44 bits) + random
  /// (20 bits) in LSB
  /// For legacy: Time+counter in MSB, random in LSB (original implementation)
  ///
  /// @return A new UUID with time-based ordering and global uniqueness.
  public static UUID generateUUID() {
    return switch (DEFAULT_STRATEGY) {
      case UUIDV7 -> ofEpochMillis(System.currentTimeMillis());
      case UNIQUE_THEN_TIME -> uniqueThenTime(UNIQUE_MSB);
      case LEGACY -> {
        // Original implementation: time+counter in MSB, random in LSB
        long msb = epochTimeThenCounterMsb();
        long lsb = LazyRandom.RANDOM.nextLong();
        yield new UUID(msb, lsb);
      }
    };
  }
}
