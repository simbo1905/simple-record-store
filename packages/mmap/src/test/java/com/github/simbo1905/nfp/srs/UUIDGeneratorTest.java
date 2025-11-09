// SPDX-FileCopyrightText: 2024 - 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package com.github.simbo1905.nfp.srs;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class UUIDGeneratorTest extends JulLoggingConfig {

  private static final Logger LOGGER = Logger.getLogger(UUIDGeneratorTest.class.getName());

  @Test
  public void testGenerateUUIDsAreUnique() {
    LOGGER.fine("testGenerateUUIDsAreUnique starting");
    final var uuids = new HashSet<UUID>();
    IntStream.range(0, 10000)
        .forEach(
            i -> {
              final var uuid = UUIDGenerator.generateUUID();
              Assert.assertNotNull(uuid);
              uuids.add(uuid);
            });
    // All UUIDs should be unique
    Assert.assertEquals(10000, uuids.size());
  }

  @Test
  public void testGenerateUUIDsAreTimeOrdered() {
    LOGGER.fine("testGenerateUUIDsAreTimeOrdered starting");

    final var strategy = UUIDGenerator.getDefaultStrategy();

    if (strategy == UUIDGenerator.Strategy.UUIDV7) {
      // For UUIDv7, test millisecond-level ordering with deliberate delays
      testUUIDv7TimeOrdering();
    } else {
      // For Legacy and uniqueThenTime, test sub-millisecond ordering
      testSubMillisecondOrdering();
    }
  }

  private void testUUIDv7TimeOrdering() {
    // UUIDv7 has millisecond-granularity ordering
    // Test by generating UUIDs with deliberate time gaps
    UUID prev = UUIDGenerator.generateUUID();
    int ascending = 0;

    for (int i = 0; i < 10; i++) {
      try {
        Thread.sleep(2); // 2ms delay to ensure different timestamps
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      UUID current = UUIDGenerator.generateUUID();
      if (current.compareTo(prev) > 0) {
        ascending++;
      }
      prev = current;
    }

    Assert.assertTrue(
        "UUIDv7 should be ordered across different milliseconds (ascending: " + ascending + ")",
        ascending >= 8); // Allow for occasional timing issues
  }

  private void testSubMillisecondOrdering() {
    // For Legacy and uniqueThenTime strategies, test rapid generation
    final var previous = new AtomicReference<>(UUIDGenerator.generateUUID());
    final var ascendingCount = new AtomicInteger(0);

    IntStream.range(0, 1000)
        .forEach(
            i -> {
              final var current = UUIDGenerator.generateUUID();
              if (current.compareTo(previous.get()) >= 0) {
                ascendingCount.incrementAndGet();
              }
              previous.set(current);
            });

    // For Legacy/uniqueThenTime, we have deterministic sub-ms ordering via counter
    Assert.assertTrue(
        "UUIDs should be strictly ordered (ascending: " + ascendingCount.get() + ")",
        ascendingCount.get() > 990); // 99%+ should be ascending
  }

  @Test
  public void testGenerateUUIDsAtHighRate() {
    LOGGER.fine("testGenerateUUIDsAtHighRate starting");
    // Test that we can generate many UUIDs quickly without collisions
    final var uuids = new HashSet<UUID>();

    final var startTime = System.currentTimeMillis();
    IntStream.range(0, 100000).forEach(i -> uuids.add(UUIDGenerator.generateUUID()));
    final var endTime = System.currentTimeMillis();

    // All UUIDs should be unique
    Assert.assertEquals(100000, uuids.size());

    // Log performance for information
    final var rate = (100000 * 1000.0) / (endTime - startTime);
    LOGGER.fine(
        "Generated "
            + 100000
            + " UUIDs in "
            + (endTime - startTime)
            + "ms (rate: "
            + String.format("%.0f", rate)
            + " UUIDs/sec)");
  }

  @Test
  public void testOfEpochMillis() {
    LOGGER.fine("testOfEpochMillis starting");
    long timestamp = System.currentTimeMillis();

    UUID uuid = UUIDGenerator.ofEpochMillis(timestamp);
    Assert.assertNotNull(uuid);

    // Verify version is 7 (bits 12-15 of time_hi_and_version should be 0111)
    int version = uuid.version();
    Assert.assertEquals("UUID version should be 7", 7, version);

    // Verify variant is IETF (bits 6-7 of clock_seq_hi should be 10)
    int variant = uuid.variant();
    Assert.assertEquals("UUID variant should be 2 (IETF)", 2, variant);
  }

  @Test
  public void testOfEpochMillisMonotonicity() {
    LOGGER.fine("testOfEpochMillisMonotonicity starting");
    // Test that increasing timestamps produce increasing UUIDs
    long timestamp1 = System.currentTimeMillis();
    long timestamp2 = timestamp1 + 100; // 100ms later

    UUID uuid1 = UUIDGenerator.ofEpochMillis(timestamp1);
    UUID uuid2 = UUIDGenerator.ofEpochMillis(timestamp2);

    Assert.assertTrue("UUID with later timestamp should be greater", uuid2.compareTo(uuid1) > 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfEpochMillisInvalidTimestamp() {
    LOGGER.fine("testOfEpochMillisInvalidTimestamp starting");
    // Timestamp that doesn't fit in 48 bits
    long invalidTimestamp = (1L << 48);
    UUIDGenerator.ofEpochMillis(invalidTimestamp);
  }

  @Test
  public void testUniqueThenTime() {
    LOGGER.fine("testUniqueThenTime starting");
    long uniqueMsb = 12345678L;

    UUID uuid = UUIDGenerator.uniqueThenTime(uniqueMsb);
    Assert.assertNotNull(uuid);
    Assert.assertEquals("MSB should match", uniqueMsb, uuid.getMostSignificantBits());
  }

  @Test
  public void testUniqueThenTimeUniqueness() {
    LOGGER.fine("testUniqueThenTimeUniqueness starting");
    long uniqueMsb = 98765432L;

    // Generate multiple UUIDs with the same MSB
    final var uuids = new HashSet<UUID>();
    IntStream.range(0, 10000).forEach(i -> uuids.add(UUIDGenerator.uniqueThenTime(uniqueMsb)));

    // All UUIDs should be unique despite having the same MSB
    Assert.assertEquals(10000, uuids.size());
  }

  @Test
  public void testStrategyConfiguration() {
    LOGGER.fine("testStrategyConfiguration starting");
    // This test verifies that the strategy can be read
    UUIDGenerator.Strategy strategy = UUIDGenerator.getDefaultStrategy();
    Assert.assertNotNull("Strategy should not be null", strategy);
    LOGGER.fine("Current strategy: " + strategy);
  }
}
