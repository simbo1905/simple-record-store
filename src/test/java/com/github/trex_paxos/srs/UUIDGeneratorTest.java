// SPDX-FileCopyrightText: 2024 - 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package com.github.trex_paxos.srs;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class UUIDGeneratorTest extends JulLoggingConfig {

    private static final Logger LOGGER = Logger.getLogger(UUIDGeneratorTest.class.getName());

    @Test
    public void testGenerateUUIDsAreUnique() {
        LOGGER.info("testGenerateUUIDsAreUnique starting");
        final var uuids = new HashSet<UUID>();
        IntStream.range(0, 10000).forEach(i -> {
            final var uuid = UUIDGenerator.generateUUID();
            Assert.assertNotNull(uuid);
            uuids.add(uuid);
        });
        // All UUIDs should be unique
        Assert.assertEquals(10000, uuids.size());
    }

    @Test
    public void testGenerateUUIDsAreTimeOrdered() {
        LOGGER.info("testGenerateUUIDsAreTimeOrdered starting");
        // Generate UUIDs and verify they are generally increasing (time-ordered)
        final var previous = new AtomicReference<>(UUIDGenerator.generateUUID());
        final var ascendingCount = new AtomicInteger(0);
        
        IntStream.range(0, 1000).forEach(i -> {
            final var current = UUIDGenerator.generateUUID();
            // Compare the most significant bits which contain the timestamp
            if (current.getMostSignificantBits() >= previous.get().getMostSignificantBits()) {
                ascendingCount.incrementAndGet();
            }
            previous.set(current);
        });
        
        // Most UUIDs should be in ascending order (allowing for some edge cases)
        // We expect at least 99% to be ascending
        Assert.assertTrue("UUIDs should be mostly time-ordered", 
            ascendingCount.get() > 1000 * 0.99);
    }

    @Test
    public void testGenerateUUIDsAtHighRate() {
        LOGGER.info("testGenerateUUIDsAtHighRate starting");
        // Test that we can generate many UUIDs quickly without collisions
        final var uuids = new HashSet<UUID>();
        
        final var startTime = System.currentTimeMillis();
        IntStream.range(0, 100000).forEach(i -> uuids.add(UUIDGenerator.generateUUID()));
        final var endTime = System.currentTimeMillis();
        
        // All UUIDs should be unique
        Assert.assertEquals(100000, uuids.size());
        
        // Log performance for information
        final var rate = (100000 * 1000.0) / (endTime - startTime);
        LOGGER.fine("Generated " + 100000 + " UUIDs in " + 
            (endTime - startTime) + "ms (rate: " + String.format("%.0f", rate) + " UUIDs/sec)");
    }
}
