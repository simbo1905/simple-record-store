// SPDX-FileCopyrightText: 2024 - 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package com.github.trex_paxos.srs;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class UUIDGeneratorTest {

    @Test
    public void testGenerateUUIDsAreUnique() {
        Set<UUID> uuids = new HashSet<>();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            UUID uuid = UUIDGenerator.generateUUID();
            Assert.assertNotNull(uuid);
            uuids.add(uuid);
        }
        // All UUIDs should be unique
        Assert.assertEquals(count, uuids.size());
    }

    @Test
    public void testGenerateUUIDsAreTimeOrdered() {
        // Generate UUIDs and verify they are generally increasing (time-ordered)
        UUID previous = UUIDGenerator.generateUUID();
        int ascendingCount = 0;
        int testCount = 1000;
        
        for (int i = 0; i < testCount; i++) {
            UUID current = UUIDGenerator.generateUUID();
            // Compare the most significant bits which contain the timestamp
            if (current.getMostSignificantBits() >= previous.getMostSignificantBits()) {
                ascendingCount++;
            }
            previous = current;
        }
        
        // Most UUIDs should be in ascending order (allowing for some edge cases)
        // We expect at least 99% to be ascending
        Assert.assertTrue("UUIDs should be mostly time-ordered", 
            ascendingCount > testCount * 0.99);
    }

    @Test
    public void testGenerateUUIDsAtHighRate() {
        // Test that we can generate many UUIDs quickly without collisions
        Set<UUID> uuids = new HashSet<>();
        int count = 100000;
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            uuids.add(UUIDGenerator.generateUUID());
        }
        long endTime = System.currentTimeMillis();
        
        // All UUIDs should be unique
        Assert.assertEquals(count, uuids.size());
        
        // Log performance for information
        double rate = (count * 1000.0) / (endTime - startTime);
        System.out.println("Generated " + count + " UUIDs in " + 
            (endTime - startTime) + "ms (rate: " + String.format("%.0f", rate) + " UUIDs/sec)");
    }
}
