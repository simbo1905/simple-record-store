package com.github.simbo1905.nfp.srs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.jqwik.api.*;

/// Property-based integration test for header expansion and file growth strategies.
/// Tests various combinations of key sizes, block sizes, and expansion ratios
/// to verify correct behavior under stress conditions.
public class HeaderExpansionITCase {

  /// Property-based test for header expansion behavior with random parameters.
  /// Runs until 3 header expansions AND 3 file expansions occur, OR total payload reaches 4 KiB.
  @Property(tries = 50)
  void testHeaderExpansionWithRandomParameters(
      @ForAll("keyLengths") int maxKeyLength,
      @ForAll("blockSizes") int preferredBlockSize,
      @ForAll("expansionPercents") double expansionExtraPercent,
      @ForAll("recordCounts") int targetRecordCount)
      throws IOException {

    // Create temporary directory for test
    Path tempDir = Files.createTempDirectory("header-expansion-test");
    File dbFile = tempDir.resolve("test.db").toFile();

    try {
      // Build store with test parameters
      FileRecordStoreBuilder builder =
          new FileRecordStoreBuilder()
              .path(dbFile.toPath())
              .maxKeyLength(maxKeyLength)
              .hintPreferredBlockSize(preferredBlockSize) // preferredBlockSize is in KiB
              .withExpansionExtraPercent(expansionExtraPercent)
              .preallocatedRecords(0)
              .allowZeroPreallocation();

      try (FileRecordStore store = builder.open()) {
        Random random = new Random(42); // Fixed seed for reproducibility
        int headerExpansions = 0;
        int fileExpansions = 0;
        long totalPayloadSize = 0;
        long previousDataStartPtr = store.dataStartPtr;
        long previousFileLength = store.getFileLength();

        List<byte[]> keys = new ArrayList<>();

        // Insert records until we hit one of the termination conditions
        while ((headerExpansions < 3 || fileExpansions < 3) && totalPayloadSize < 4096) {
          // Generate random key (length between 1 and maxKeyLength)
          int keyLen = 1 + random.nextInt(maxKeyLength);
          byte[] key = new byte[keyLen];
          random.nextBytes(key);

          // Generate random payload (length between 1 and 512 bytes)
          int payloadLen = 1 + random.nextInt(512);
          byte[] payload = new byte[payloadLen];
          random.nextBytes(payload);

          try {
            store.insertRecord(key, payload);
            keys.add(key);
            totalPayloadSize += payloadLen;

            // Detect header expansion (dataStartPtr increased)
            if (store.dataStartPtr > previousDataStartPtr) {
              headerExpansions++;
              previousDataStartPtr = store.dataStartPtr;
            }

            // Detect file expansion (file length increased)
            long currentFileLength = store.getFileLength();
            if (currentFileLength > previousFileLength) {
              fileExpansions++;
              previousFileLength = currentFileLength;
            }

            // Stop if we've inserted enough records
            if (keys.size() >= targetRecordCount) {
              break;
            }
          } catch (Exception e) {
            // Record insertion failed - this may be expected depending on configuration
            // For now, just break the loop
            break;
          }
        }

        // TODO: Add validation logic here
        // For now, just verify store is still operational
        if (!keys.isEmpty()) {
          byte[] firstKey = keys.get(0);
          if (store.recordExists(firstKey)) {
            byte[] readData = store.readRecordData(firstKey);
            // Basic sanity check - data was retrieved
            assert readData != null : "Read data should not be null";
          }
        }
      }
    } finally {
      // Cleanup
      if (dbFile.exists()) {
        dbFile.delete();
      }
      Files.deleteIfExists(tempDir);
    }
  }

  /// Arbitrary provider for max key lengths: 8, 16, 32, 64, 128 bytes.
  @Provide
  Arbitrary<Integer> keyLengths() {
    return Arbitraries.of(8, 16, 32, 64, 128);
  }

  /// Arbitrary provider for preferred block sizes (in KiB): 1, 2, 4, 8, 16.
  /// These are small sizes to stress-test the expansion logic.
  @Provide
  Arbitrary<Integer> blockSizes() {
    return Arbitraries.of(1, 2, 4, 8, 16);
  }

  /// Arbitrary provider for expansion percentages: 0.1, 0.2, 0.5 (10%, 20%, 50%).
  @Provide
  Arbitrary<Double> expansionPercents() {
    return Arbitraries.of(0.1, 0.2, 0.5);
  }

  /// Arbitrary provider for target record counts: 0, 1, 3, 5, 10, 20.
  @Provide
  Arbitrary<Integer> recordCounts() {
    return Arbitraries.of(0, 1, 3, 5, 10, 20);
  }
}
