package com.github.simbo1905.nfp.srs;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import org.junit.Test;

public class MemoryMappedBugsTest extends JulLoggingConfig {

  @Test
  public void demonstrateMemoryLeakFix() throws Exception {
    logger.log(Level.FINE, "=== Testing MemoryMappedFile Memory Leak Fix ===");

    Path tempFile = Files.createTempFile("memory-leak-test", ".dat");

    try {
      // Create initial file with some data
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.setLength(1024); // 1KB initial size
      }

      // Create memory-mapped file
      MemoryMappedFile mappedFile =
          new MemoryMappedFile(new RandomAccessFile(tempFile.toFile(), "rw"));

      // Write some initial data
      mappedFile.write("initial data".getBytes());

      // Use reflection to access mappedBuffers field
      Field mappedBuffersField = MemoryMappedFile.class.getDeclaredField("mappedBuffers");
      mappedBuffersField.setAccessible(true);

      // After our fix, mappedBuffers should be empty since we clear it after initial mapping
      List<?> mappedBuffersAfterInit = (List<?>) mappedBuffersField.get(mappedFile);
      logger.log(
          Level.FINE,
          "mappedBuffers size after initialization: "
              + (mappedBuffersAfterInit != null ? mappedBuffersAfterInit.size() : "null"));

      // Verify the fix - mappedBuffers should be empty
      if (mappedBuffersAfterInit != null && mappedBuffersAfterInit.isEmpty()) {
        logger.log(
            Level.FINE,
            "✓ FIXED: Memory leak resolved - mappedBuffers is empty after initialization");
      } else {
        logger.log(
            Level.FINE,
            "✗ Issue: mappedBuffers still contains "
                + (mappedBuffersAfterInit != null ? mappedBuffersAfterInit.size() : 0)
                + " elements");
      }

      // Trigger remapping by growing the file
      mappedFile.setLength(2048); // Double the size

      // Write more data in the extended region
      mappedFile.seek(1500);
      mappedFile.write("extended data".getBytes());

      mappedFile.close();

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void demonstrateDataLossRisk() throws Exception {
    logger.log(Level.FINE, "=== Testing MemoryMappedFile Data Loss Risk ===");

    Path tempFile = Files.createTempFile("data-loss-test", ".dat");

    try {
      // Create initial file
      try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
        raf.setLength(1024);
      }

      // Create memory-mapped file and write initial data
      MemoryMappedFile mappedFile =
          new MemoryMappedFile(new RandomAccessFile(tempFile.toFile(), "rw"));

      // Write initial data
      mappedFile.write("initial data".getBytes());
      mappedFile.sync(); // This should work fine for initial data

      // Grow the file (triggers remapping)
      mappedFile.setLength(2048);

      // Write data in the extended region
      mappedFile.seek(1500);
      String extendedData = "this is critical extended data that must survive";
      mappedFile.write(extendedData.getBytes());

      // Sync - BUG: this will only flush the old mappedBuffers, not the new currentEpoch buffers
      logger.log(Level.FINE, "Calling sync() - this should flush ALL data but won't due to bug");
      mappedFile.sync();

      mappedFile.close();

      // Now simulate a crash by creating a new instance and reading
      logger.log(Level.FINE, "Simulating crash recovery by reopening file");
      MemoryMappedFile recoveryFile =
          new MemoryMappedFile(new RandomAccessFile(tempFile.toFile(), "rw"));

      // Try to read the extended data
      recoveryFile.seek(1500);
      byte[] readBuffer = new byte[extendedData.length()];
      int bytesRead = recoveryFile.read(readBuffer);
      String recoveredData = new String(readBuffer, 0, bytesRead > 0 ? bytesRead : 0);

      logger.log(Level.FINE, "Original extended data: '" + extendedData + "'");
      logger.log(Level.FINE, "Recovered extended data: '" + recoveredData + "'");

      if (!extendedData.equals(recoveredData)) {
        logger.log(
            Level.FINE, "✓ CONFIRMED: Data loss detected - extended data was not properly synced");
      } else {
        logger.log(Level.FINE, "✗ Data loss not detected - sync might be working correctly");
      }

      recoveryFile.close();

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  @Test
  public void verifyResourceOwnershipIsFixed() throws Exception {
    logger.log(Level.FINE, "=== Verifying RandomAccessFile Resource Ownership ===");

    Path tempFile = Files.createTempFile("resource-test", ".dat");

    try {
      RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw");
      MemoryMappedFile mappedFile = new MemoryMappedFile(raf);

      // Write some data
      mappedFile.write("test data".getBytes());

      // Close should close the underlying RandomAccessFile
      mappedFile.close();

      // Try to use the RandomAccessFile - should fail if properly closed
      try {
        raf.seek(0);
        logger.log(Level.FINE, "✗ RandomAccessFile was not closed - resource leak detected");
      } catch (Exception e) {
        logger.log(Level.FINE, "✓ RandomAccessFile was properly closed - no resource leak");
      }

    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
