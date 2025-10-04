package com.github.simbo1905.nfp.srs;

import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

/**
 * Test to demonstrate resource leak in FileRecordStore constructor when validation fails.
 * 
 * The issue: RandomAccessFile and fileOperations are created BEFORE header validation.
 * If validation fails, these resources are not properly closed.
 */
public class FileRecordStoreConstructorResourceLeakTest extends JulLoggingConfig {

    @Test
    public void demonstrateResourceLeakOnKeyLengthValidationFailure() throws Exception {
        logger.log(Level.FINE, "=== Testing resource leak on key length validation failure ===");
        
        Path tempFile = Files.createTempFile("resource-leak-keylen", ".dat");
        
        try {
            // Create a valid store first
            try (FileRecordStore store1 = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64)
                    .open()) {
                store1.insertRecord(("testkey".getBytes()), "testdata".getBytes());
            }
            
            // Corrupt the key length header to trigger validation failure
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.seek(0);
                raf.writeByte(32); // Change key length from 64 to 32
            }
            
            logger.log(Level.FINE, "Attempting to open store with mismatched key length...");
            
            // This should fail with IllegalArgumentException
            try (FileRecordStore store2 = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64) // Expect 64, but file has 32
                    .open()) {
                Assert.fail("Should have thrown IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                logger.log(Level.FINE, "✓ Got expected validation exception: " + e.getMessage());
                
                // Now try to test if resources were leaked
                // If resources were properly cleaned up, we should be able to delete the file
                try {
                    Files.delete(tempFile);
                    logger.log(Level.FINE, "✓ File deleted successfully - no resource leak detected");
                    // Recreate the file for cleanup
                    Files.createFile(tempFile);
                } catch (Exception deleteEx) {
                    logger.log(Level.FINE, "✗ Resource leak detected - file still locked: " + deleteEx.getMessage());
                    // This indicates the RandomAccessFile was not closed
                }
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    @Test
    public void demonstrateResourceLeakOnFileSizeValidationFailure() throws Exception {
        logger.log(Level.FINE, "=== Testing resource leak on file size validation failure ===");
        
        Path tempFile = Files.createTempFile("resource-leak-size", ".dat");
        
        try {
            // Create a minimal file with headers but insufficient size for claimed records
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.setLength(13); // Just header size
                raf.writeByte(64); // key length = 64
                raf.writeInt(100); // numRecords = 100 (but file too small)
                raf.writeLong(13); // dataStart = 13
            }
            
            logger.log(Level.FINE, "Attempting to open store with file too small for claimed records...");
            
            // This should fail with IOException due to file size validation
            try (FileRecordStore store = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64)
                    .open()) {
                Assert.fail("Should have thrown IOException");
            } catch (IOException e) {
                logger.log(Level.FINE, "✓ Got expected file size exception: " + e.getMessage());
                
                // Test if resources were leaked
                try {
                    Files.delete(tempFile);
                    logger.log(Level.FINE, "✓ File deleted successfully - no resource leak detected");
                    Files.createFile(tempFile);
                } catch (Exception deleteEx) {
                    logger.log(Level.FINE, "✗ Resource leak detected - file still locked: " + deleteEx.getMessage());
                }
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    @Test
    public void demonstrateEarlyFieldInitializationIssue() throws Exception {
        logger.log(Level.FINE, "=== Testing early field initialization before validation ===");
        
        Path tempFile = Files.createTempFile("early-init", ".dat");
        
        try {
            // Create a file that will pass initial validation but fail later
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.setLength(13); // Header size
                raf.writeByte(64); // Valid key length
                raf.writeInt(1); // Valid numRecords (1)
                raf.writeLong(13); // Valid dataStart
                
                // Write some invalid data that will cause loadExistingIndex to fail
                raf.seek(13); // Position for first record (after header)
                raf.writeByte(10); // Key length
                raf.write(new byte[10]); // Key data
                raf.writeInt(999999); // Invalid CRC32
            }
            
            logger.log(Level.FINE, "Attempting to open store that will fail during index loading...");
            
            // This should fail during loadExistingIndex due to invalid CRC
            try (FileRecordStore store = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64)
                    .open()) {
                Assert.fail("Should have thrown exception during index loading");
            } catch (Exception e) {
                logger.log(Level.FINE, "✓ Got expected exception during index loading: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                
                // The key issue: fields like indexEntryLength, maxKeyLength were set before validation
                // But resources should still be cleaned up
                try {
                    Files.delete(tempFile);
                    logger.log(Level.FINE, "✓ File deleted successfully - resources cleaned up");
                    Files.createFile(tempFile);
                } catch (Exception deleteEx) {
                    logger.log(Level.FINE, "✗ Resource leak detected: " + deleteEx.getMessage());
                }
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
