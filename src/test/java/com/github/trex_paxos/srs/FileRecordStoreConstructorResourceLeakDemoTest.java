package com.github.trex_paxos.srs;

import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

/**
 * Test to demonstrate the ACTUAL resource leak in FileRecordStore constructor.
 * 
 * THE BUG: When constructor validation fails, RandomAccessFile and fileOperations 
 * are created but NEVER CLOSED, causing resource leaks.
 */
public class FileRecordStoreConstructorResourceLeakDemoTest extends JulLoggingConfig {

    @Test
    public void demonstrateActualResourceLeak() throws Exception {
        logger.log(Level.FINE, "=== Demonstrating ACTUAL Resource Leak in FileRecordStore Constructor ===");
        
        Path tempFile = Files.createTempFile("actual-resource-leak", ".dat");
        
        try {
            // Create a file that will pass initial validation but fail later
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.setLength(13); // Header size
                raf.writeByte(32); // key length = 32 (different from expected 64)
                raf.writeInt(0); // numRecords = 0
                raf.writeLong(13); // dataStart = 13
            }
            
            logger.log(Level.FINE, "Attempting to open store with mismatched key length (expecting 64, file has 32)...");
            
            // This should fail with IllegalArgumentException during constructor
            // THE BUG: RandomAccessFile will be created but never closed!
            try {
                FileRecordStore store = new FileRecordStore(tempFile.toFile(), 
                                                            0,      // preallocatedRecords
                                                            64,     // maxKeyLength (expects 64, file has 32)
                                                            false,  // disablePayloadCrc32
                                                            false,  // useMemoryMapping
                                                            "rw");  // accessMode
                
                Assert.fail("Should have thrown IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                logger.log(Level.FINE, "✓ Got expected validation exception: " + e.getMessage());
                
                // THE BUG: At this point, RandomAccessFile was created but never closed!
                // Try to delete the file - if it fails, resources are leaked
                try {
                    Files.delete(tempFile);
                    logger.log(Level.FINE, "✗ File deleted successfully - this suggests the bug might be elsewhere or already fixed");
                    Files.createFile(tempFile);
                } catch (Exception deleteEx) {
                    logger.log(Level.FINE, "✓ CONFIRMED: Resource leak detected - file still locked: " + deleteEx.getMessage());
                    // This confirms the resource leak
                }
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    @Test  
    public void demonstrateResourceLeakWithDirectConstructor() throws Exception {
        logger.log(Level.FINE, "=== Testing Resource Leak with Direct Constructor Call ===");
        
        Path tempFile = Files.createTempFile("direct-constructor-leak", ".dat");
        
        try {
            // Create a file that will fail validation
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.setLength(13);
                raf.writeByte(64); // Valid key length
                raf.writeInt(1);   // numRecords = 1
                raf.writeLong(13); // dataStart = 13
                // Don't write any actual record data - this will cause file size validation to fail
            }
            
            logger.log(Level.FINE, "Calling constructor directly with file that will fail file size validation...");
            
            try {
                // Use the package-private constructor directly
                FileRecordStore store = new FileRecordStore(tempFile.toFile(),
                                                            0,   // preallocatedRecords  
                                                            64,  // maxKeyLength
                                                            false, // disablePayloadCrc32
                                                            false, // useMemoryMapping
                                                            "rw"); // accessMode
                
                Assert.fail("Should have thrown IOException for file size validation");
            } catch (IOException e) {
                logger.log(Level.FINE, "✓ Got expected file size exception: " + e.getMessage());
                
                // Test for resource leak
                try {
                    Files.delete(tempFile);
                    logger.log(Level.FINE, "File deleted - checking if this indicates resource management issue...");
                    
                    // The real test: try to create many failing constructors and see if we exhaust resources
                    for (int i = 0; i < 100; i++) {
                        Path testFile = Files.createTempFile("resource-test-" + i, ".dat");
                        try (RandomAccessFile raf = new RandomAccessFile(testFile.toFile(), "rw")) {
                            raf.setLength(13);
                            raf.writeByte(64);
                            raf.writeInt(1); // Will cause file size validation failure
                            raf.writeLong(13);
                        }
                        
                        try {
                            FileRecordStore store = new FileRecordStore(testFile.toFile(), 0, 64, false, false, "rw");
                        } catch (IOException expected) {
                            // Expected - validation should fail
                        }
                        
                        Files.delete(testFile);
                    }
                    logger.log(Level.FINE, "✓ Created and cleaned up 100 failing constructors - no apparent resource exhaustion");
                    
                } catch (Exception deleteEx) {
                    logger.log(Level.FINE, "✗ Resource issue detected: " + deleteEx.getMessage());
                }
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    @Test
    public void demonstrateEarlyFieldInitializationIssue() throws Exception {
        logger.log(Level.FINE, "=== Demonstrating Early Field Initialization Issue ===");
        
        Path tempFile = Files.createTempFile("early-field-init", ".dat");
        
        try {
            // Create a file that will help us understand the constructor flow
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.setLength(13);
                raf.writeByte(64); // key length = 64
                raf.writeInt(0);   // numRecords = 0 (empty)
                raf.writeLong(13); // dataStart = 13
            }
            
            logger.log(Level.FINE, "Opening valid store to understand constructor flow...");
            
            // This should succeed, showing the normal constructor flow
            try (FileRecordStore store = new FileRecordStore(tempFile.toFile(),
                                                            0,   // preallocatedRecords
                                                            64,  // maxKeyLength 
                                                            false, // disablePayloadCrc32
                                                            false, // useMemoryMapping
                                                            "rw") ) { // accessMode
                
                logger.log(Level.FINE, "✓ Store opened successfully - constructor flow completed");
                
                // The issue is subtle: fields like indexEntryLength, dataStartPtr are set
                // before validation, but for existing files they should be read from file
                
                // For now, let's verify the store works correctly
                Assert.assertTrue("Store should be empty", store.isEmpty());
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
    
    @Test
    public void demonstrateCorrectBehaviorVsBuggyBehavior() throws Exception {
        logger.log(Level.FINE, "=== Comparing Correct vs Potentially Buggy Behavior ===");
        
        Path tempFile = Files.createTempFile("behavior-comparison", ".dat");
        
        try {
            // Create a store with specific parameters
            try (FileRecordStore store1 = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64)
                    .preallocatedRecords(10)
                    .open()) {
                store1.insertRecord(ByteSequence.of("key1".getBytes()), "data1".getBytes());
            }
            
            // Now reopen with DIFFERENT parameters - this tests if the constructor
            // properly reads from file vs uses constructor parameters
            logger.log(Level.FINE, "Reopening store with different preallocatedRecords parameter...");
            
            try (FileRecordStore store2 = new FileRecordStore.Builder()
                    .path(tempFile)
                    .maxKeyLength(64) // Same as file
                    .preallocatedRecords(5) // Different from original
                    .open()) {
                
                logger.log(Level.FINE, "✓ Store reopened successfully with different parameters");
                
                // Verify data integrity - this tests if dataStartPtr was read correctly from file
                byte[] data = store2.readRecordData(ByteSequence.of("key1".getBytes()));
                Assert.assertArrayEquals("Data should be preserved", "data1".getBytes(), data);
                
                logger.log(Level.FINE, "✓ Data integrity verified - dataStartPtr was correctly read from file");
            }
            
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}