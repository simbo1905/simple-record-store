package com.github.trex_paxos.srs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Detailed investigation test to determine if the crash bug is fundamental 
/// or introduced by the builder pattern refactor.
/// 
/// This test uses controlled halting instead of exception throwing to precisely
/// measure file operations and verify file integrity at specific points.
public class CrashBugInvestigationTest extends JulLoggingConfig {
    
    private static final Logger logger = Logger.getLogger(CrashBugInvestigationTest.class.getName());
    
    private Path tempFile;
    private FileRecordStore store;
    
    @Before
    public void setup() throws IOException {
        tempFile = Files.createTempFile("crash-investigation-", ".db");
        tempFile.toFile().deleteOnExit();
        logger.log(Level.FINE, () -> "Setup: Created temp file " + tempFile);
    }
    
    @After
    public void cleanup() throws IOException {
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                logger.log(Level.FINE, "Error closing store during cleanup", e);
            }
        }
        try {
            Files.deleteIfExists(tempFile);
            logger.log(Level.FINE, () -> "Cleanup: Deleted temp file " + tempFile);
        } catch (IOException e) {
            logger.log(Level.FINE, "Error deleting temp file", e);
        }
    }
    
    /// First discover the total operation count for a simple insert
    @Test
    public void testDiscoverOperationCount() throws Exception {
        logger.log(Level.FINE, "=== Starting testDiscoverOperationCount ===");
        
        // Reset file
        Files.deleteIfExists(tempFile);
        Files.createFile(tempFile);
        tempFile.toFile().deleteOnExit();
        
        // Create store with counting wrapper (halt at a very high number to count all operations)
        int veryHighHaltPoint = 1000;
        store = createStoreWithHalt(veryHighHaltPoint);
        
        // Perform simple insert
        ByteSequence key = ByteSequence.of("testkey".getBytes());
        byte[] data = "testdata".getBytes();
        
        logger.log(Level.FINE, () -> String.format("Inserting key=%s, data=%s", 
            new String(key.bytes), new String(data)));
        
        store.insertRecord(key, data);
        
        // Get the operation count
        int totalOps = ((DelegatingHaltOperations) store.fileOperations).getOperationCount();
        logger.log(Level.FINE, () -> String.format("Total operations for simple insert: %d", totalOps));
        
        // Verify data is intact
        byte[] readData = store.readRecordData(key);
        Assert.assertArrayEquals("Data should be intact", data, readData);
        
        store.close();
        
        // Now test systematic halting at each operation
        testSystematicHalting(totalOps);
        
        logger.log(Level.FINE, "=== testDiscoverOperationCount completed successfully ===");
    }
    
    /// Systematically test halting at each operation count
    private void testSystematicHalting(int maxOperations) throws Exception {
        logger.log(Level.FINE, () -> String.format("=== Starting systematic halting test for %d operations ===", maxOperations));
        
        // Test halt at each operation from 1 to maxOperations
        java.util.stream.IntStream.rangeClosed(1, maxOperations).forEach(haltAt -> {
            try {
                logger.log(Level.FINE, () -> String.format("Testing halt at operation %d/%d", haltAt, maxOperations));
                
                // Reset file for each test
                Files.deleteIfExists(tempFile);
                Files.createFile(tempFile);
                tempFile.toFile().deleteOnExit();
                
                // Create store with halt wrapper
                FileRecordStore testStore = createStoreWithHalt(haltAt);
                
                // Perform simple insert
                ByteSequence key = ByteSequence.of("testkey".getBytes());
                byte[] data = "testdata".getBytes();
                
                testStore.insertRecord(key, data);
                
                // Flush and close
                testStore.fsync();
                testStore.close();
                
                // Try to reopen and verify
                try {
                    FileRecordStore reopenedStore = new FileRecordStore.Builder()
                        .path(tempFile)
                        .accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY)
                        .disablePayloadCrc32(false)
                        .open();
                    
                    // Verify data integrity
                    if (reopenedStore.recordExists(key)) {
                        byte[] readData = reopenedStore.readRecordData(key);
                        Assert.assertArrayEquals("Data should match after halt at operation " + haltAt, data, readData);
                        logger.log(Level.FINE, () -> String.format("✓ Data integrity verified for halt at operation %d", haltAt));
                    } else {
                        logger.log(Level.FINE, () -> String.format("Key not found after halt at operation %d", haltAt));
                    }
                    
                    reopenedStore.close();
                    
                } catch (Exception e) {
                    logger.log(Level.SEVERE, String.format("Failed to reopen store after halt at operation %d", haltAt), e);
                    
                    // For critical early operations, this indicates a real bug
                    if (haltAt <= 3) {
                        throw new RuntimeException("Critical failure at early operation " + haltAt, e);
                    }
                }
                
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error during systematic halting test", e);
                throw new RuntimeException("Systematic halting test failed at operation " + haltAt, e);
            }
        });
        
        logger.log(Level.FINE, "=== Systematic halting test completed successfully ===");
    }
    
    /// Create a FileRecordStore with halt wrapper at specified operation count
    private FileRecordStore createStoreWithHalt(int haltAtOperation) throws IOException {
        logger.log(Level.FINE, () -> String.format("Creating store with halt at operation %d", haltAtOperation));
        
        // Create the base RandomAccessFile
        RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw");
        
        // Create the direct file operations
        DirectRandomAccessFile directOps = new DirectRandomAccessFile(raf);
        
        // Wrap with halt operations
        DelegatingHaltOperations haltOps = new DelegatingHaltOperations(directOps, haltAtOperation);
        
        // Create FileRecordStore with custom operations
        FileRecordStore store = new FileRecordStore(tempFile.toFile(), 10, 64, false, false, "rw");
        
        // Replace the file operations with our halt wrapper
        store.fileOperations = haltOps;
        
        logger.log(Level.FINE, "Store created with halt wrapper");
        return store;
    }
    
    /// Log current file state
    private void logFileState(String context) throws IOException {
        logger.log(Level.FINE, () -> String.format("=== File state: %s ===", context));
        
        if (store != null) {
            logger.log(Level.FINE, () -> String.format("Store has %d records", store.getNumRecords()));
            
            // Log all keys
            for (ByteSequence key : store.keys()) {
                try {
                    byte[] data = store.readRecordData(key);
                    logger.log(Level.FINEST, () -> String.format("  Key: %s, Data: %s (len=%d)", 
                        new String(key.bytes), new String(data), data.length));
                } catch (Exception e) {
                    logger.log(Level.FINE, () -> String.format("  Key: %s - ERROR reading data: %s", 
                        new String(key.bytes), e.getMessage()));
                }
            }
        }
        
        // Log raw file info
        try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "r")) {
            long fileLength = raf.length();
            logger.log(Level.FINE, () -> String.format("Raw file length: %d bytes", fileLength));
            
            if (fileLength > 0) {
                // Read key length header
                raf.seek(0);
                int keyLength = raf.readByte() & 0xFF;
                logger.log(Level.FINE, () -> String.format("Key length header: %d", keyLength));
                
                // Read number of records
                raf.seek(1);
                int numRecords = raf.readInt();
                logger.log(Level.FINE, () -> String.format("Number of records: %d", numRecords));
                
                // Read data start pointer
                raf.seek(5);
                long dataStart = raf.readLong();
                logger.log(Level.FINE, () -> String.format("Data start pointer: %d", dataStart));
            }
        }
    }
    
    /// Dump raw file contents for debugging
    private void dumpFileContents() {
        logger.log(Level.SEVERE, "=== DUMPING FILE CONTENTS ===");
        
        try {
            byte[] fileBytes = Files.readAllBytes(tempFile);
            logger.log(Level.SEVERE, () -> String.format("File size: %d bytes", fileBytes.length));
            
            // Dump first 100 bytes in hex
            StringBuilder hex = new StringBuilder();
            StringBuilder ascii = new StringBuilder();
            for (int i = 0; i < Math.min(100, fileBytes.length); i++) {
                byte b = fileBytes[i];
                hex.append(String.format("%02X ", b));
                ascii.append((b >= 32 && b <= 126) ? (char) b : '.');
            }
            
            logger.log(Level.SEVERE, "First 100 bytes (hex): " + hex.toString());
            logger.log(Level.SEVERE, "First 100 bytes (ascii): " + ascii.toString());
            
            // Try to parse headers
            if (fileBytes.length >= 13) {
                int keyLength = fileBytes[0] & 0xFF;
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(fileBytes, 1, 12);
                int numRecords = buffer.getInt();
                long dataStart = buffer.getLong();
                
                logger.log(Level.SEVERE, () -> String.format("Parsed headers - KeyLength: %d, NumRecords: %d, DataStart: %d", 
                    keyLength, numRecords, dataStart));
            }
            
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to dump file contents", e);
        }
        
        logger.log(Level.SEVERE, "=== END FILE DUMP ===");
    }
}