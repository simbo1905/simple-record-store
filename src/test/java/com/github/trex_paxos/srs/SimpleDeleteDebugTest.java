package com.github.trex_paxos.srs;

import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;

/// Simple debug test for delete operations with FINER logging
public class SimpleDeleteDebugTest extends JulLoggingConfig {
    
    @Test
    public void testSimpleDeleteDebug() throws Exception {
        // Set FINER logging for detailed operation tracking
        java.util.logging.Logger.getLogger("com.github.trex_paxos.srs").setLevel(Level.FINER);
        
        logger.log(Level.FINE, "=== Simple Delete Debug Test ===");
        
        // First, discover the actual operation count for a simple delete
        int totalOps = discoverDeleteOperationCount();
        logger.log(Level.FINE, "Discovered " + totalOps + " operations for simple delete");
        
        // Now test exceptions at each operation
        for (int throwAt = 1; throwAt <= totalOps; throwAt++) {
            logger.log(Level.FINE, "Testing exception at operation " + throwAt + "/" + totalOps);
            testDeleteWithException(throwAt);
        }
        
        logger.log(Level.FINE, "=== Completed simple delete debug ===");
    }
    
    private int discoverDeleteOperationCount() throws Exception {
        FileRecordStore.Builder builder = new FileRecordStore.Builder()
            .tempFile("delete-discovery-", ".db");
        
        FileRecordStore baseStore = builder.open();
        
        // Replace with counting delegate
        RandomAccessFile raf = new RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
        DirectRandomAccessFile directOps = new DirectRandomAccessFile(raf);
        DelegatingExceptionOperations countingOps = new DelegatingExceptionOperations(directOps, Integer.MAX_VALUE);
        
        FileRecordStore countingStore = builder.path(baseStore.getFilePath()).open();
        countingStore.fileOperations = countingOps;
        
        baseStore.close();
        
        // Setup: insert a record first
        ByteSequence key = ByteSequence.of("testkey".getBytes());
        byte[] data = "testdata".getBytes();
        countingStore.insertRecord(key, data);
        
        // Reset counter before delete
        countingOps.getOperationCount(); // This resets the count
        
        // Perform delete
        countingStore.deleteRecord(key);
        
        int totalOps = countingOps.getOperationCount();
        
        countingStore.close();
        return totalOps;
    }
    
    private void testDeleteWithException(int throwAt) throws Exception {
        FileRecordStore store = null;
        DelegatingExceptionOperations exceptionOps = null;
        
        try {
            // Create store with data
            store = createStoreWithData();
            
            // Replace with exception delegate
            RandomAccessFile raf = new RandomAccessFile(store.getFilePath().toFile(), "rw");
            DirectRandomAccessFile directOps = new DirectRandomAccessFile(raf);
            exceptionOps = new DelegatingExceptionOperations(directOps, throwAt);
            store.fileOperations = exceptionOps;
            
            ByteSequence key = ByteSequence.of("testkey".getBytes());
            
            try {
                store.deleteRecord(key);
                logger.log(Level.FINE, "  Delete completed - no exception at operation " + throwAt);
            } catch (IOException e) {
                logger.log(Level.FINE, "  Got expected exception at operation " + throwAt + ": " + e.getMessage());
                logger.log(Level.FINE, "  Store state: " + store.getState());
                
                // Verify subsequent operations fail
                try {
                    store.recordExists(key);
                    logger.log(Level.FINE, "  ERROR: recordExists should have failed!");
                } catch (IllegalStateException ise) {
                    logger.log(Level.FINE, "  ✓ Subsequent operation correctly failed: " + ise.getMessage());
                }
            }
            
        } finally {
            if (store != null) {
                try {
                    store.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }
    
    private FileRecordStore createStoreWithData() throws IOException {
        FileRecordStore store = new FileRecordStore.Builder()
            .tempFile("delete-test-", ".db")
            .open();
        
        ByteSequence key = ByteSequence.of("testkey".getBytes());
        byte[] data = "testdata".getBytes();
        store.insertRecord(key, data);
        
        return store;
    }
}