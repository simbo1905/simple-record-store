package com.github.trex_paxos.srs;

import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Debug test to isolate the exception handling issue
public class ExceptionHandlingDebugTest extends JulLoggingConfig {
    
    private static final Logger logger = Logger.getLogger(ExceptionHandlingDebugTest.class.getName());
    
    @Test
    public void testExceptionHandlingDebug() throws Exception {
        logger.log(Level.FINE, "=== Exception Handling Debug Test ===");
        
        // Test operation 1 (should trigger during construction)
        testOperationWithException(1);
        
        // Test operation 5 (should trigger during normal operation)
        testOperationWithException(5);
        
        // Test operation 10 (should trigger during normal operation)
        testOperationWithException(10);
    }
    
    private void testOperationWithException(int throwAt) throws Exception {
        logger.log(Level.FINE, "Testing exception at operation " + throwAt);
        
        FileRecordStore store = null;
        DelegatingExceptionOperations exceptionOps;
        
        try {
            // Create store with exception injection
            store = createStoreWithException(throwAt);
            exceptionOps = (DelegatingExceptionOperations) store.fileOperations;
            
            logger.log(Level.FINE, "Store created successfully, state: " + store.getState());
            logger.log(Level.FINE, "Exception delegate created, target: " + exceptionOps.getTargetOperation() + ", current count: " + exceptionOps.getOperationCount());
            
            ByteSequence key = ByteSequence.of("testkey".getBytes());
            byte[] data = "testdata".getBytes();
            
            try {
                logger.log(Level.FINE, "Attempting insertRecord...");
                store.insertRecord(key, data);
                logger.log(Level.FINE, "insertRecord completed successfully");
                
                // Verify data
                byte[] readData = store.readRecordData(key);
                logger.log(Level.FINE, "Data verification: " + new String(readData));
                
            } catch (IOException e) {
                logger.log(Level.FINE, "IOException caught: " + e.getMessage());
                logger.log(Level.FINE, "Delegate threw exception: " + exceptionOps.didThrow());
                logger.log(Level.FINE, "Delegate operation count: " + exceptionOps.getOperationCount());
                logger.log(Level.FINE, "Store state after exception: " + store.getState());
                
                // Test if subsequent operations fail
                testSubsequentOperation(store, key);
            }
            
        } finally {
            if (store != null) {
                try {
                    logger.log(Level.FINE, "Closing store...");
                    store.close();
                    logger.log(Level.FINE, "Store closed successfully");
                } catch (Exception e) {
                    logger.log(Level.FINE, "Exception during close: " + e.getMessage());
                }
            }
        }
    }
    
    private void testSubsequentOperation(FileRecordStore store, ByteSequence key) {
        logger.log(Level.FINE, "Testing subsequent operation after exception...");
        try {
            store.recordExists(key);
            logger.log(Level.FINE, "recordExists succeeded - store is still operational");
        } catch (IllegalStateException e) {
            logger.log(Level.FINE, () -> "recordExists correctly threw IllegalStateException: " + e.getMessage());
        } catch (Exception e) {
            logger.log(Level.FINE, () -> "recordExists threw unexpected exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
    
    private FileRecordStore createStoreWithException(int throwAtOperation) throws IOException {
        logger.log(Level.FINE, () -> String.format("Creating store with exception at operation %d", throwAtOperation));
        
        FileRecordStore.Builder builder = new FileRecordStore.Builder()
            .tempFile("debug-test-", ".db");
        
        FileRecordStore baseStore = builder.open();
        
        RandomAccessFile raf = new RandomAccessFile(baseStore.getFilePath().toFile(), "rw");
        DirectRandomAccessFile directOps = new DirectRandomAccessFile(raf);
        DelegatingExceptionOperations exceptionOps = new DelegatingExceptionOperations(directOps, throwAtOperation);
        
        FileRecordStore exceptionStore = builder.path(baseStore.getFilePath()).open();
        exceptionStore.fileOperations = exceptionOps;
        
        baseStore.close();
        
        logger.log(Level.FINE, "Store created with exception injection wrapper");
        return exceptionStore;
    }
}
