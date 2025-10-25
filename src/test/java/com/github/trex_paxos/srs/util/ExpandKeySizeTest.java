package com.github.trex_paxos.srs.util;

import com.github.trex_paxos.srs.ByteSequence;
import com.github.trex_paxos.srs.FileRecordStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

/**
 * Comprehensive test suite for ExpandKeySize utility.
 */
public class ExpandKeySizeTest {

    private static final String TMP = System.getProperty("java.io.tmpdir") + 
                                     System.getProperty("file.separator");
    
    private List<String> filesToCleanup = new ArrayList<>();

    @Before
    public void setUp() {
        filesToCleanup.clear();
    }

    @After
    public void tearDown() {
        for (String file : filesToCleanup) {
            new File(file).delete();
        }
    }

    private String getTempFile(String name) {
        String path = TMP + name;
        filesToCleanup.add(path);
        return path;
    }

    @Test
    public void testExpandKeySize_SimpleCase() throws IOException {
        String oldFile = getTempFile("test_expand_simple_old.db");
        String newFile = getTempFile("test_expand_simple_new.db");

        // Create a database with small key size
        int oldKeySize = 32;
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, oldKeySize, false)) {
            store.insertRecord(ByteSequence.of("key1".getBytes()), "value1".getBytes());
            store.insertRecord(ByteSequence.of("key2".getBytes()), "value2".getBytes());
            store.insertRecord(ByteSequence.of("key3".getBytes()), "value3".getBytes());
        }

        // Expand key size
        int newKeySize = 64;
        ExpandKeySize.expandKeySize(oldFile, newFile, newKeySize);

        // Verify new file
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(3, store.size());
            Assert.assertEquals(64, store.maxKeyLength);
            
            Assert.assertArrayEquals("value1".getBytes(), 
                store.readRecordData(ByteSequence.of("key1".getBytes())));
            Assert.assertArrayEquals("value2".getBytes(), 
                store.readRecordData(ByteSequence.of("key2".getBytes())));
            Assert.assertArrayEquals("value3".getBytes(), 
                store.readRecordData(ByteSequence.of("key3".getBytes())));
        }
    }

    @Test
    public void testExpandKeySize_EmptyDatabase() throws IOException {
        String oldFile = getTempFile("test_expand_empty_old.db");
        String newFile = getTempFile("test_expand_empty_new.db");

        // Create an empty database
        int oldKeySize = 32;
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, oldKeySize, false)) {
            // Empty
        }

        // Expand key size
        int newKeySize = 128;
        ExpandKeySize.expandKeySize(oldFile, newFile, newKeySize);

        // Verify new file
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(0, store.size());
            Assert.assertEquals(128, store.maxKeyLength);
        }
    }

    @Test
    public void testExpandKeySize_LargeData() throws IOException {
        String oldFile = getTempFile("test_expand_large_old.db");
        String newFile = getTempFile("test_expand_large_new.db");

        // Create database with larger data
        int oldKeySize = 32;
        byte[] largeValue = new byte[10000];
        Arrays.fill(largeValue, (byte) 'X');
        
        try (FileRecordStore store = new FileRecordStore(oldFile, 50000, oldKeySize, false)) {
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                store.insertRecord(ByteSequence.of(key.getBytes()), largeValue);
            }
        }

        // Expand key size
        int newKeySize = 100;
        ExpandKeySize.expandKeySize(oldFile, newFile, newKeySize);

        // Verify all data
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(10, store.size());
            Assert.assertEquals(100, store.maxKeyLength);
            
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                byte[] value = store.readRecordData(ByteSequence.of(key.getBytes()));
                Assert.assertArrayEquals(largeValue, value);
            }
        }
    }

    @Test
    public void testExpandKeySize_ManyRecords() throws IOException {
        String oldFile = getTempFile("test_expand_many_old.db");
        String newFile = getTempFile("test_expand_many_new.db");

        // Create database with many small records
        int oldKeySize = 16;
        int numRecords = 100;
        
        Map<String, String> testData = new HashMap<>();
        try (FileRecordStore store = new FileRecordStore(oldFile, 10000, oldKeySize, false)) {
            for (int i = 0; i < numRecords; i++) {
                String key = "k" + i;
                String value = "value_" + i;
                testData.put(key, value);
                store.insertRecord(ByteSequence.of(key.getBytes()), value.getBytes());
            }
        }

        // Expand key size
        int newKeySize = 200;
        ExpandKeySize.expandKeySize(oldFile, newFile, newKeySize);

        // Verify all records
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(numRecords, store.size());
            Assert.assertEquals(200, store.maxKeyLength);
            
            for (Map.Entry<String, String> entry : testData.entrySet()) {
                byte[] value = store.readRecordData(ByteSequence.of(entry.getKey().getBytes()));
                Assert.assertEquals(entry.getValue(), new String(value));
            }
        }
    }

    @Test
    public void testExpandKeySize_MaxKeyLength() throws IOException {
        String oldFile = getTempFile("test_expand_maxkey_old.db");
        String newFile = getTempFile("test_expand_maxkey_new.db");

        // Create database and expand to theoretical max
        int oldKeySize = 64;
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, oldKeySize, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        int newKeySize = 252; // MAX_KEY_LENGTH_THEORETICAL
        ExpandKeySize.expandKeySize(oldFile, newFile, newKeySize);

        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(252, store.maxKeyLength);
            Assert.assertArrayEquals("data".getBytes(), 
                store.readRecordData(ByteSequence.of("test".getBytes())));
        }
    }

    @Test
    public void testExpandKeySize_PreservesDataIntegrity() throws IOException {
        String oldFile = getTempFile("test_expand_integrity_old.db");
        String newFile = getTempFile("test_expand_integrity_new.db");

        // Create database with specific data patterns
        int oldKeySize = 32;
        try (FileRecordStore store = new FileRecordStore(oldFile, 5000, oldKeySize, false)) {
            // Different length values
            store.insertRecord(ByteSequence.of("short".getBytes()), "x".getBytes());
            store.insertRecord(ByteSequence.of("medium".getBytes()), "hello world!".getBytes());
            
            byte[] longValue = new byte[1000];
            for (int i = 0; i < longValue.length; i++) {
                longValue[i] = (byte) (i % 256);
            }
            store.insertRecord(ByteSequence.of("long".getBytes()), longValue);
            
            // Empty value
            store.insertRecord(ByteSequence.of("empty".getBytes()), new byte[0]);
        }

        // Expand
        ExpandKeySize.expandKeySize(oldFile, newFile, 128);

        // Verify exact data
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertArrayEquals("x".getBytes(), 
                store.readRecordData(ByteSequence.of("short".getBytes())));
            Assert.assertArrayEquals("hello world!".getBytes(), 
                store.readRecordData(ByteSequence.of("medium".getBytes())));
            
            byte[] longValue = new byte[1000];
            for (int i = 0; i < longValue.length; i++) {
                longValue[i] = (byte) (i % 256);
            }
            Assert.assertArrayEquals(longValue, 
                store.readRecordData(ByteSequence.of("long".getBytes())));
            
            Assert.assertArrayEquals(new byte[0], 
                store.readRecordData(ByteSequence.of("empty".getBytes())));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_SmallerSize() throws IOException {
        String oldFile = getTempFile("test_expand_smaller_old.db");
        String newFile = getTempFile("test_expand_smaller_new.db");

        // Create database with key size 64
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 64, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        // Try to "expand" to smaller size - should fail
        ExpandKeySize.expandKeySize(oldFile, newFile, 32);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_SameSize() throws IOException {
        String oldFile = getTempFile("test_expand_same_old.db");
        String newFile = getTempFile("test_expand_same_new.db");

        // Create database with key size 64
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 64, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        // Try to expand to same size - should fail
        ExpandKeySize.expandKeySize(oldFile, newFile, 64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_SourceDoesNotExist() throws IOException {
        String oldFile = getTempFile("nonexistent_old.db");
        String newFile = getTempFile("test_expand_notexist_new.db");

        ExpandKeySize.expandKeySize(oldFile, newFile, 128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_DestinationExists() throws IOException {
        String oldFile = getTempFile("test_expand_destexists_old.db");
        String newFile = getTempFile("test_expand_destexists_new.db");

        // Create both files
        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 32, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }
        try (FileRecordStore store = new FileRecordStore(newFile, 1000, 64, false)) {
            // Empty file but exists
        }

        // Should fail because destination exists
        ExpandKeySize.expandKeySize(oldFile, newFile, 128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_InvalidNewSize_TooLarge() throws IOException {
        String oldFile = getTempFile("test_expand_toolarge_old.db");
        String newFile = getTempFile("test_expand_toolarge_new.db");

        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 32, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        // 253 is too large (max is 252)
        ExpandKeySize.expandKeySize(oldFile, newFile, 253);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_InvalidNewSize_Zero() throws IOException {
        String oldFile = getTempFile("test_expand_zero_old.db");
        String newFile = getTempFile("test_expand_zero_new.db");

        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 32, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        ExpandKeySize.expandKeySize(oldFile, newFile, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandKeySize_InvalidNewSize_Negative() throws IOException {
        String oldFile = getTempFile("test_expand_negative_old.db");
        String newFile = getTempFile("test_expand_negative_new.db");

        try (FileRecordStore store = new FileRecordStore(oldFile, 1000, 32, false)) {
            store.insertRecord(ByteSequence.of("test".getBytes()), "data".getBytes());
        }

        ExpandKeySize.expandKeySize(oldFile, newFile, -1);
    }

    @Test
    public void testExpandKeySize_WithUpdatedRecords() throws IOException {
        String oldFile = getTempFile("test_expand_updated_old.db");
        String newFile = getTempFile("test_expand_updated_new.db");

        // Create database and do some updates
        int oldKeySize = 32;
        try (FileRecordStore store = new FileRecordStore(oldFile, 5000, oldKeySize, false)) {
            store.insertRecord(ByteSequence.of("key1".getBytes()), "original1".getBytes());
            store.insertRecord(ByteSequence.of("key2".getBytes()), "original2".getBytes());
            store.insertRecord(ByteSequence.of("key3".getBytes()), "original3".getBytes());
            
            // Update some records
            store.updateRecord(ByteSequence.of("key1".getBytes()), "updated1".getBytes());
            store.updateRecord(ByteSequence.of("key3".getBytes()), "updated3_longer_value".getBytes());
        }

        // Expand
        ExpandKeySize.expandKeySize(oldFile, newFile, 64);

        // Verify updated values are preserved
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals("updated1", 
                new String(store.readRecordData(ByteSequence.of("key1".getBytes()))));
            Assert.assertEquals("original2", 
                new String(store.readRecordData(ByteSequence.of("key2".getBytes()))));
            Assert.assertEquals("updated3_longer_value", 
                new String(store.readRecordData(ByteSequence.of("key3".getBytes()))));
        }
    }

    @Test
    public void testExpandKeySize_WithDeletedRecords() throws IOException {
        String oldFile = getTempFile("test_expand_deleted_old.db");
        String newFile = getTempFile("test_expand_deleted_new.db");

        // Create database with some deletions
        int oldKeySize = 32;
        try (FileRecordStore store = new FileRecordStore(oldFile, 5000, oldKeySize, false)) {
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                store.insertRecord(ByteSequence.of(key.getBytes()), ("value" + i).getBytes());
            }
            
            // Delete some records
            store.deleteRecord(ByteSequence.of("key2".getBytes()));
            store.deleteRecord(ByteSequence.of("key5".getBytes()));
            store.deleteRecord(ByteSequence.of("key8".getBytes()));
        }

        // Expand
        ExpandKeySize.expandKeySize(oldFile, newFile, 96);

        // Verify correct records remain
        try (FileRecordStore store = new FileRecordStore(newFile, "r", false)) {
            Assert.assertEquals(7, store.size());
            
            // Deleted keys should not exist
            Assert.assertFalse(store.recordExists(ByteSequence.of("key2".getBytes())));
            Assert.assertFalse(store.recordExists(ByteSequence.of("key5".getBytes())));
            Assert.assertFalse(store.recordExists(ByteSequence.of("key8".getBytes())));
            
            // Other keys should exist
            Assert.assertTrue(store.recordExists(ByteSequence.of("key0".getBytes())));
            Assert.assertTrue(store.recordExists(ByteSequence.of("key1".getBytes())));
            Assert.assertTrue(store.recordExists(ByteSequence.of("key3".getBytes())));
        }
    }
}
