package com.github.simbo1905.nfp.srs.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.simbo1905.nfp.srs.*;

import static com.github.simbo1905.nfp.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;

public class SimpleRecordStoreApiTest extends JulLoggingConfig {
    String fileName;
    FileRecordStore recordsFile = null;
    int initialSize;
    static final String TMP = System.getProperty("java.io.tmpdir")+ FileSystems.getDefault().getSeparator();

    private final static Logger LOGGER = Logger.getLogger(SimpleRecordStoreApiTest.class.getName());

    public SimpleRecordStoreApiTest() {
        LOGGER.setLevel(Level.ALL);
        init(TMP + "junit.records", 0);
    }

    public void init(final String fileName, final int initialSize) {
        this.fileName = fileName;
        this.initialSize = initialSize;
        File db = new File(this.fileName);
        if (db.exists()) {
            if( !db.delete() ) {
                throw new IllegalStateException("Failed to delete " + db);
            }
        }
        db.deleteOnExit();
    }

    @After
    public void deleteDb() {
        File db = new File(this.fileName);
        if (db.exists()) {
            if( ! db.delete() ) {
              throw new IllegalStateException("Failed to delete " + db);
            }
        }
    }

    @Test
    public void testInsertOneRecordMapEntry() throws Exception {
        // given
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();
        String uuid = UUIDGenerator.generateUUID().toString();
        final var key = uuid.getBytes();

        // when
        this.recordsFile.insertRecord(key, uuid.getBytes());
        if( recordsFile.recordExists(uuid.getBytes())){
            this.recordsFile.deleteRecord(uuid.getBytes());
        }

        Assert.assertTrue(this.recordsFile.isEmpty());
        Assert.assertFalse(this.recordsFile.recordExists(uuid.getBytes()));

        this.recordsFile.insertRecord(uuid.getBytes(), uuid.getBytes());

        Assert.assertFalse(this.recordsFile.isEmpty());
        Assert.assertTrue(this.recordsFile.recordExists(uuid.getBytes()));

        this.recordsFile.fsync();

        final var data = this.recordsFile.readRecordData(uuid.getBytes());
        Assert.assertEquals(new String(data), uuid);

        this.recordsFile.updateRecord(uuid.getBytes(), "updated".getBytes());

        this.recordsFile.fsync();

        this.recordsFile.close();

        // then
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).open();
        final var updated = this.recordsFile.readRecordData(uuid.getBytes());
        Assert.assertEquals("updated", new String(updated));
        Assert.assertEquals(1, recordsFile.size());

        Assert.assertArrayEquals(recordsFile.keysBytes().iterator().next(), key);
    }


    @Test
    public void testKeyLengthRecordedInFile() throws Exception {
        // set a super sized key
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());
        // create a store with this key
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();

        final String longestKey = String.join("", Collections
            .nCopies(recordsFile.maxKeyLength - 5, "1"));
        byte[] key = longestKey.getBytes();
        byte[] value = longestKey.getBytes();
        recordsFile.insertRecord(key, value);

        // reset to the normal default
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).open();

        String put0 = new String(recordsFile.readRecordData(longestKey.getBytes()));

        Assert.assertEquals(put0, longestKey);

    }
}
