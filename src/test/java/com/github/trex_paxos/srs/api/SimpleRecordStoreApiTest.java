package com.github.trex_paxos.srs.api;

import com.github.trex_paxos.srs.ByteSequence;
import com.github.trex_paxos.srs.FileRecordStore;
import com.github.trex_paxos.srs.JulLoggingConfig;
import com.github.trex_paxos.srs.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.trex_paxos.srs.TestByteSequences.fromUtf8;
import static com.github.trex_paxos.srs.TestByteSequences.toUtf8String;
import static com.github.trex_paxos.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;

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
        final var key = fromUtf8(uuid);

        // when
        this.recordsFile.insertRecord(key, uuid.getBytes());
        if( recordsFile.recordExists(fromUtf8(uuid))){
            this.recordsFile.deleteRecord(fromUtf8(uuid));
        }

        Assert.assertTrue(this.recordsFile.isEmpty());
        Assert.assertFalse(this.recordsFile.recordExists(fromUtf8(uuid)));

        this.recordsFile.insertRecord(fromUtf8(uuid), uuid.getBytes());

        Assert.assertFalse(this.recordsFile.isEmpty());
        Assert.assertTrue(this.recordsFile.recordExists(fromUtf8(uuid)));

        this.recordsFile.fsync();

        final var data = this.recordsFile.readRecordData(fromUtf8(uuid));
        Assert.assertEquals(toUtf8String(data), uuid);

        this.recordsFile.updateRecord(fromUtf8(uuid), "updated".getBytes());

        this.recordsFile.fsync();

        this.recordsFile.close();

        // then
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).open();
        final var updated = this.recordsFile.readRecordData(fromUtf8(uuid));
        Assert.assertEquals("updated", new String(updated));
        Assert.assertEquals(1, recordsFile.size());

        Assert.assertEquals(recordsFile.keys().iterator().next(), key);
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
        ByteSequence key = fromUtf8(longestKey);
        byte[] value = longestKey.getBytes();
        recordsFile.insertRecord(key, value);

        // reset to the normal default
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
        recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).open();

        String put0 = new String(recordsFile.readRecordData(fromUtf8(longestKey)));

        Assert.assertEquals(put0, longestKey);

    }
}
