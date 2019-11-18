package com.github.simbo1905.srs.test;

import com.github.simbo1905.srs.FileRecordStore;
import com.github.simbo1905.srs.SimpleRecordStoreTest;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.github.simbo1905.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;
import static com.github.simbo1905.srs.FileRecordStore.stringToBytes;
import static org.hamcrest.Matchers.is;

public class SimpleRecordStoreApiTest {
    String fileName;
    FileRecordStore recordsFile = null;
    int initialSize;
    static final String TMP = System.getProperty("java.io.tmpdir");

    private final static Logger LOGGER = Logger.getLogger(SimpleRecordStoreTest.class.getName());

    public SimpleRecordStoreApiTest() {
        LOGGER.setLevel(Level.ALL);
        init(TMP + "junit.records", 0);
    }

    public void init(final String fileName, final int initialSize) {
        this.fileName = fileName;
        this.initialSize = initialSize;
        File db = new File(this.fileName);
        if (db.exists()) {
            db.delete();
        }
        db.deleteOnExit();
    }

    @After
    public void deleteDb() throws Exception {
        File db = new File(this.fileName);
        if (db.exists()) {
            db.delete();
        }
    }

    @Test
    public void testInsertOneRecordMapEntry() throws Exception {
        // given
        recordsFile = new FileRecordStore(fileName, initialSize);
        String uuid = UUID.randomUUID().toString();

        // when
        this.recordsFile.insertRecord(stringToBytes(uuid), stringToBytes(uuid));
        if( recordsFile.recordExists(stringToBytes(uuid))){
            this.recordsFile.deleteRecord(stringToBytes(uuid));
        }

        Assert.assertTrue(this.recordsFile.isEmpty());
        Assert.assertFalse(this.recordsFile.recordExists(stringToBytes(uuid)));

        this.recordsFile.insertRecord(stringToBytes(uuid), stringToBytes(uuid));

        Assert.assertFalse(this.recordsFile.isEmpty());
        Assert.assertTrue(this.recordsFile.recordExists(stringToBytes(uuid)));

        this.recordsFile.fsync();

        val data = this.recordsFile.readRecordData(FileRecordStore.keyOf(uuid.toString()));
        Assert.assertThat(FileRecordStore.bytesToString(data), is(uuid.toString()));

        this.recordsFile.updateRecord(stringToBytes(uuid), stringToBytes("updated"));

        this.recordsFile.fsync();

        this.recordsFile.close();

        // then
        recordsFile = new FileRecordStore(fileName, "r", false);
        val updated = this.recordsFile.readRecordData(FileRecordStore.keyOf(uuid.toString()));
        Assert.assertThat(recordsFile.bytesToString(updated), is("updated"));

    }


    @Test
    public void testKeyLengthRecordedInFile() throws Exception {
        // set a super sized key
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());
        // create a store with this key
        recordsFile = new FileRecordStore(fileName, initialSize);

        final String longestKey = Collections.nCopies( recordsFile.maxKeyLength - 5, "1" ).stream().collect( Collectors.joining() );
        byte[] key = FileRecordStore.stringToBytes(longestKey);
        byte[] value = FileRecordStore.stringToBytes(longestKey);
        recordsFile.insertRecord(key, value);


        // reset to the normal default
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
        recordsFile = new FileRecordStore(fileName, "r");

        String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(longestKey)));

        Assert.assertThat(put0, is(longestKey));

    }
}
