package com.github.simbo1905.srs.test;

import com.github.simbo1905.srs.ByteSequence;
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

import static com.github.simbo1905.srs.ByteSequence.utf8ToString;
import static com.github.simbo1905.srs.ByteSequence.stringToUtf8;
import static com.github.simbo1905.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;
import static org.hamcrest.Matchers.is;

public class SimpleRecordStoreApiTest {
    String fileName;
    FileRecordStore recordsFile = null;
    int initialSize;
    static final String TMP = System.getProperty("java.io.tmpdir")+System.getProperty("file.separator");

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
        val key = stringToUtf8(uuid);

        // when
        this.recordsFile.insertRecord(key, uuid.getBytes());
        if( recordsFile.recordExists(stringToUtf8(uuid))){
            this.recordsFile.deleteRecord(stringToUtf8(uuid));
        }

        Assert.assertTrue(this.recordsFile.isEmpty());
        Assert.assertFalse(this.recordsFile.recordExists(stringToUtf8(uuid)));

        this.recordsFile.insertRecord(stringToUtf8(uuid), uuid.getBytes());

        Assert.assertFalse(this.recordsFile.isEmpty());
        Assert.assertTrue(this.recordsFile.recordExists(stringToUtf8(uuid)));

        this.recordsFile.fsync();

        val data = this.recordsFile.readRecordData(stringToUtf8(uuid));
        Assert.assertThat(utf8ToString(data), is(uuid));

        this.recordsFile.updateRecord(stringToUtf8(uuid), "updated".getBytes());

        this.recordsFile.fsync();

        this.recordsFile.close();

        // then
        recordsFile = new FileRecordStore(fileName, "r", false);
        val updated = this.recordsFile.readRecordData(stringToUtf8(uuid));
        Assert.assertThat(new String(updated), is("updated"));
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
        recordsFile = new FileRecordStore(fileName, initialSize);

        final String longestKey = Collections.nCopies( recordsFile.maxKeyLength - 5, "1" ).stream().collect( Collectors.joining() );
        ByteSequence key = stringToUtf8(longestKey);
        byte[] value = longestKey.getBytes();
        recordsFile.insertRecord(key, value);

        // reset to the normal default
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
        recordsFile = new FileRecordStore(fileName, "r");

        String put0 = new String(recordsFile.readRecordData(stringToUtf8(longestKey)));

        Assert.assertThat(put0, is(longestKey));

    }
}
