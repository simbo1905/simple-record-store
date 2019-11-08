package com.github.simbo1905.srs.test;

import com.github.simbo1905.srs.BaseRecordStore;
import com.github.simbo1905.srs.FileRecordStore;
import com.github.simbo1905.srs.SimpleRecordStoreTests;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.simbo1905.srs.BaseRecordStore.deserializerString;
import static com.github.simbo1905.srs.BaseRecordStore.serializerString;
import static org.hamcrest.Matchers.is;

public class SimpleRecordStoreApiTests {
    String fileName;
    BaseRecordStore recordsFile = null;
    int initialSize;
    static final String TMP = System.getProperty("java.io.tmpdir");

    private final static Logger LOGGER = Logger.getLogger(SimpleRecordStoreTests.class.getName());

    public SimpleRecordStoreApiTests() {
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
        UUID uuids = UUID.randomUUID();
        String uuid = uuids.toString();

        // when
        this.recordsFile.insertRecord(serializerString.apply(uuid), serializerString.apply(uuid));
        this.recordsFile.fsync();
        this.recordsFile.close();

        // then
        recordsFile = new FileRecordStore(fileName, "r");
        val data = this.recordsFile.readRecordData(BaseRecordStore.keyOf(uuid.toString()));
        Assert.assertThat(deserializerString.apply(data), is(uuid.toString()));
    }

}
