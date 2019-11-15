package com.github.simbo1905.srs;

import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import static java.util.prefs.Preferences.MAX_KEY_LENGTH;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests that the simple random access storage 'db' works and does not get
 * corrupted under write errors.
 */
public class SimpleRecordStoreTests {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tT %4$s %2$s %5$s%6$s%n");
    }

    /**
     * A utility to recored how many times file write operations are called
     * and what the stack looks like for them.
     */
    private static final class StackCollectingWriteCallback implements
            WriteCallback {
        private final List<List<String>> writeStacks;

        private StackCollectingWriteCallback(List<List<String>> writeStacks) {
            this.writeStacks = writeStacks;
        }

        @Override
        public void onWrite() {
            List<String> stack = new ArrayList<String>();
            writeStacks.add(stack);
            StackTraceElement[] st = Thread.currentThread().getStackTrace();
            for (int index = 2; index < st.length; index++) {
                String s = st[index].toString();
                stack.add(s);
                if (!s.contains("com.github.simbo1905")) {
                    break;
                }
            }
        }
    }

    /**
     * A untility to throw an exception at a particular write operation.
     */
    private static final class CrashAtWriteCallback implements WriteCallback {
        final int crashAtIndex;
        int calls = 0;

        private CrashAtWriteCallback(int index) {
            crashAtIndex = index;
        }

        @Override
        public void onWrite() throws IOException {
            if (crashAtIndex == calls++) {
                throw new IOException("simulated IOException at call index: " + crashAtIndex);
            }
        }

    }

    static final String TMP = System.getProperty("java.io.tmpdir");

    private final static Logger logger = Logger.getLogger(SimpleRecordStoreTests.class.getName());

    String fileName;
    int initialSize;

    public SimpleRecordStoreTests() {
        logger.setLevel(Level.ALL);
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

    FileRecordStore recordsFile = null;

    @After
    public void deleteDb() throws Exception {
        File db = new File(this.fileName);
        if (db.exists()) {
            db.delete();
        }
    }

    public static class ByteUtils {
        private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        public static byte[] longToBytes(long x) {
            buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(0, x);
            return buffer.array();
        }

        public static long bytesToLong(byte[] bytes) {
            buffer.put(bytes, 0, bytes.length);
            buffer.flip();//need flip
            return buffer.getLong();
        }

    }

    Function<Date, byte[]> serializerDate = (date) -> {
        return ByteUtils.longToBytes(date.getTime());
    };

    Function<byte[], Date> deserializerDate = (bytes) -> {
        return new Date(ByteUtils.bytesToLong(bytes));
    };

    /**
     * Taken from http://www.javaworld.com/jw-01-1999/jw-01-step.html
     */
    @Test
    public void originalTest() throws Exception {
        recordsFile = new FileRecordStore(fileName, initialSize, false);

        logger.info("creating records file...");

        logger.info("adding a record...");
        final Date date = new Date();
        recordsFile.insertRecord(FileRecordStore.keyOf("foo.lastAccessTime"), serializerDate.apply(date));

        logger.info("reading record...");
        Date d = deserializerDate.apply(recordsFile.readRecordData(FileRecordStore.keyOf("foo.lastAccessTime")));
        // System.out.println("\tlast access was at: " + d.toString());

        Assert.assertEquals(date, d);

        logger.info("updating record...");
        recordsFile.updateRecord(FileRecordStore.keyOf("foo.lastAccessTime"), serializerDate.apply(new Date()));

        logger.info("reading record...");
        d = deserializerDate.apply(recordsFile.readRecordData(FileRecordStore.keyOf("foo.lastAccessTime")));
        // System.out.println("\tlast access was at: " + d.toString());

        logger.info("deleting record...");
        recordsFile.deleteRecord(FileRecordStore.keyOf("foo.lastAccessTime"));
        if (recordsFile.recordExists(FileRecordStore.keyOf("foo.lastAccessTime"))) {
            throw new Exception("Record not deleted");
        } else {
            logger.info("record successfully deleted.");
        }

        recordsFile = new FileRecordStore(fileName, "r", false);

        logger.info("test completed.");
    }

    @Test
    public void testInsertOneRecordWithIOExceptions() throws Exception {
        final List<UUID> uuids = createUuid(1);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {

                logger.info(String.format("writing to: " + fileName));

                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                UUID uuid = uuids.get(0);

                writeUuid(uuid);

                recordsFile = new FileRecordStore(fileName, "r", false);
            }
        }, uuids);
    }

    private void writeUuid(UUID k) throws IOException {
        writeUuid(k, k);
    }

    private void writeString(String k) throws IOException {
        writeString(k, k);
    }

    private void writeString(String k, String v) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k);
        byte[] value = FileRecordStore.stringToBytes(v);
        recordsFile.insertRecord(key, value);
    }

    private void writeUuid(UUID k, UUID v) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k.toString());
        byte[] value = FileRecordStore.stringToBytes(v.toString());
        recordsFile.insertRecord(key, value);
    }

    private void writeUuid(UUID k, UUID v, UUID v2) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k.toString());
        byte[] value = FileRecordStore.stringToBytes(v.toString() + v2.toString());
        recordsFile.insertRecord(key, value);
    }

    private void updateUuid(UUID k, UUID v) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k.toString());
        byte[] value = FileRecordStore.stringToBytes(v.toString());
        recordsFile.updateRecord(key, value);
    }

    private void updateString(String k, String v) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k);
        byte[] value = FileRecordStore.stringToBytes(v);
        recordsFile.updateRecord(key, value);
    }

    private void updateUuid(UUID k, UUID v1, UUID v2) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k.toString());
        byte[] value = FileRecordStore.stringToBytes(v1.toString() + v2.toString());
        recordsFile.updateRecord(key, value);
    }

    private void updateString(String k, String v1, String v2) throws IOException {
        byte[] key = FileRecordStore.stringToBytes(k);
        byte[] value = FileRecordStore.stringToBytes(v1 + v2);
        recordsFile.updateRecord(key, value);
    }

    @Test
    public void testInsertOneRecord() throws Exception {
        // given
        recordsFile = new FileRecordStore(fileName, initialSize, false);
        List<UUID> uuids = createUuid(1);

        // when
        UUID uuid0 = uuids.get(0);
        writeUuid(uuid0);

        // then
        String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));

        Assert.assertThat(put0, is(uuid0.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testInsertTwoRecords() throws Exception {
        // given
        recordsFile = new FileRecordStore(fileName, initialSize, false);
        List<UUID> uuids = createUuid(2);

        // when
        UUID uuid0 = uuids.get(0);
        writeUuid(uuid0);
        UUID uuid1 = uuids.get(1);
        writeUuid(uuid1);

        // then
        String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
        String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));

        Assert.assertThat(put0, is(uuid0.toString()));
        Assert.assertThat(put1, is(uuid1.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testInsertThenDeleteRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(1);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                recordsFile = new FileRecordStore(fileName, initialSize, false);

                // given
                UUID uuid0 = uuids.get(0);
                writeUuid(uuid0);

                // when
                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid0.toString()));

                // then
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid0.toString()))) {
                    throw new Exception("Record not deleted");
                }
            }
        }, uuids);
    }

    @Test
    public void testInsertTwoRecordsWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                // given
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // when
                UUID uuid0 = uuids.get(0);
                writeUuid(uuid0);
                UUID uuid1 = uuids.get(1);
                writeUuid(uuid1);

                // then
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));
                Assert.assertThat(put0, is(uuid0.toString()));
                Assert.assertThat(put1, is(uuid1.toString()));
            }
        }, uuids);
    }

    @Test
    public void testInsertTwoThenDeleteTwoRecordsWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                // given
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                UUID uuid0 = uuids.get(0);
                writeUuid(uuid0);
                UUID uuid1 = uuids.get(1);
                writeUuid(uuid1);

                // when
                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid0.toString()));
                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid1.toString()));

                // then
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid0.toString()))) {
                    throw new Exception("Record not deleted");
                }
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid1.toString()))) {
                    throw new Exception("Record not deleted");
                }
            }
        }, uuids);
    }


    @Test
    public void testInsertTwoDeleteFirstInsertOneWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(3);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                // given
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);
                UUID uuid2 = uuids.get(2);

                // System.out.println("\nbefore write uuid0 -----------------");

                writeUuid(uuid0);

                // System.out.println("\nafter write uuid0 -----------------");
                // System.out.println("\nmemory: ");
                //recordsFile.dumpHeaders(// System.out, true);
                // System.out.println("\ndisk: ");
                FileRecordStore.dumpFile(Level.INFO, fileName, false);

                // System.out.println("\nbefore write uuid1 -----------------");


                writeUuid(uuid1);

                // System.out.println("\nafter write uuid1 -----------------");
                // System.out.println("\nmemory: ");
                //recordsFile.dumpHeaders(// System.out, true);
                // System.out.println("\ndisk: ");
                FileRecordStore.dumpFile(Level.INFO, fileName, false);

                // System.out.println("\nbefore delete uuid0 -----------------");

                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid0.toString()));

                // System.out.println("\nafter delete uuid0 -----------------");
                // System.out.println("\nmemory: ");
                //ecordsFile.dumpHeaders(// System.out, true);
                // System.out.println("\ndisk: ");
                FileRecordStore.dumpFile(Level.INFO, fileName, false);
                // System.out.flush();

                // System.out.println("\nbefore write uuid2 -----------------");

                writeUuid(uuid2);

                // System.out.println("\nafter write uuid2 -----------------");
                // System.out.println("\nmemory: ");
                //recordsFile.dumpHeaders(// System.out, true);
                // System.out.println("\ndisk: ");
                //FileRecordStore.dumpFile(fileName, true);

                // then
                String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));
                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid2.toString())));
                Assert.assertThat(put1, is(uuid1.toString()));
                Assert.assertThat(put2, is(uuid2.toString()));
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid0.toString()))) {
                    throw new Exception("Record not deleted");
                }
            }
        }, uuids);
    }


    @Test
    public void testInsertTwoDeleteSecondInsertOneWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(3);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                // given
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);
                UUID uuid2 = uuids.get(2);

                writeUuid(uuid0);
                writeUuid(uuid1);
                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid1.toString()));
                writeUuid(uuid2);

                // then
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid2.toString())));
                Assert.assertThat(put0, is(uuid0.toString()));
                Assert.assertThat(put2, is(uuid2.toString()));
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid1.toString()))) {
                    throw new Exception("Record not deleted");
                }
            }
        }, uuids);
    }

    @Test
    public void testInsertThreeDeleteSecondInsertOneWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(4);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                // given
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);
                UUID uuid2 = uuids.get(2);
                UUID uuid3 = uuids.get(3);

                writeUuid(uuid0);
                writeUuid(uuid1);
                writeUuid(uuid2);
                recordsFile.deleteRecord(FileRecordStore.keyOf(uuid1.toString()));
                writeUuid(uuid3);

                // then
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid2.toString())));
                String put3 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid3.toString())));
                Assert.assertThat(put0, is(uuid0.toString()));
                Assert.assertThat(put2, is(uuid2.toString()));
                Assert.assertThat(put3, is(uuid3.toString()));
                if (recordsFile.recordExists(FileRecordStore.keyOf(uuid1.toString()))) {
                    throw new Exception("Record not deleted");
                }
            }
        }, uuids);
    }

    @Test
    public void testUpdateOneRecord() throws Exception {
        List<UUID> uuids = createUuid(2);
        recordsFile = new FileRecordStore(fileName, initialSize, false);

        // given
        UUID uuid0 = uuids.get(0);
        UUID uuidUpdated = uuids.get(1);

        // when
        writeUuid(uuid0);
        updateUuid(uuid0, uuidUpdated);

        String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
        Assert.assertThat(put, is(uuidUpdated.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testUpdateOneRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);

        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID uuid0 = uuids.get(0);
                UUID uuidUpdated = uuids.get(1);

                // when
                writeUuid(uuid0);
                updateUuid(uuid0, uuidUpdated);

                String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                Assert.assertThat(put, is(uuidUpdated.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateExpandLastRecord() throws Exception {
        List<UUID> uuids = createUuid(2);
        deleteFileIfExists(fileName);
        recordsFile = new FileRecordStore(fileName, initialSize, false);

        // given
        UUID first = uuids.get(0);
        UUID last = uuids.get(1);

        // when
        writeUuid(first);
        writeUuid(last);
        updateUuid(last, last, last);

        String pfirst = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
        Assert.assertThat(pfirst, is(first.toString()));
        String pLast = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(last.toString())));
        Assert.assertThat(pLast, is(last.toString() + last.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }


    @Test
    public void testUpdateExpandLastRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID first = uuids.get(0);
                UUID last = uuids.get(1);

                // when
                writeUuid(first);
                writeUuid(last);
                updateUuid(last, last, last);

                String pfirst = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
                Assert.assertThat(pfirst, is(first.toString()));
                String pLast = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(last.toString())));
                Assert.assertThat(pLast, is(last.toString() + last.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateExpandFirstRecord() throws Exception {
        List<UUID> uuids = createUuid(2);
        recordsFile = new FileRecordStore(fileName, initialSize, false);

        // given
        UUID first = uuids.get(0);
        UUID last = uuids.get(1);

        // when
        writeUuid(first);
        writeUuid(last);
        updateUuid(first, last, last);

        String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
        Assert.assertThat(put, is(last.toString() + last.toString()));
        String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(last.toString())));
        Assert.assertThat(put2, is(last.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testUpdateExpandFirstRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID first = uuids.get(0);
                UUID last = uuids.get(1);

                // when
                writeUuid(first);
                writeUuid(last);

                updateUuid(first, last, last);

                String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
                Assert.assertThat(put, is(last.toString() + last.toString()));
                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(last.toString())));
                Assert.assertThat(put2, is(last.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateExpandOnlyRecord() throws Exception {
        List<UUID> uuids = createUuid(1);
        recordsFile = new FileRecordStore(fileName, initialSize, false);

        // given
        UUID first = uuids.get(0);

        // when
        writeUuid(first);
        updateUuid(first, first, first);

        String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
        Assert.assertThat(put, is(first.toString() + first.toString()));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testUpdateExpandOnlyRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(1);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID first = uuids.get(0);

                // when
                writeUuid(first);
                updateUuid(first, first, first);

                String put = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(first.toString())));
                Assert.assertThat(put, is(first.toString() + first.toString()));
            }
        }, uuids);
    }

    @Test
    public void indexOfFatRecordCausesHole() throws Exception {

        AtomicReference<Map<String, RecordHeader>> hook = new AtomicReference<>();

        recordsFile = new FileRecordStore(fileName, initialSize, false) {
            {
                hook.set(this.memIndex);
            }
        };
        // given
        List<UUID> uuids = createUuid(4);
        UUID uuid0 = uuids.get(0);
        UUID uuid1 = uuids.get(1);
        UUID uuid2 = uuids.get(2);
        UUID uuid3 = uuids.get(3);

        // when
        writeUuid(uuid0);

        writeUuid(uuid1);

        updateUuid(uuid1, uuid1, uuid1);

        // then
        writeUuid(uuid2);

        updateUuid(uuid1, uuid3);

        recordsFile = new FileRecordStore(fileName, "r", false);

    }

    @Test
    public void testUpdateShrinkLastRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);

                // when
                writeUuid(uuid0);
                writeUuid(uuid1, uuid1, uuid1);
                updateUuid(uuid1, uuid0);

                //
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                Assert.assertThat(put0, is(uuid0.toString()));
                String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));
                Assert.assertThat(put1, is(uuid0.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateShrinkFirstRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);

                // when
                writeUuid(uuid0, uuid0, uuid0);
                writeUuid(uuid1, uuid1);
                updateUuid(uuid0, uuid1);

                //
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                Assert.assertThat(put0, is(uuid1.toString()));
                String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));
                Assert.assertThat(put1, is(uuid1.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateShrinkMiddleRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(3);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);
                UUID uuid2 = uuids.get(2);

                // when
                writeUuid(uuid0, uuid0);
                writeUuid(uuid1, uuid1, uuid1);
                writeUuid(uuid2, uuid2);
                updateUuid(uuid1, uuid2);

                //
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                Assert.assertThat(put0, is(uuid0.toString()));
                String put1 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid1.toString())));
                Assert.assertThat(put1, is(uuid2.toString()));
                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid2.toString())));
                Assert.assertThat(put2, is(uuid2.toString()));
            }
        }, uuids);
    }

    @Test
    public void testUpdateShrinkOnlyRecordWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(2);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc, String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // given
                UUID uuid0 = uuids.get(0);
                UUID uuid1 = uuids.get(1);

                // when
                writeUuid(uuid0, uuid0, uuid0);
                updateUuid(uuid0, uuid1);

                //
                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(uuid0.toString())));
                Assert.assertThat(put0, is(uuid1.toString()));
            }
        }, uuids);
    }

    private void deleteFileIfExists(String path) {
        val file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testDeleteFirstEntries() throws Exception {
        List<UUID> uuids = createUuid(4);
        recordsFile = new FileRecordStore(fileName, initialSize, false);
        String smallEntry = uuids.get(0).toString();
        String largeEntry = uuids.get(1).toString() + uuids.get(2).toString() + uuids.get(3).toString();

        // when
        recordsFile.insertRecord(FileRecordStore.keyOf("small"), FileRecordStore.stringToBytes(smallEntry));
        logger.info("after insert small ------");
        recordsFile.insertRecord(FileRecordStore.keyOf("larger"), FileRecordStore.stringToBytes(largeEntry));
        logger.info("after insert larger ------");
        recordsFile.deleteRecord(FileRecordStore.keyOf("small"));
        logger.info("after delete small ------");
        String large = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf("larger")));
        Assert.assertThat(large, is(largeEntry));

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testDeleteFirstEntriesWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(4);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                String smallEntry = uuids.get(0).toString();
                String largeEntry = uuids.get(1).toString() + uuids.get(2).toString() + uuids.get(3).toString();

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("small"), FileRecordStore.stringToBytes(smallEntry));
                recordsFile.insertRecord(FileRecordStore.keyOf("larger"), FileRecordStore.stringToBytes(largeEntry));
                recordsFile.deleteRecord(FileRecordStore.keyOf("small"));
                String large = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf("larger")));
                Assert.assertThat(large, is(largeEntry));
            }
        }, uuids);
    }

    @Test
    public void testDeleteLastEntriesWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(4);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                String smallEntry = uuids.get(0).toString();
                String largeEntry = uuids.get(1).toString() + uuids.get(2).toString() + uuids.get(3).toString();

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("small"), FileRecordStore.stringToBytes(smallEntry));
                recordsFile.insertRecord(FileRecordStore.keyOf("small2"), FileRecordStore.stringToBytes(smallEntry)); // expansion reorders first couple of entries so try three
                recordsFile.insertRecord(FileRecordStore.keyOf("larger1"), FileRecordStore.stringToBytes(largeEntry));
                recordsFile.deleteRecord(FileRecordStore.keyOf("small2"));
                recordsFile.deleteRecord(FileRecordStore.keyOf("larger1"));
                String small = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf("small")));
                Assert.assertThat(small, is(smallEntry));
            }
        }, uuids);
    }

    @Test
    public void testDeleteMiddleEntriesWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(4);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                String smallEntry = uuids.get(0).toString();
                String largeEntry = uuids.get(1).toString() + uuids.get(2).toString() + uuids.get(3).toString();

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("small"), FileRecordStore.stringToBytes(smallEntry));
                recordsFile.insertRecord(FileRecordStore.keyOf("small2"), FileRecordStore.stringToBytes(smallEntry)); // expansion reorders first couple of entries so try three
                recordsFile.insertRecord(FileRecordStore.keyOf("larger1"), FileRecordStore.stringToBytes(largeEntry));
                recordsFile.deleteRecord(FileRecordStore.keyOf("small2"));

                // then
                String small = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf("small")));
                Assert.assertThat(small, is(smallEntry));
                String large = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf("larger1")));
                Assert.assertThat(large, is(largeEntry));
            }
        }, uuids);
    }

    @Test
    public void testDeleteOnlyEntryWithIOExceptions() throws Exception {
        List<UUID> uuids = createUuid(1);
        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName,
                                              List<UUID> uuids,
                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
                String smallEntry = uuids.get(0).toString();

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("small"), FileRecordStore.stringToBytes(smallEntry));
                recordsFile.deleteRecord(FileRecordStore.keyOf("small"));

                // then
                Assert.assertTrue(recordsFile.isEmpty());
            }
        }, uuids);
    }

    private void removeFiles(List<String> localFileNames) {
        for (String file : localFileNames) {
            File f = new File(file);
            f.delete();
        }
    }

    private String stackToString(List<String> stack) {
        StringBuilder sb = new StringBuilder();
        for (String s : stack) {
            sb.append("\\n\\t");
            sb.append(s);
        }
        return sb.toString();
    }

    private String fileName(String base) {
        String fileName = TMP + base;
        File file = new File(fileName);
        file.deleteOnExit();
        return fileName;
    }

    static interface InterceptedTestOperations {
        void performTestOperations(WriteCallback wc,
                                   String fileName,
                                   List<UUID> uuids,
                                   AtomicReference<Set<Entry<String, String>>> written) throws Exception;
    }


    public static List<UUID> createUuid(int count) {
        List<UUID> uuids = new ArrayList<UUID>(count);
        for (int index = 0; index < count; index++) {
            uuids.add(UUID.randomUUID());
        }
        return uuids;
    }

    void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations, List<UUID> uuids) throws Exception {
        final List<List<String>> writeStacks = new ArrayList<List<String>>();

        WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

        final List<String> localFileNames = new ArrayList<String>();
        final String recordingFile = fileName("record");
        localFileNames.add(recordingFile);

        final AtomicReference<Set<Entry<String, String>>> written = new AtomicReference<>(new HashSet<>());
        interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile, uuids, written);

        try {
            for (int index = 0; index < writeStacks.size(); index++) {
                final List<String> stack = writeStacks.get(index);
                final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
                final String localFileName = fileName("crash" + index);
                localFileNames.add(localFileName);
                try {
                    interceptedOperations.performTestOperations(crashAt, localFileName, uuids, written);
                } catch (Exception ioe) {
                    FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false);
                    try {
                        int count = possiblyCorruptedFile.getNumRecords();
                        for (String k : possiblyCorruptedFile.keys()) {
                            // readRecordData has a CRC32 check where the payload must match the header
                            FileRecordStore.bytesToString(possiblyCorruptedFile.readRecordData(FileRecordStore.keyOf(k)));
                            count--;
                        }
                        assertThat(count, is(0));
                    } catch (Exception e) {
                        FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
                        final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
                        throw new RuntimeException(msg, e);
                    }
                }
            }
        } finally {
            removeFiles(localFileNames);
        }
    }


    void verifyWorkWithIOExceptions2(InterceptedTestOperations2 interceptedOperations, List<UUID> uuids) throws Exception {
        final List<List<String>> writeStacks = new ArrayList<List<String>>();

        WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

        final List<String> localFileNames = new ArrayList<String>();
        final String recordingFile = fileName("record");
        localFileNames.add(recordingFile);

        final AtomicReference<Set<Entry<String, String>>> written = new AtomicReference<>(new HashSet<>());
        interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile);

        try {
            for (int index = 0; index < writeStacks.size(); index++) {
                final List<String> stack = writeStacks.get(index);
                final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
                final String localFileName = fileName("crash" + index);
                localFileNames.add(localFileName);
                try {
                    interceptedOperations.performTestOperations(crashAt, localFileName);
                } catch (Exception ioe) {
                    FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false);
                    try {
                        int count = possiblyCorruptedFile.getNumRecords();
                        for (String k : possiblyCorruptedFile.keys()) {
                            // readRecordData has a CRC32 check where the payload must match the header
                            FileRecordStore.bytesToString(possiblyCorruptedFile.readRecordData(FileRecordStore.keyOf(k)));
                            count--;
                        }
                        assertThat(count, is(0));
                    } catch (Exception e) {
                        FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
                        final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
                        throw new RuntimeException(msg, e);
                    }
                }
            }
        } finally {
            removeFiles(localFileNames);
        }
    }


    static interface InterceptedTestOperations2 {
        void performTestOperations(WriteCallback wc,
                                   String fileName) throws Exception;
    }

    void verifyWorkWithIOExceptions2(InterceptedTestOperations2 interceptedOperations) throws Exception {
        final List<List<String>> writeStacks = new ArrayList<List<String>>();

        WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

        final List<String> localFileNames = new ArrayList<String>();
        final String recordingFile = fileName("record");
        localFileNames.add(recordingFile);

        final AtomicReference<Set<Entry<String, String>>> written = new AtomicReference<>(new HashSet<>());
        interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile);

        try {
            for (int index = 0; index < writeStacks.size(); index++) {
                final List<String> stack = writeStacks.get(index);
                final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
                final String localFileName = fileName("crash" + index);
                localFileNames.add(localFileName);
                try {
                    interceptedOperations.performTestOperations(crashAt, localFileName);
                } catch (Exception ioe) {
                    FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false);
                    try {
                        int count = possiblyCorruptedFile.getNumRecords();
                        for (String k : possiblyCorruptedFile.keys()) {
                            // readRecordData has a CRC32 check where the payload must match the header
                            FileRecordStore.bytesToString(possiblyCorruptedFile.readRecordData(FileRecordStore.keyOf(k)));
                            count--;
                        }
                        assertThat(count, is(0));
                    } catch (Exception e) {
                        FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
                        final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
                        throw new RuntimeException(msg, e);
                    }
                }
            }
        } finally {
            removeFiles(localFileNames);
        }
    }

    @Test
    public void tesSplitFirstWithIOExceptions() throws Exception {

        final String oneSmall = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() );
        final String oneLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );
        final String twoSmall = Collections.nCopies( 38, "2" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // when

                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneLarge));

                recordsFile.updateRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneSmall));

                recordsFile.insertRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoSmall));

                // then
                Assert.assertEquals(2, recordsFile.size());
            }
        });
    }

    @Test
    public void tesSplitLastWithIOExceptions() throws Exception {

        final String oneSmall = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() );
        final String twoLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );
        final String twoSmall = Collections.nCopies( 38, "2" ).stream().collect( Collectors.joining() );
        final String threeSmall = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneSmall));
                recordsFile.insertRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoLarge));
                recordsFile.updateRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoSmall));
                recordsFile.insertRecord(FileRecordStore.keyOf("three"), FileRecordStore.stringToBytes(threeSmall));

                // then
                Assert.assertEquals(3, recordsFile.size());
            }
        });
    }

    @Test
    public void testUpdateLargeMiddleWithIOExceptions() throws Exception {

        final String oneSmall = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() );
        final String twoLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );
        final String twoSmall = Collections.nCopies( 38, "2" ).stream().collect( Collectors.joining() );
        final String threeSmall = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneSmall));
                recordsFile.insertRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoLarge));
                recordsFile.updateRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoSmall));
                recordsFile.insertRecord(FileRecordStore.keyOf("three"), FileRecordStore.stringToBytes(threeSmall));

                // then
                Assert.assertEquals(3, recordsFile.size());
            }
        });
    }

    @Test
    public void testUpdateExpandWithIOExceptions() throws Exception {

        final String oneSmall = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() );
        final String oneLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneSmall));

                val length = recordsFile.getFileLength();

                recordsFile.updateRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneLarge));

                // then
                Assert.assertEquals(1, recordsFile.size());
            }
        });
    }

    @Test
    public void testFreeSpaceInIndexWithIOExceptions() throws Exception {
        final String oneLarge = Collections.nCopies( 1024, "1" ).stream().collect( Collectors.joining() );
        final String twoSmall = Collections.nCopies( 38, "2" ).stream().collect( Collectors.joining() );
        final String threeSmall = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);

                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(oneLarge));
                logger.info("after insert one ------------------");

                recordsFile.insertRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoSmall));
                logger.info("after insert two ------------------");

                val maxLen = recordsFile.getFileLength();
                logger.info("after getFileLength ------------------");

                recordsFile.deleteRecord(FileRecordStore.keyOf("one"));
                logger.info("after delete one ------------------");

                recordsFile.insertRecord(FileRecordStore.keyOf("three"), FileRecordStore.stringToBytes(threeSmall));
                logger.info("after insert three ------------------");

                val finalLen = recordsFile.getFileLength();

                // then
                Assert.assertEquals(2, recordsFile.size());
                assertEquals(maxLen, finalLen);

            }
        });
    }

    static {
        Logger.getLogger("").setLevel(Level.FINEST);
        Logger.getLogger("").getHandlers()[0].setLevel(Level.FINEST);
    }

    @Test
    public void testFreeSpaceInMiddleWithIOExceptions() throws Exception {
        final String one = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() );
        final String twoLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );
        final String three = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() );
        final String four = Collections.nCopies( 38, "4" ).stream().collect( Collectors.joining() );

        verifyWorkWithIOExceptions2(new InterceptedTestOperations2() {
            @Override
            public void performTestOperations(WriteCallback wc,
                                              String fileName) throws Exception {
                deleteFileIfExists(fileName);
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

                // when
                recordsFile.insertRecord(FileRecordStore.keyOf("one"), FileRecordStore.stringToBytes(one));

                recordsFile.insertRecord(FileRecordStore.keyOf("two"), FileRecordStore.stringToBytes(twoLarge));

                recordsFile.insertRecord(FileRecordStore.keyOf("three"), FileRecordStore.stringToBytes(three));

                val maxLen = recordsFile.getFileLength();
                recordsFile.deleteRecord(FileRecordStore.keyOf("two"));

                recordsFile.insertRecord(FileRecordStore.keyOf("four"), FileRecordStore.stringToBytes(four));

                val finalLen = recordsFile.getFileLength();

                // then
                Assert.assertEquals(3, recordsFile.size());
                assertEquals(maxLen, finalLen);
            }
        });
    }

    @Test
    public void testMaxKeySize() throws IOException {
        try{
            System.setProperty(FileRecordStore.class.getCanonicalName()+".MAX_KEY_LENGTH", "256");

            // given
            recordsFile = new FileRecordStore(fileName, initialSize, false);

            // when
            final String longestKey = Collections.nCopies( MAX_KEY_LENGTH, "1" ).stream().collect( Collectors.joining() );
            writeString(longestKey);

            // then
            String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(FileRecordStore.keyOf(longestKey)));

            Assert.assertThat(put0, is(longestKey.toString()));
        } finally {
            System.setProperty(FileRecordStore.class.getCanonicalName()+".MAX_KEY_LENGTH", "64");
        }
    }

    @Test
    public void testCrc32asUnsignedInteger() {
        final String data = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() );
        val crc32 = new CRC32();
        crc32.update(data.getBytes(), 0, 1024);
        long crcLong = crc32.getValue();
        int crcInt = (int) crcLong & 0xFFFFFFFF;
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(crcInt);
        buffer.flip();
        int crcIntOut = buffer.getInt();
        long crcLongOut = crcIntOut & 0xFFFFFFFFL;

        assertEquals(crcLong, crcLongOut);
    }
}
