package com.github.trex_paxos.srs;

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

import static com.github.trex_paxos.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests that the simple random access storage 'db' works and does not get
 * corrupted under write errors.
 */
public class SimpleRecordStoreTest {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tT %4$s %2$s %5$s%6$s%n");
    }

    static {
        Logger.getLogger("").setLevel(Level.FINEST);
        Logger.getLogger("").getHandlers()[0].setLevel(Level.FINEST);
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

    interface InterceptedTestOperations {
        void performTestOperations(WriteCallback wc,
                                   String fileName) throws Exception;
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
        String fileName = TMP + System.getProperty("file.separator")+base;
        File file = new File(fileName);
        file.deleteOnExit();
        return fileName;
    }

    void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations) throws Exception {
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
                    recordsFile.close();
                    FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false);
                    try {
                        int count = possiblyCorruptedFile.getNumRecords();
                        for (val k : possiblyCorruptedFile.keys()) {
                            // readRecordData has a CRC32 check where the payload must match the header
                            possiblyCorruptedFile.readRecordData(k);
                            count--;
                        }
                        assertEquals(count, 0);
                    } catch (Exception e) {
                        FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
                        final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
                        throw new RuntimeException(msg, e);
                    } finally {
                        possiblyCorruptedFile.close();
                    }
                }
            }
        } finally {
            removeFiles(localFileNames);
        }
    }

    static final String TMP = System.getProperty("java.io.tmpdir");

    private final static Logger logger = Logger.getLogger(SimpleRecordStoreTest.class.getName());

    static {
        val msg = String.format(">TMP:%s", TMP);
        System.out.println(msg);
        logger.info(msg);
    }

    String fileName;
    int initialSize;

    public SimpleRecordStoreTest() {
        logger.setLevel(Level.ALL);
        init(TMP + System.getProperty("file.separator")+"junit.records", 0);
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

        logger.info("creating records file...");
        recordsFile = new FileRecordStore(fileName, initialSize);

        logger.info("adding a record...");
        final Date date = new Date();
        recordsFile.insertRecord(ByteSequence.stringToUtf8("foo.lastAccessTime"), serializerDate.apply(date));

        logger.info("reading record...");
        Date d = deserializerDate.apply(recordsFile.readRecordData(ByteSequence.stringToUtf8("foo.lastAccessTime")));

        Assert.assertEquals(date, d);

        logger.info("updating record...");
        recordsFile.updateRecord(ByteSequence.stringToUtf8("foo.lastAccessTime"), serializerDate.apply(new Date()));

        logger.info("reading record...");
        d = deserializerDate.apply(recordsFile.readRecordData(ByteSequence.stringToUtf8("foo.lastAccessTime")));

        logger.info("deleting record...");
        recordsFile.deleteRecord(ByteSequence.stringToUtf8("foo.lastAccessTime"));

        if (recordsFile.recordExists(ByteSequence.stringToUtf8("foo.lastAccessTime"))) {
            throw new Exception("Record not deleted");
        } else {
            logger.info("record successfully deleted.");
        }

        recordsFile = new FileRecordStore(fileName, "r", false);

        logger.info("test completed.");
    }

    @Test
    public void testInsertOneRecordWithIOExceptions() throws Exception {
        insertOneRecordWithIOExceptions("");
        insertOneRecordWithIOExceptions(string1k);
    }

    public void insertOneRecordWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            val key = ByteSequence.of("key".getBytes());
            val data = (dataPadding + "data").getBytes();
            recordsFile.insertRecord(key, data);

            // then
            val put0 = recordsFile.readRecordData(key);
            Assert.assertArrayEquals(put0, data);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testInsertTwoRecordWithIOExceptions() throws Exception {
        insertTwoRecordWithIOExceptions("");
        insertTwoRecordWithIOExceptions(string1k);
    }

    public void insertTwoRecordWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);
            val key2 = ByteSequence.of("key2".getBytes());
            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.insertRecord(key2, data2);

            // then
            val put1 = recordsFile.readRecordData(key1);
            val put2 = recordsFile.readRecordData(key2);

            Assert.assertArrayEquals(put1, data1);
            Assert.assertArrayEquals(put2, data2);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testInsertThenDeleteRecordWithIOExceptions() throws Exception {
        insertThenDeleteRecordWithIOExceptions("");
        insertThenDeleteRecordWithIOExceptions(string1k);
    }

    public void insertThenDeleteRecordWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);
            recordsFile.deleteRecord(key1);

            // then
            if (recordsFile.recordExists(key1)) {
                throw new Exception("Record not deleted");
            }

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }


    @Test
    public void testInsertTwoThenDeleteTwoRecordsWithIOExceptions() throws Exception {
        insertTwoThenDeleteTwoRecordsWithIOExceptions("");
        insertTwoThenDeleteTwoRecordsWithIOExceptions(string1k);
    }

    public void insertTwoThenDeleteTwoRecordsWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val key2 = ByteSequence.of("key2".getBytes());
            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.insertRecord(key2, data2);

            recordsFile.deleteRecord(key1);
            recordsFile.deleteRecord(key2);

            // then
            if (recordsFile.recordExists(key1)) {
                throw new Exception("Record not deleted");
            }
            // then
            if (recordsFile.recordExists(key2)) {
                throw new Exception("Record not deleted");
            }

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    final String string1k = Collections.nCopies(1024, "1").stream().collect(Collectors.joining());

    @Test
    public void testInsertTwoDeleteFirstInsertOneWithIOExceptions() throws Exception {
        insertTwoDeleteFirstInsertOneWithIOExceptions("");
        insertTwoDeleteFirstInsertOneWithIOExceptions(string1k);
    }

    public void insertTwoDeleteFirstInsertOneWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val key2 = ByteSequence.of("key2".getBytes());
            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.insertRecord(key2, data2);

            recordsFile.deleteRecord(key1);

            val key3 = ByteSequence.of("key3".getBytes());
            val data3 = (dataPadding + "data3").getBytes();
            recordsFile.insertRecord(key3, data3);

            // then
            if (recordsFile.recordExists(key1)) {
                throw new Exception("Record not deleted");
            }
            val put2 = recordsFile.readRecordData(key2);
            val put3 = recordsFile.readRecordData(key3);

            Assert.assertArrayEquals(put2, data2);
            Assert.assertArrayEquals(put3, data3);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testInsertTwoDeleteSecondInsertOneWithIOExceptions() throws Exception {
        insertTwoDeleteSecondInsertOneWithIOExceptions("");
        insertTwoDeleteSecondInsertOneWithIOExceptions(string1k);
    }

    void insertTwoDeleteSecondInsertOneWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val key2 = ByteSequence.of("key2".getBytes());
            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.insertRecord(key2, data2);

            recordsFile.deleteRecord(key1);

            val key3 = ByteSequence.of("key3".getBytes());
            val data3 = (dataPadding + "data3").getBytes();
            recordsFile.insertRecord(key3, data3);

            // then
            if (recordsFile.recordExists(key1)) {
                throw new Exception("Record not deleted");
            }
            val put2 = recordsFile.readRecordData(key2);
            val put3 = recordsFile.readRecordData(key3);

            Assert.assertArrayEquals(put2, data2);
            Assert.assertArrayEquals(put3, data3);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }
//        List<UUID> uuids = createUuid(3);
//
//        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
//            @Override
//            public void performTestOperations(WriteCallback wc, String fileName,
//                                              List<UUID> uuids,
//                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
//                // given
//                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
//                UUID uuid0 = uuids.get(0);
//                UUID uuid1 = uuids.get(1);
//                UUID uuid2 = uuids.get(2);
//
//                writeUuid(uuid0);
//                writeUuid(uuid1);
//                recordsFile.deleteRecord(stringToUtf8(uuid1.toString()));
//                writeUuid(uuid2);
//
//                // then
//                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(stringToUtf8(uuid0.toString())));
//                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(stringToUtf8(uuid2.toString())));
//                Assert.assertThat(put0, is(uuid0.toString()));
//                Assert.assertThat(put2, is(uuid2.toString()));
//                if (recordsFile.recordExists(stringToUtf8(uuid1.toString()))) {
//                    throw new Exception("Record not deleted");
//                }
//            }
//        }, uuids);
//    }
//
//    @Test
//    public void testInsertThreeDeleteSecondInsertOneWithIOExceptions() throws Exception {
//        List<UUID> uuids = createUuid(4);
//
//        verifyWorkWithIOExceptions(new InterceptedTestOperations() {
//            @Override
//            public void performTestOperations(WriteCallback wc, String fileName,
//                                              List<UUID> uuids,
//                                              AtomicReference<Set<Entry<String, String>>> written) throws Exception {
//                deleteFileIfExists(fileName);
//                // given
//                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);
//                UUID uuid0 = uuids.get(0);
//                UUID uuid1 = uuids.get(1);
//                UUID uuid2 = uuids.get(2);
//                UUID uuid3 = uuids.get(3);
//
//                writeUuid(uuid0);
//                writeUuid(uuid1);
//                writeUuid(uuid2);
//                recordsFile.deleteRecord(stringToUtf8(uuid1.toString()));
//                writeUuid(uuid3);
//
//                // then
//                String put0 = FileRecordStore.bytesToString(recordsFile.readRecordData(stringToUtf8(uuid0.toString())));
//                String put2 = FileRecordStore.bytesToString(recordsFile.readRecordData(stringToUtf8(uuid2.toString())));
//                String put3 = FileRecordStore.bytesToString(recordsFile.readRecordData(stringToUtf8(uuid3.toString())));
//                Assert.assertThat(put0, is(uuid0.toString()));
//                Assert.assertThat(put2, is(uuid2.toString()));
//                Assert.assertThat(put3, is(uuid3.toString()));
//                if (recordsFile.recordExists(stringToUtf8(uuid1.toString()))) {
//                    throw new Exception("Record not deleted");
//                }
//            }
//        }, uuids);
//    }
//

    @Test
    public void testUpdateOneRecordWithIOExceptions() throws Exception {
        updateOneRecordWithIOExceptions("");
        updateOneRecordWithIOExceptions(string1k);
    }

    void updateOneRecordWithIOExceptions(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.updateRecord(key1, data2);

            val put1 = recordsFile.readRecordData(key1);

            Assert.assertArrayEquals(put1, data2);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }


    @Test
    public void testUpdateExpandLastRecord() throws Exception {
        updateExpandLastRecord("");
        updateExpandLastRecord(string1k);
    }

    void updateExpandLastRecord(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val data2 = (dataPadding + "data2xxxxxxxxxxxxxxxx").getBytes();
            recordsFile.updateRecord(key1, data2);

            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, data2);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testUpdateExpandFirstRecord() throws Exception {
        updateExpandFirstRecord("");
        updateExpandFirstRecord(string1k);
    }

    void updateExpandFirstRecord(String dataPadding) throws Exception {
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

            // when
            val key1 = ByteSequence.of("key1".getBytes());
            val data1 = (dataPadding + "data1").getBytes();
            recordsFile.insertRecord(key1, data1);

            val key2 = ByteSequence.of("key2".getBytes());
            val data2 = (dataPadding + "data2").getBytes();
            recordsFile.insertRecord(key2, data2);

            val data1large = (dataPadding + "data1" + dataPadding).getBytes();
            recordsFile.updateRecord(key1, data1large);

            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, data1large);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testUpdateExpandMiddleRecordWithIOExceptions() throws Exception {
        val one = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val two = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        val three = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());
            val key3 = ByteSequence.of("key3".getBytes());
            recordsFile.insertRecord(key1, one);
            recordsFile.insertRecord(key2, one); // 256
            recordsFile.insertRecord(key3, three);

            //when
            recordsFile.updateRecord(key2, two); // 512

            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, one);
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, two);
            val put3 = (recordsFile.readRecordData(key3));
            Assert.assertArrayEquals(put3, three);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void testUpdateExpandOnlyRecord() throws Exception {
        val one = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val two = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            recordsFile.insertRecord(key1, one);

            // when
            recordsFile.updateRecord(key1, two); // 512

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, two);

            recordsFile = new FileRecordStore(fileName, "r", false);
        });
    }

    @Test
    public void indexOfFatRecordCausesHole() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        recordsFile = new FileRecordStore(fileName, initialSize);

        // given
        val key1 = ByteSequence.of("key1".getBytes());
        val key2 = ByteSequence.of("key2".getBytes());
        val key3 = ByteSequence.of("key3".getBytes());

        // when
        recordsFile.insertRecord(key1, narrow);

        recordsFile.insertRecord(key2, narrow);

        recordsFile.updateRecord(key2, wide);

        // then
        recordsFile.insertRecord(key3, narrow);
        recordsFile.updateRecord(key2, narrow);

        recordsFile = new FileRecordStore(fileName, "r", false);
    }

    @Test
    public void testUpdateShrinkLastRecordWithIOExceptions() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());

            // when
            recordsFile.insertRecord(key1, narrow);

            recordsFile.insertRecord(key2, wide);

            recordsFile.updateRecord(key2, narrow);

            // then
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, narrow);
        });
    }

    @Test
    public void testUpdateShrinkFirstRecordWithIOExceptions() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());

            // when
            recordsFile.insertRecord(key1, wide);

            recordsFile.insertRecord(key2, narrow);

            recordsFile.updateRecord(key1, narrow);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, narrow);
        });
    }

    @Test
    public void testUpdateShrinkMiddleRecordWithIOExceptions() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());
            val key3 = ByteSequence.of("key3".getBytes());

            // when
            recordsFile.insertRecord(key1, narrow);
            recordsFile.insertRecord(key2, wide);
            recordsFile.insertRecord(key3, narrow);

            recordsFile.updateRecord(key2, narrow);

            // then
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, narrow);
        });
    }

    @Test
    public void testUpdateShrinkOnlyRecordWithIOExceptions() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());

            // when
            recordsFile.insertRecord(key1, wide);
            recordsFile.updateRecord(key1, narrow);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, narrow);
        });
    }

    @Test
    public void testDeleteFirstEntries() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());

            // when
            recordsFile.insertRecord(key1, narrow);
            recordsFile.insertRecord(key2, wide);
            recordsFile.deleteRecord(key1);

            // then
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, wide);
        });
    }

    @Test
    public void testDeleteLastEntry() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());

            // when
            recordsFile.insertRecord(key1, narrow);
            recordsFile.insertRecord(key2, wide);
            recordsFile.deleteRecord(key2);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, narrow);
        });
    }

    @Test
    public void testDeleteMiddleEntriesWithIOExceptions() throws Exception {
        val narrow = Collections.nCopies(256, "1").stream().collect(Collectors.joining()).getBytes();
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());
            val key3 = ByteSequence.of("key3".getBytes());

            // when
            recordsFile.insertRecord(key1, narrow);
            recordsFile.insertRecord(key2, narrow);
            recordsFile.insertRecord(key3, wide);
            recordsFile.deleteRecord(key2);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, narrow);
            val put3 = recordsFile.readRecordData(key3);
            Assert.assertArrayEquals(put3, wide);
        });
    }

    @Test
    public void testDeleteOnlyEntryWithIOExceptions() throws Exception {
        val wide = Collections.nCopies(512, "1").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());

            // when
            recordsFile.insertRecord(key1, wide);
            recordsFile.deleteRecord(key1);

            // then
            assertEquals(0, recordsFile.size());
            assertEquals(false, recordsFile.recordExists(key1));
        });
    }

    @Test
    public void tesSplitFirstWithIOExceptions() throws Exception {
        val oneNarrow = Collections.nCopies(38, "1").stream().collect(Collectors.joining()).getBytes();
        val oneWide = Collections.nCopies(1024, "1").stream().collect(Collectors.joining()).getBytes();
        val twoNarrow = Collections.nCopies(38, "2").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());

            // when
            recordsFile.insertRecord(key1, oneWide);
            recordsFile.updateRecord(key1, oneNarrow);
            recordsFile.insertRecord(key2, twoNarrow);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, oneNarrow);
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, twoNarrow);
        });
    }

    @Test
    public void tesSplitLastWithIOExceptions() throws Exception {
        val oneNarrow = Collections.nCopies(38, "1").stream().collect(Collectors.joining()).getBytes();
        val twoNarrow = Collections.nCopies(38, "2").stream().collect(Collectors.joining()).getBytes();
        val twoWide = Collections.nCopies(1024, "2").stream().collect(Collectors.joining()).getBytes();
        val threeNarrow = Collections.nCopies(38, "3").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());
            val key3 = ByteSequence.of("key3".getBytes());

            // when
            recordsFile.insertRecord(key1, oneNarrow);
            recordsFile.insertRecord(key2, twoWide);
            recordsFile.updateRecord(key2, twoNarrow);
            recordsFile.insertRecord(key3, threeNarrow);

            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, oneNarrow);
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, twoNarrow);
            val put3 = recordsFile.readRecordData(key3);
            Assert.assertArrayEquals(put3, threeNarrow);
        });
    }

    @Test
    public void tesSplitMiddleWithIOExceptions() throws Exception {
        val oneNarrow = Collections.nCopies(38, "1").stream().collect(Collectors.joining()).getBytes();
        val twoWide = Collections.nCopies(1024, "2").stream().collect(Collectors.joining()).getBytes();
        val twoNarrow = Collections.nCopies(38, "2").stream().collect(Collectors.joining()).getBytes();
        val threeNarrow = Collections.nCopies(38, "3").stream().collect(Collectors.joining()).getBytes();
        val fourNarrow = Collections.nCopies(38, "4").stream().collect(Collectors.joining()).getBytes();
        verifyWorkWithIOExceptions((wc, fileName) -> {
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

            // given
            val key1 = ByteSequence.of("key1".getBytes());
            val key2 = ByteSequence.of("key2".getBytes());
            val key3 = ByteSequence.of("key3".getBytes());
            val key4 = ByteSequence.of("key4".getBytes());

            // when
            recordsFile.insertRecord(key1, oneNarrow);
            recordsFile.insertRecord(key2, twoWide);
            recordsFile.insertRecord(key3, threeNarrow);
            recordsFile.updateRecord(key2, twoNarrow);
            recordsFile.insertRecord(key4, fourNarrow);


            // then
            val put1 = recordsFile.readRecordData(key1);
            Assert.assertArrayEquals(put1, oneNarrow);
            val put2 = recordsFile.readRecordData(key2);
            Assert.assertArrayEquals(put2, twoNarrow);
            val put3 = recordsFile.readRecordData(key3);
            Assert.assertArrayEquals(put3, threeNarrow);
            val put4 = recordsFile.readRecordData(key4);
            Assert.assertArrayEquals(put4, fourNarrow);
        });
    }

    @Test
    public void testFreeSpaceInIndexWithIOExceptions() throws Exception {
        val oneLarge = Collections.nCopies( 1024, "1" ).stream().collect( Collectors.joining() ).getBytes();
        val twoSmall = Collections.nCopies( 38, "2" ).stream().collect( Collectors.joining() ).getBytes();
        val threeSmall = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() ).getBytes();

        verifyWorkWithIOExceptions((wc, fileName) -> {
            // set initial size equal to 2x header and 2x padded payload
            recordsFile = new RecordsFileSimulatesDiskFailures(fileName,
                    4 * FileRecordStore.DEFAULT_MAX_KEY_LENGTH, wc, false);

            // when
            recordsFile.insertRecord(ByteSequence.stringToUtf8("one"), oneLarge);
            recordsFile.insertRecord(ByteSequence.stringToUtf8("two"), twoSmall);
            val maxLen = recordsFile.getFileLength();
            recordsFile.deleteRecord(ByteSequence.stringToUtf8("one"));
            recordsFile.insertRecord(ByteSequence.stringToUtf8("three"), threeSmall);

            val finalLen = recordsFile.getFileLength();

            // then
            assertEquals(2, recordsFile.size());
            Assert.assertEquals(maxLen, finalLen);

        });
    }

    @Test
    public void testFreeSpaceInMiddleWithIOExceptions() throws Exception {
        val one = Collections.nCopies( 38, "1" ).stream().collect( Collectors.joining() ).getBytes();
        val twoLarge = Collections.nCopies( 1024, "2" ).stream().collect( Collectors.joining() ).getBytes();
        val three = Collections.nCopies( 38, "3" ).stream().collect( Collectors.joining() ).getBytes();
        val four = Collections.nCopies( 38, "4" ).stream().collect( Collectors.joining() ).getBytes();

        verifyWorkWithIOExceptions((wc, fileName) -> {
                recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

                // when
                recordsFile.insertRecord(ByteSequence.stringToUtf8("one"), (one));

                recordsFile.insertRecord(ByteSequence.stringToUtf8("two"), (twoLarge));

                recordsFile.insertRecord(ByteSequence.stringToUtf8("three"), (three));

                val maxLen = recordsFile.getFileLength();
                recordsFile.deleteRecord(ByteSequence.stringToUtf8("two"));

                recordsFile.insertRecord(ByteSequence.stringToUtf8("four"), (four));

                val finalLen = recordsFile.getFileLength();

                // then
                assertEquals(3, recordsFile.size());
                Assert.assertEquals(maxLen, finalLen);
        });
    }

    @Test
    public void testMaxKeySize() throws IOException {
        try{
            System.setProperty(String.format("%s.%s",
                    FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                    Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());

            // given
            recordsFile = new FileRecordStore(fileName, initialSize);

            // when
            val longestKey = Collections.nCopies( recordsFile.maxKeyLength, "1" ).stream().collect( Collectors.joining() ).getBytes();
            recordsFile.insertRecord(ByteSequence.of(longestKey), longestKey);

            // then
            val put0 = recordsFile.readRecordData(ByteSequence.of(longestKey));

            Assert.assertArrayEquals(put0, longestKey);
        } finally {
            System.setProperty(String.format("%s.%s",
                    FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                    Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
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

    @Test
    public void testKeyLengthRecordedInFile() throws Exception {
        // set a super sized key
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());

        val longestKey = Collections.nCopies( FileRecordStore.MAX_KEY_LENGTH_THEORETICAL - 5, "1" ).stream().collect( Collectors.joining() ).getBytes();

        try (FileRecordStore recordsFile = new FileRecordStore(fileName, initialSize)){
            recordsFile.insertRecord(ByteSequence.of(longestKey), longestKey);
        }

        // reset to the normal default
        System.setProperty(String.format("%s.%s",
                FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
                Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
        recordsFile = new FileRecordStore(fileName, "r");

        val put0 = recordsFile.readRecordData(ByteSequence.of(longestKey));

        Assert.assertArrayEquals(put0, longestKey);

    }

    byte[] bytes(int b){
        return new byte[]{(byte)b};
    }

    @Test
    public void testByteStringAsMapKey() {
        HashMap<ByteSequence, byte[]> kvs = new HashMap<>();
        IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b->{
            byte[] key = {(byte)b};
            byte[] value = {(byte)b};
            kvs.put(ByteSequence.of(key), value);
        });
        Assert.assertEquals(255, kvs.size());
        byte[] empty = {};
        kvs.put(ByteSequence.of(empty), bytes(1));
        Assert.assertArrayEquals(bytes(1), kvs.get(ByteSequence.of(empty)));
    }

    @Test
    public void testCanCallCloseTwice() throws IOException {
        FileRecordStore recordsFile = new FileRecordStore(fileName, initialSize);
        recordsFile.close();
        recordsFile.close();
    }
}
