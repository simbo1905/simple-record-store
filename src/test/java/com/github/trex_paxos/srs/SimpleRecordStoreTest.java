package com.github.trex_paxos.srs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import static com.github.trex_paxos.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;
import static com.github.trex_paxos.srs.TestByteSequences.fromUtf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/// Tests that the simple random access storage 'db' works and does not get
/// corrupted under write errors.
public class SimpleRecordStoreTest extends JulLoggingConfig {

  static final String TMP = System.getProperty("java.io.tmpdir");
  private final static Logger logger = Logger.getLogger(SimpleRecordStoreTest.class.getName());

  final String string1k = String.join("", Collections.nCopies(1024, "1"));
  private final Function<Date, byte[]> serializerDate = (date) -> ByteUtils.longToBytes(date.getTime());
  private final Function<byte[], Date> deserializerDate = (bytes) -> new Date(ByteUtils.bytesToLong(bytes));
  String fileName;
  int initialSize;
  FileRecordStore recordsFile = null;

  public SimpleRecordStoreTest() {
    logger.info("SimpleRecordStoreTest constructor");
    // unique file per test class instance
    String unique = getClass().getSimpleName() + "_" + System.nanoTime();
    this.fileName = Paths.get(System.getProperty("java.io.tmpdir"), unique + ".records").toString();
    this.initialSize = 0;
    File db = new File(this.fileName);
    if (db.exists()) {
      logger.fine("init deleting " + db);
      //noinspection ResultOfMethodCallIgnored
      db.delete();
    }
    db.deleteOnExit();
  }

  @After
  public void deleteDb() {
    File db = new File(this.fileName);
    if (db.exists()) {
      logger.fine("deleteDb deleting " + db);
      if (!db.delete()) throw new IllegalStateException("could not delete " + db);
    }
  }

  /// Taken from <a href="http://www.javaworld.com/jw-01-1999/jw-01-step.html">original source</a>
  @Test
  public void testOriginalArticle() throws Exception {
    logger.info("creating records file...");
    recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();

    logger.info("adding a record...");
    final Date date = new Date();
    recordsFile.insertRecord(fromUtf8("foo.lastAccessTime"), serializerDate.apply(date));

    logger.info("reading record...");
    Date d = deserializerDate.apply(recordsFile.readRecordData(fromUtf8("foo.lastAccessTime")));

    Assert.assertEquals(date, d);

    logger.info("updating record...");
    recordsFile.updateRecord(fromUtf8("foo.lastAccessTime"), serializerDate.apply(new Date()));

    logger.info("reading record...");
    //noinspection UnusedAssignment
    d = deserializerDate.apply(recordsFile.readRecordData(fromUtf8("foo.lastAccessTime")));

    logger.info("deleting record...");
    recordsFile.deleteRecord(fromUtf8("foo.lastAccessTime"));

    if (recordsFile.recordExists(fromUtf8("foo.lastAccessTime"))) {
      throw new Exception("Record not deleted");
    } else {
      logger.info("record successfully deleted.");
    }

    recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();

    logger.info("test completed.");
  }

  @Test
  public void testMoveUpdatesPositionMap() throws Exception {
    final var recordStore = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(100).maxKeyLength(64).disablePayloadCrc32(false).open();
    final String value = String.join("", Collections.nCopies(100, "x"));
    IntStream.range(0, 4).forEach(i -> {
      final String key = String.join("", Collections.nCopies(64, "" + i));
      try {
        recordStore.insertRecord(fromUtf8(key), value.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }

  @Test
  public void testDoubleInsertIntoLargeFile() throws Exception {
    final var recordStore = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(1000).maxKeyLength(64).disablePayloadCrc32(false).open();
    final String value = String.join("", Collections.nCopies(100, "x"));
    final var key1 = fromUtf8(String.join("", Collections.nCopies(4, "1")));
    final var key2 = fromUtf8(String.join("", Collections.nCopies(4, "2")));
    recordStore.insertRecord(key1, value.getBytes());
    recordStore.insertRecord(key2, value.getBytes());
  }

  @Test
  public void testInsertOneRecordWithIOExceptions() throws Exception {
    insertOneRecordWithIOExceptions("");
    insertOneRecordWithIOExceptions(string1k);
  }

  public void insertOneRecordWithIOExceptions(String dataPadding) throws Exception {
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

      final var key = ByteSequence.of("key".getBytes());
      final var data = (dataPadding + "data").getBytes();
      
      // Only insert if key doesn't exist (handle replay scenarios)
      if (!recordsFile.recordExists(key)) {
        if (!recordsFile.recordExists(key)) { recordsFile.insertRecord(key, data); }
      }

      // then
      final var put0 = recordsFile.readRecordData(key);
      Assert.assertArrayEquals(put0, data);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations) throws Exception {
    final List<List<String>> writeStacks = new ArrayList<>();

    WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

    final String recordingFile = fileName("record");

    interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile);

    for (int index = 0; index < writeStacks.size(); index++) {
      final List<String> stack = writeStacks.get(index);
      final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
      final String localFileName = fileName("crash" + index);
      try {
        interceptedOperations.performTestOperations(crashAt, localFileName);
      } catch (Exception ioe) {
        recordsFile.close();
        try (FileRecordStore possiblyCorruptedFile = new FileRecordStore.Builder().path(Paths.get(localFileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open()) {
          int count = possiblyCorruptedFile.getNumRecords();
          for (var k : possiblyCorruptedFile.keys()) {
            // readRecordData has a CRC32 check where the payload must match the header
            possiblyCorruptedFile.readRecordData(k);
            count--;
          }
          assertEquals(0, count);
        } catch (Exception e) {
          FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
          final String msg = String.format("corrupted file due to exception at write index %s with stack %s", index, stackToString(stack));
          throw new RuntimeException(msg, e);
        }
      }
    }

  }

  private String fileName(String base) {
    String fileName = TMP + FileSystems.getDefault().getSeparator() + base;
    File file = new File(fileName);
    file.deleteOnExit();
    return fileName;
  }

  private String stackToString(List<String> stack) {
    StringBuilder sb = new StringBuilder();
    for (String s : stack) {
      sb.append("\\n\\t");
      sb.append(s);
    }
    return sb.toString();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }
      final var key2 = ByteSequence.of("key2".getBytes());
      final var data2 = (dataPadding + "data2").getBytes();
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, data2); }

      // then
      final var put1 = recordsFile.readRecordData(key1);
      final var put2 = recordsFile.readRecordData(key2);

      Assert.assertArrayEquals(put1, data1);
      Assert.assertArrayEquals(put2, data2);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }
      recordsFile.deleteRecord(key1);

      // then
      if (recordsFile.recordExists(key1)) {
        throw new Exception("Record not deleted");
      }

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      final var key2 = ByteSequence.of("key2".getBytes());
      final var data2 = (dataPadding + "data2").getBytes();
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, data2); }

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

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  @Test
  public void testInsertTwoDeleteFirstInsertOneWithIOExceptions() throws Exception {
    insertTwoDeleteFirstInsertOneWithIOExceptions("");
    insertTwoDeleteFirstInsertOneWithIOExceptions(string1k);
  }

  public void insertTwoDeleteFirstInsertOneWithIOExceptions(String dataPadding) throws Exception {
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

      // when
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      final var key2 = ByteSequence.of("key2".getBytes());
      final var data2 = (dataPadding + "data2").getBytes();
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, data2); }

      recordsFile.deleteRecord(key1);

      final var key3 = ByteSequence.of("key3".getBytes());
      final var data3 = (dataPadding + "data3").getBytes();
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, data3); }

      // then
      if (recordsFile.recordExists(key1)) {
        throw new Exception("Record not deleted");
      }
      final var put2 = recordsFile.readRecordData(key2);
      final var put3 = recordsFile.readRecordData(key3);

      Assert.assertArrayEquals(put2, data2);
      Assert.assertArrayEquals(put3, data3);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      final var key2 = ByteSequence.of("key2".getBytes());
      final var data2 = (dataPadding + "data2").getBytes();
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, data2); }

      recordsFile.deleteRecord(key1);

      final var key3 = ByteSequence.of("key3".getBytes());
      final var data3 = (dataPadding + "data3").getBytes();
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, data3); }

      // then
      if (recordsFile.recordExists(key1)) {
        throw new Exception("Record not deleted");
      }
      final var put2 = recordsFile.readRecordData(key2);
      final var put3 = recordsFile.readRecordData(key3);

      Assert.assertArrayEquals(put2, data2);
      Assert.assertArrayEquals(put3, data3);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  @Test
  public void testUpdateOneRecordWithIOExceptions() throws Exception {
    updateOneRecordWithIOExceptions("");
    updateOneRecordWithIOExceptions(string1k);
  }

  void updateOneRecordWithIOExceptions(String dataPadding) throws Exception {
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, initialSize, wc, false);

      // when
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      final var data2 = (dataPadding + "data2").getBytes();
      recordsFile.updateRecord(key1, data2);

      final var put1 = recordsFile.readRecordData(key1);

      Assert.assertArrayEquals(put1, data2);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      @SuppressWarnings("SpellCheckingInspection")
      final var data2 = (dataPadding + "data2xxxxxxxxxxxxxxxx").getBytes();
      recordsFile.updateRecord(key1, data2);

      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, data2);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
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
      final var key1 = ByteSequence.of("key1".getBytes());
      final var data1 = (dataPadding + "data1").getBytes();
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, data1); }

      final var key2 = ByteSequence.of("key2".getBytes());
      final var data2 = (dataPadding + "data2").getBytes();
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, data2); }

      final var data1large = (dataPadding + "data1" + dataPadding).getBytes();
      recordsFile.updateRecord(key1, data1large);

      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, data1large);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  @Test
  public void testUpdateExpandMiddleRecordWithIOExceptions() throws Exception {
    final var one = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var two = String.join("", Collections.nCopies(512, "1")).getBytes();
    final var three = String.join("", Collections.nCopies(256, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());
      final var key3 = ByteSequence.of("key3".getBytes());
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, one); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, one); } // 256
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, three); }

      //when
      recordsFile.updateRecord(key2, two); // 512

      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, one);
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, two);
      final var put3 = (recordsFile.readRecordData(key3));
      Assert.assertArrayEquals(put3, three);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  @Test
  public void testUpdateExpandOnlyRecord() throws Exception {
    final var one = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var two = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, one); }

      // when
      recordsFile.updateRecord(key1, two); // 512

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, two);

      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
    });
  }

  @Test
  public void indexOfFatRecordCausesHole() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();

    // given
    final var key1 = ByteSequence.of("key1".getBytes());
    final var key2 = ByteSequence.of("key2".getBytes());
    final var key3 = ByteSequence.of("key3".getBytes());

    // when
    if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }

    if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, narrow); }

    recordsFile.updateRecord(key2, wide);

    // then
    if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, narrow); }
    recordsFile.updateRecord(key2, narrow);

    recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).disablePayloadCrc32(false).open();
  }

  @Test
  public void testUpdateShrinkLastRecordWithIOExceptions() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }

      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, wide); }

      recordsFile.updateRecord(key2, narrow);

      // then
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, narrow);
    });
  }

  @Test
  public void testUpdateShrinkFirstRecordWithIOExceptions() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, wide); }

      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, narrow); }

      recordsFile.updateRecord(key1, narrow);

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, narrow);
    });
  }

  @Test
  public void testUpdateShrinkMiddleRecordWithIOExceptions() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());
      final var key3 = ByteSequence.of("key3".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, wide); }
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, narrow); }

      recordsFile.updateRecord(key2, narrow);

      // then
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, narrow);
    });
  }

  @Test
  public void testUpdateShrinkOnlyRecordWithIOExceptions() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, wide); }
      recordsFile.updateRecord(key1, narrow);

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, narrow);
    });
  }

  @Test
  public void testDeleteFirstEntries() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, wide); }
      recordsFile.deleteRecord(key1);

      // then
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, wide);
    });
  }

  @Test
  public void testDeleteLastEntry() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, wide); }
      recordsFile.deleteRecord(key2);

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, narrow);
    });
  }

  @Test
  public void testDeleteMiddleEntriesWithIOExceptions() throws Exception {
    final var narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());
      final var key3 = ByteSequence.of("key3".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, narrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, narrow); }
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, wide); }
      recordsFile.deleteRecord(key2);

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, narrow);
      final var put3 = recordsFile.readRecordData(key3);
      Assert.assertArrayEquals(put3, wide);
    });
  }

  @Test
  public void testDeleteOnlyEntryWithIOExceptions() throws Exception {
    final var wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, wide); }
      recordsFile.deleteRecord(key1);

      // then
      assertEquals(0, recordsFile.size());
      assertFalse(recordsFile.recordExists(key1));
    });
  }

  @Test
  public void testSplitFirstWithIOExceptions() throws Exception {
    final var oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    final var oneWide = String.join("", Collections.nCopies(1024, "1")).getBytes();
    final var twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, oneWide); }
      recordsFile.updateRecord(key1, oneNarrow);
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, twoNarrow); }

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, oneNarrow);
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, twoNarrow);
    });
  }

  @Test
  public void tesSplitLastWithIOExceptions() throws Exception {
    final var oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    final var twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
    final var twoWide = String.join("", Collections.nCopies(1024, "2")).getBytes();
    final var threeNarrow = String.join("", Collections.nCopies(38, "3")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());
      final var key3 = ByteSequence.of("key3".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, oneNarrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, twoWide); }
      recordsFile.updateRecord(key2, twoNarrow);
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, threeNarrow); }

      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, oneNarrow);
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, twoNarrow);
      final var put3 = recordsFile.readRecordData(key3);
      Assert.assertArrayEquals(put3, threeNarrow);
    });
  }

  @Test
  public void testSplitMiddleWithIOExceptions() throws Exception {
    final var oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    final var twoWide = String.join("", Collections.nCopies(1024, "2")).getBytes();
    final var twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
    final var threeNarrow = String.join("", Collections.nCopies(38, "3")).getBytes();
    final var fourNarrow = String.join("", Collections.nCopies(38, "4")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      final var key1 = ByteSequence.of("key1".getBytes());
      final var key2 = ByteSequence.of("key2".getBytes());
      final var key3 = ByteSequence.of("key3".getBytes());
      final var key4 = ByteSequence.of("key4".getBytes());

      // when
      if (!recordsFile.recordExists(key1)) { recordsFile.insertRecord(key1, oneNarrow); }
      if (!recordsFile.recordExists(key2)) { recordsFile.insertRecord(key2, twoWide); }
      if (!recordsFile.recordExists(key3)) { recordsFile.insertRecord(key3, threeNarrow); }
      recordsFile.updateRecord(key2, twoNarrow);
      if (!recordsFile.recordExists(key4)) { recordsFile.insertRecord(key4, fourNarrow); }


      // then
      final var put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, oneNarrow);
      final var put2 = recordsFile.readRecordData(key2);
      Assert.assertArrayEquals(put2, twoNarrow);
      final var put3 = recordsFile.readRecordData(key3);
      Assert.assertArrayEquals(put3, threeNarrow);
      final var put4 = recordsFile.readRecordData(key4);
      Assert.assertArrayEquals(put4, fourNarrow);
    });
  }

  @Test
  public void testFreeSpaceInIndexWithIOExceptions() throws Exception {
    final var oneLarge = String.join("", Collections.nCopies(1024, "1")).getBytes();
    final var twoSmall = String.join("", Collections.nCopies(38, "2")).getBytes();
    final var threeSmall = String.join("", Collections.nCopies(38, "3")).getBytes();

    verifyWorkWithIOExceptions((wc, fileName) -> {
      // set initial size equal to 2x header and 2x padded payload
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName,
          4 * FileRecordStore.DEFAULT_MAX_KEY_LENGTH, wc, false);

      // when
      recordsFile.insertRecord(fromUtf8("one"), oneLarge);
      recordsFile.insertRecord(fromUtf8("two"), twoSmall);
      final var maxLen = recordsFile.getFileLength();
      recordsFile.deleteRecord(fromUtf8("one"));
      recordsFile.insertRecord(fromUtf8("three"), threeSmall);

      final var finalLen = recordsFile.getFileLength();

      // then
      assertEquals(2, recordsFile.size());
      Assert.assertEquals(maxLen, finalLen);

    });
  }

  @Test
  public void testFreeSpaceInMiddleWithIOExceptions() throws Exception {
    final var one = String.join("", Collections.nCopies(38, "1")).getBytes();
    final var twoLarge = String.join("", Collections.nCopies(1024, "2")).getBytes();
    final var three = String.join("", Collections.nCopies(38, "3")).getBytes();
    final var four = String.join("", Collections.nCopies(38, "4")).getBytes();

    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // when
      recordsFile.insertRecord(fromUtf8("one"), (one));

      recordsFile.insertRecord(fromUtf8("two"), (twoLarge));

      recordsFile.insertRecord(fromUtf8("three"), (three));

      final var maxLen = recordsFile.getFileLength();
      recordsFile.deleteRecord(fromUtf8("two"));

      recordsFile.insertRecord(fromUtf8("four"), (four));

      final var finalLen = recordsFile.getFileLength();

      // then
      assertEquals(3, recordsFile.size());
      Assert.assertEquals(maxLen, finalLen);
    });
  }

  @Test
  public void testMaxKeySize() throws IOException {
    try {
      System.setProperty(String.format("%s.%s",
              FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
          Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());

      // given
      recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();

      // when
      final var longestKey = String.join("", Collections.nCopies(recordsFile.maxKeyLength, "1")).getBytes();
      recordsFile.insertRecord(ByteSequence.of(longestKey), longestKey);

      // then
      final var put0 = recordsFile.readRecordData(ByteSequence.of(longestKey));

      Assert.assertArrayEquals(put0, longestKey);
    } finally {
      System.setProperty(String.format("%s.%s",
              FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
          Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
    }
  }

  @Test
  public void testCrc32asUnsignedInteger() {
    final String data = String.join("", Collections.nCopies(1024, "2"));
    final var crc32 = new CRC32();
    crc32.update(data.getBytes(), 0, 1024);
    long crcLong = crc32.getValue();
    int crcInt = (int) crcLong;
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

    final var longestKey = String.join("", Collections.nCopies(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL - 5, "1")).getBytes();

    try (FileRecordStore recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open()) {
      recordsFile.insertRecord(ByteSequence.of(longestKey), longestKey);
    }

    // reset to the normal default
    System.setProperty(String.format("%s.%s",
            FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
        Integer.valueOf(FileRecordStore.DEFAULT_MAX_KEY_LENGTH).toString());
    recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).accessMode(FileRecordStore.Builder.AccessMode.READ_ONLY).open();

    final var put0 = recordsFile.readRecordData(ByteSequence.of(longestKey));

    Assert.assertArrayEquals(put0, longestKey);

  }

  @Test
  public void testByteStringAsMapKey() {
    HashMap<ByteSequence, byte[]> kvs = new HashMap<>();
    IntStream.range(Byte.MIN_VALUE, Byte.MAX_VALUE).forEach(b -> {
      byte[] key = {(byte) b};
      byte[] value = {(byte) b};
      kvs.put(ByteSequence.of(key), value);
    });
    Assert.assertEquals(255, kvs.size());
    byte[] empty = {};
    kvs.put(ByteSequence.of(empty), bytes(1));
    Assert.assertArrayEquals(bytes(1), kvs.get(ByteSequence.of(empty)));
  }

  @SuppressWarnings("SameParameterValue")
  byte[] bytes(int b) {
    return new byte[]{(byte) b};
  }

  @Test
  public void testCanCallCloseTwice() throws IOException {
    FileRecordStore recordsFile = new FileRecordStore.Builder().path(Paths.get(fileName)).preallocatedRecords(initialSize).open();
    recordsFile.close();
    recordsFile.close();
  }

  interface InterceptedTestOperations {
    void performTestOperations(WriteCallback wc,
                               String fileName) throws Exception;
  }

  /// A utility to record how many times file write operations are called
  /// and what the stack looks like for them.
  private record StackCollectingWriteCallback(List<List<String>> writeStacks) implements
      WriteCallback {

    @Override
      public void onWrite() {
        List<String> stack = new ArrayList<>();
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

  /// A utility to throw an exception at a particular write operation.
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

  public static class ByteUtils {
    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    static byte[] longToBytes(long x) {
      buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(0, x);
      return buffer.array();
    }

    static long bytesToLong(byte[] bytes) {
      buffer.put(bytes, 0, bytes.length);
      buffer.flip();//need flip
      return buffer.getLong();
    }

  }
}
