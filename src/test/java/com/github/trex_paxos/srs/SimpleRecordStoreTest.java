package com.github.trex_paxos.srs;

import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

import static com.github.trex_paxos.srs.FileRecordStore.MAX_KEY_LENGTH_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/// Tests that the simple random access storage 'db' works and does not get
/// corrupted under write errors.
public class SimpleRecordStoreTest extends JulLoggingConfig {

  static final String TMP = System.getProperty("java.io.tmpdir");
  private final static Logger logger = Logger.getLogger(SimpleRecordStoreTest.class.getName());

  static {
    val msg = String.format(">TMP:%s", TMP);
    logger.info(msg);
  }

  final String string1k = String.join("", Collections.nCopies(1024, "1"));
  private final Function<Date, byte[]> serializerDate = (date) -> ByteUtils.longToBytes(date.getTime());
  private final Function<byte[], Date> deserializerDate = (bytes) -> new Date(ByteUtils.bytesToLong(bytes));
  String fileName;
  int initialSize;
  FileRecordStore recordsFile = null;

  public SimpleRecordStoreTest() {
    logger.setLevel(Level.ALL);
    init(TMP + FileSystems.getDefault().getSeparator() + "junit.records", 0);
  }

  public void init(final String fileName, final int initialSize) {
    this.fileName = fileName;
    this.initialSize = initialSize;
    File db = new File(this.fileName);
    if (db.exists()) {
      //noinspection ResultOfMethodCallIgnored
      db.delete();
    }
    db.deleteOnExit();
  }

  @After
  public void deleteDb() {
    File db = new File(this.fileName);
    if (db.exists()) {
      if (!db.delete()) throw new IllegalStateException("could not delete " + db);
    }
  }

  /// Taken from <a href="http://www.javaworld.com/jw-01-1999/jw-01-step.html">original source</a>
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
    //noinspection UnusedAssignment
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
public void testMoveUpdatesPositionMap() throws Exception {
  final String value = String.join("", Collections.nCopies(100, "x"));
  try (val recordStore = new FileRecordStore(fileName, 100, 64, false)) {
    IntStream.range(0, 4).forEach(i -> {
      final String key = String.join("", Collections.nCopies(64, "" + i));
      try {
        recordStore.insertRecord(ByteSequence.stringToUtf8(key), value.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }
}

@Test
public void testDoubleInsertIntoLargeFile() throws Exception {
  final String value = String.join("", Collections.nCopies(100, "x"));
  val key1 = ByteSequence.stringToUtf8(String.join("", Collections.nCopies(4, "1")));
  val key2 = ByteSequence.stringToUtf8(String.join("", Collections.nCopies(4, "2")));

  try (val recordStore = new FileRecordStore(fileName, 1000, 64, false)) {
    recordStore.insertRecord(key1, value.getBytes());
    recordStore.insertRecord(key2, value.getBytes());
  }
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

  void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations) throws Exception {
    final List<List<String>> writeStacks = new ArrayList<>();

    WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

    final List<String> localFileNames = new ArrayList<>();
    final String recordingFile = fileName("record");
    localFileNames.add(recordingFile);

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
          try (FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false)) {
            // Use snapshotRecords() for proper structural validation
            validateFileStructure(possiblyCorruptedFile, index);
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

  private void removeFiles(List<String> localFileNames) {
    for (String file : localFileNames) {
      File f = new File(file);
      //noinspection ResultOfMethodCallIgnored
      f.delete();
    }
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

      @SuppressWarnings("SpellCheckingInspection")
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
    val one = String.join("", Collections.nCopies(256, "1")).getBytes();
    val two = String.join("", Collections.nCopies(512, "1")).getBytes();
    val three = String.join("", Collections.nCopies(256, "1")).getBytes();
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
    val one = String.join("", Collections.nCopies(256, "1")).getBytes();
    val two = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val narrow = String.join("", Collections.nCopies(256, "1")).getBytes();
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
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
    val wide = String.join("", Collections.nCopies(512, "1")).getBytes();
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = new RecordsFileSimulatesDiskFailures(fileName, 2, wc, false);

      // given
      val key1 = ByteSequence.of("key1".getBytes());

      // when
      recordsFile.insertRecord(key1, wide);
      recordsFile.deleteRecord(key1);

      // then
      assertEquals(0, recordsFile.size());
      assertFalse(recordsFile.recordExists(key1));
    });
  }

  @Test
  public void tesSplitFirstWithIOExceptions() throws Exception {
    val oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    val oneWide = String.join("", Collections.nCopies(1024, "1")).getBytes();
    val twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
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
    val oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    val twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
    val twoWide = String.join("", Collections.nCopies(1024, "2")).getBytes();
    val threeNarrow = String.join("", Collections.nCopies(38, "3")).getBytes();
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
    val oneNarrow = String.join("", Collections.nCopies(38, "1")).getBytes();
    val twoWide = String.join("", Collections.nCopies(1024, "2")).getBytes();
    val twoNarrow = String.join("", Collections.nCopies(38, "2")).getBytes();
    val threeNarrow = String.join("", Collections.nCopies(38, "3")).getBytes();
    val fourNarrow = String.join("", Collections.nCopies(38, "4")).getBytes();
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
    val oneLarge = String.join("", Collections.nCopies(1024, "1")).getBytes();
    val twoSmall = String.join("", Collections.nCopies(38, "2")).getBytes();
    val threeSmall = String.join("", Collections.nCopies(38, "3")).getBytes();

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
    val one = String.join("", Collections.nCopies(38, "1")).getBytes();
    val twoLarge = String.join("", Collections.nCopies(1024, "2")).getBytes();
    val three = String.join("", Collections.nCopies(38, "3")).getBytes();
    val four = String.join("", Collections.nCopies(38, "4")).getBytes();

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
    try {
      System.setProperty(String.format("%s.%s",
              FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY),
          Integer.valueOf(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL).toString());

      // given
      recordsFile = new FileRecordStore(fileName, initialSize);

      // when
      val longestKey = String.join("", Collections.nCopies(recordsFile.maxKeyLength, "1")).getBytes();
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
    final String data = String.join("", Collections.nCopies(1024, "2"));
    val crc32 = new CRC32();
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

    val longestKey = String.join("", Collections.nCopies(FileRecordStore.MAX_KEY_LENGTH_THEORETICAL - 5, "1")).getBytes();

    try (FileRecordStore recordsFile = new FileRecordStore(fileName, initialSize)) {
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
    FileRecordStore recordsFile = new FileRecordStore(fileName, initialSize);
    recordsFile.close();
    recordsFile.close();
  }

  /// Validates file structure after a simulated crash
  static void validateFileStructure(FileRecordStore store, int crashIndex) throws IOException {
    // 1. Get structural snapshot
    List<RecordSnapshot> actualStructure = store.snapshotRecords();

    // 2. Validate structural invariants
    validateStructuralInvariants(actualStructure, crashIndex, store.getFileLength());

    // 3. Validate data integrity - all records can be read with CRC32 validation
    validateDataIntegrity(store, actualStructure);
  }

  private static void validateStructuralInvariants(List<RecordSnapshot> structure, int crashIndex, long fileLength) {
    // Check index positions are unique (not necessarily sequential due to deletes)
    Set<Integer> indexPositions = new HashSet<>();
    for (RecordSnapshot snapshot : structure) {
      if (!indexPositions.add(snapshot.indexPosition())) {
        throw new RuntimeException("Duplicate index position: " + snapshot.indexPosition() + " at crash index " + crashIndex);
      }
    }

    // Verify all records are within file bounds
    structure.forEach(snapshot -> {
      long recordEnd = snapshot.dataPointer() + snapshot.dataCapacity();
      if (recordEnd > fileLength) {
        throw new RuntimeException("Record extends beyond file length: recordEnd=" + recordEnd + ", fileLength=" + fileLength);
      }
      if (snapshot.dataPointer() < 0) {
        throw new RuntimeException("Invalid negative data pointer: " + snapshot.dataPointer());
      }
    });

    // Check that actual data regions don't overlap (capacity regions may overlap after crashes)
    // This validates that the live data in memIndex doesn't have corrupted pointers
    validateNoOverlappingData(structure, crashIndex);
  }

  private static void validateNoOverlappingData(List<RecordSnapshot> structure, int crashIndex) {
    List<RecordSnapshot> byPosition = structure.stream()
            .sorted(Comparator.comparingLong(RecordSnapshot::dataPointer))
            .toList();

    IntStream.range(0, byPosition.size() - 1).forEach(i -> {
      RecordSnapshot current = byPosition.get(i);
      RecordSnapshot next = byPosition.get(i + 1);

      // Check if actual data overlaps (not capacity)
      // Data format: 4-byte length prefix + data + 8-byte CRC32 (if enabled)
      // We conservatively estimate overhead as 4 + 8 = 12 bytes
      long currentDataEnd = current.dataPointer() + current.dataCount() + 12;
      if (currentDataEnd > next.dataPointer()) {
        String msg = String.format("Overlapping data at crash index %d: record key=%s dataPointer=%d dataCount=%d ends at %d, next key=%s starts at %d",
                crashIndex, new String(current.key().bytes), current.dataPointer(), current.dataCount(), currentDataEnd,
                new String(next.key().bytes), next.dataPointer());
        throw new RuntimeException(msg);
      }
    });
  }

  private static void validateDataIntegrity(FileRecordStore store,
                                                   List<RecordSnapshot> structure) throws IOException {
    // For each record in the structure, verify it can be read
    // readRecordData has a CRC32 check where the payload must match the header
    for (RecordSnapshot snapshot : structure) {
      ByteSequence key = snapshot.key();
      store.readRecordData(key);
    }
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
