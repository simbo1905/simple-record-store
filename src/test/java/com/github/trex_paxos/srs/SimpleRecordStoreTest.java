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
  private final CrashValidationTracker crashValidator = new CrashValidationTracker();

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
    val recordStore = new FileRecordStore(fileName, 100, 64, false);
    final String value = String.join("", Collections.nCopies(100, "x"));
    IntStream.range(0, 4).forEach(i -> {
      final String key = String.join("", Collections.nCopies(64, "" + i));
      try {
        recordStore.insertRecord(ByteSequence.stringToUtf8(key), value.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }

  @Test
  public void testDoubleInsertIntoLargeFile() throws Exception {
    val recordStore = new FileRecordStore(fileName, 1000, 64, false);
    final String value = String.join("", Collections.nCopies(100, "x"));
    val key1 = ByteSequence.stringToUtf8(String.join("", Collections.nCopies(4, "1")));
    val key2 = ByteSequence.stringToUtf8(String.join("", Collections.nCopies(4, "2")));
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
      recordsFile = openTrackedStore(fileName, wc);

      val key = ByteSequence.of("key".getBytes());
      val data = (dataPadding + "data").getBytes();
      insertRecordWithTracking(key, data);

      // then
      val put0 = recordsFile.readRecordData(key);
      Assert.assertArrayEquals(put0, data);

      recordsFile = new FileRecordStore(fileName, "r", false);
    });
  }

  private RecordsFileSimulatesDiskFailures openTrackedStore(String fileName, WriteCallback callback) throws IOException {
    return openTrackedStore(fileName, this.initialSize, callback);
  }

  private RecordsFileSimulatesDiskFailures openTrackedStore(String fileName,
                                                            int initialSizeOverride,
                                                            WriteCallback callback) throws IOException {
    try {
      RecordsFileSimulatesDiskFailures store = new RecordsFileSimulatesDiskFailures(fileName, initialSizeOverride, callback, false);
      crashValidator.bootstrap(store);
      return store;
    } catch (Exception e) {
      crashValidator.flagInitializationFailure(e);
      throw e;
    }
  }

  private void insertRecordWithTracking(ByteSequence key, byte[] data) throws IOException {
    crashValidator.beginOperation(CrashValidationTracker.OperationType.INSERT, key, data, recordsFile);
    try {
      recordsFile.insertRecord(key, data);
      crashValidator.operationCommitted(recordsFile);
    } catch (IOException | RuntimeException e) {
      crashValidator.operationFailed(e);
      throw e;
    }
  }

  private void updateRecordWithTracking(ByteSequence key, byte[] data) throws IOException {
    crashValidator.beginOperation(CrashValidationTracker.OperationType.UPDATE, key, data, recordsFile);
    try {
      recordsFile.updateRecord(key, data);
      crashValidator.operationCommitted(recordsFile);
    } catch (IOException | RuntimeException e) {
      crashValidator.operationFailed(e);
      throw e;
    }
  }

  private void deleteRecordWithTracking(ByteSequence key) throws IOException {
    crashValidator.beginOperation(CrashValidationTracker.OperationType.DELETE, key, null, recordsFile);
    try {
      recordsFile.deleteRecord(key);
      crashValidator.operationCommitted(recordsFile);
    } catch (IOException | RuntimeException e) {
      crashValidator.operationFailed(e);
      throw e;
    }
  }

  void verifyWorkWithIOExceptions(InterceptedTestOperations interceptedOperations) throws Exception {
    final List<List<String>> writeStacks = new ArrayList<>();

    WriteCallback collectsWriteStacks = new StackCollectingWriteCallback(writeStacks);

    final List<String> localFileNames = new ArrayList<>();
    final String recordingFile = fileName("record");
    localFileNames.add(recordingFile);

    crashValidator.reset();
    interceptedOperations.performTestOperations(collectsWriteStacks, recordingFile);
    closeQuietly();
    crashValidator.reset();

    try {
      for (int index = 0; index < writeStacks.size(); index++) {
        final List<String> stack = writeStacks.get(index);
        crashValidator.prepareForCrashScenario(index, stack);
        final CrashAtWriteCallback crashAt = new CrashAtWriteCallback(index);
        final String localFileName = fileName("crash" + index);
        localFileNames.add(localFileName);
        try {
          interceptedOperations.performTestOperations(crashAt, localFileName);
        } catch (Exception ioe) {
          closeQuietly();
          try (FileRecordStore possiblyCorruptedFile = new FileRecordStore(localFileName, "r", false)) {
            crashValidator.assertPostCrashState(possiblyCorruptedFile);
          } catch (AssertionError ae) {
            FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
            throw ae;
          } catch (Exception e) {
            FileRecordStore.dumpFile(Level.SEVERE, localFileName, true);
            throw crashValidator.wrapAsCorruption(e);
          }
        } finally {
          closeQuietly();
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

  private void closeQuietly() {
    if (recordsFile != null) {
      try {
        recordsFile.close();
      } catch (Exception ignored) {
        // ignore close failures during crash replay assertions
      } finally {
        recordsFile = null;
      }
    }
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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);
      val key2 = ByteSequence.of("key2".getBytes());
      val data2 = (dataPadding + "data2").getBytes();
      insertRecordWithTracking(key2, data2);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);
      deleteRecordWithTracking(key1);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      val key2 = ByteSequence.of("key2".getBytes());
      val data2 = (dataPadding + "data2").getBytes();
      insertRecordWithTracking(key2, data2);

      deleteRecordWithTracking(key1);
      deleteRecordWithTracking(key2);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      val key2 = ByteSequence.of("key2".getBytes());
      val data2 = (dataPadding + "data2").getBytes();
      insertRecordWithTracking(key2, data2);

      deleteRecordWithTracking(key1);

      val key3 = ByteSequence.of("key3".getBytes());
      val data3 = (dataPadding + "data3").getBytes();
      insertRecordWithTracking(key3, data3);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      val key2 = ByteSequence.of("key2".getBytes());
      val data2 = (dataPadding + "data2").getBytes();
      insertRecordWithTracking(key2, data2);

      deleteRecordWithTracking(key1);

      val key3 = ByteSequence.of("key3".getBytes());
      val data3 = (dataPadding + "data3").getBytes();
      insertRecordWithTracking(key3, data3);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      val data2 = (dataPadding + "data2").getBytes();
      updateRecordWithTracking(key1, data2);

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
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      @SuppressWarnings("SpellCheckingInspection")
      val data2 = (dataPadding + "data2xxxxxxxxxxxxxxxx").getBytes();
      updateRecordWithTracking(key1, data2);

      val put1 = recordsFile.readRecordData(key1);
      Assert.assertArrayEquals(put1, data2);

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
//                recordsFile = openTrackedStore(fileName, wc);
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
//                recordsFile = openTrackedStore(fileName, wc);
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
  public void testUpdateExpandFirstRecord() throws Exception {
    updateExpandFirstRecord("");
    updateExpandFirstRecord(string1k);
  }

  void updateExpandFirstRecord(String dataPadding) throws Exception {
    verifyWorkWithIOExceptions((wc, fileName) -> {
      recordsFile = openTrackedStore(fileName, wc);

      // when
      val key1 = ByteSequence.of("key1".getBytes());
      val data1 = (dataPadding + "data1").getBytes();
      insertRecordWithTracking(key1, data1);

      val key2 = ByteSequence.of("key2".getBytes());
      val data2 = (dataPadding + "data2").getBytes();
      insertRecordWithTracking(key2, data2);

      val data1large = (dataPadding + "data1" + dataPadding).getBytes();
      updateRecordWithTracking(key1, data1large);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());
      val key3 = ByteSequence.of("key3".getBytes());
      insertRecordWithTracking(key1, one);
      insertRecordWithTracking(key2, one); // 256
      insertRecordWithTracking(key3, three);

      //when
      updateRecordWithTracking(key2, two); // 512

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      insertRecordWithTracking(key1, one);

      // when
      updateRecordWithTracking(key1, two); // 512

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());

      // when
      insertRecordWithTracking(key1, narrow);

      insertRecordWithTracking(key2, wide);

      updateRecordWithTracking(key2, narrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());

      // when
      insertRecordWithTracking(key1, wide);

      insertRecordWithTracking(key2, narrow);

      updateRecordWithTracking(key1, narrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());
      val key3 = ByteSequence.of("key3".getBytes());

      // when
      insertRecordWithTracking(key1, narrow);
      insertRecordWithTracking(key2, wide);
      insertRecordWithTracking(key3, narrow);

      updateRecordWithTracking(key2, narrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());

      // when
      insertRecordWithTracking(key1, wide);
      updateRecordWithTracking(key1, narrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());

      // when
      insertRecordWithTracking(key1, narrow);
      insertRecordWithTracking(key2, wide);
      deleteRecordWithTracking(key1);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());

      // when
      insertRecordWithTracking(key1, narrow);
      insertRecordWithTracking(key2, wide);
      deleteRecordWithTracking(key2);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());
      val key3 = ByteSequence.of("key3".getBytes());

      // when
      insertRecordWithTracking(key1, narrow);
      insertRecordWithTracking(key2, narrow);
      insertRecordWithTracking(key3, wide);
      deleteRecordWithTracking(key2);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());

      // when
      insertRecordWithTracking(key1, wide);
      deleteRecordWithTracking(key1);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());

      // when
      insertRecordWithTracking(key1, oneWide);
      updateRecordWithTracking(key1, oneNarrow);
      insertRecordWithTracking(key2, twoNarrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());
      val key3 = ByteSequence.of("key3".getBytes());

      // when
      insertRecordWithTracking(key1, oneNarrow);
      insertRecordWithTracking(key2, twoWide);
      updateRecordWithTracking(key2, twoNarrow);
      insertRecordWithTracking(key3, threeNarrow);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // given
      val key1 = ByteSequence.of("key1".getBytes());
      val key2 = ByteSequence.of("key2".getBytes());
      val key3 = ByteSequence.of("key3".getBytes());
      val key4 = ByteSequence.of("key4".getBytes());

      // when
      insertRecordWithTracking(key1, oneNarrow);
      insertRecordWithTracking(key2, twoWide);
      insertRecordWithTracking(key3, threeNarrow);
      updateRecordWithTracking(key2, twoNarrow);
      insertRecordWithTracking(key4, fourNarrow);


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
      recordsFile = openTrackedStore(fileName, 4 * FileRecordStore.DEFAULT_MAX_KEY_LENGTH, wc);

      // when
      insertRecordWithTracking(ByteSequence.stringToUtf8("one"), oneLarge);
      insertRecordWithTracking(ByteSequence.stringToUtf8("two"), twoSmall);
      val maxLen = recordsFile.getFileLength();
      deleteRecordWithTracking(ByteSequence.stringToUtf8("one"));
      insertRecordWithTracking(ByteSequence.stringToUtf8("three"), threeSmall);

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
      recordsFile = openTrackedStore(fileName, 2, wc);

      // when
      insertRecordWithTracking(ByteSequence.stringToUtf8("one"), (one));

      insertRecordWithTracking(ByteSequence.stringToUtf8("two"), (twoLarge));

      insertRecordWithTracking(ByteSequence.stringToUtf8("three"), (three));

      val maxLen = recordsFile.getFileLength();
      deleteRecordWithTracking(ByteSequence.stringToUtf8("two"));

      insertRecordWithTracking(ByteSequence.stringToUtf8("four"), (four));

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

  interface InterceptedTestOperations {
    void performTestOperations(WriteCallback wc,
                               String fileName) throws Exception;
  }

  private static final class CrashValidationTracker {
    private LinkedHashMap<ByteSequence, byte[]> expectedData = new LinkedHashMap<>();
    private List<FileRecordStore.RecordSnapshot> expectedSnapshots = new ArrayList<>();
    private long expectedFileLength = 0L;

    private LinkedHashMap<ByteSequence, byte[]> snapshotBeforeOperation = new LinkedHashMap<>();
    private List<FileRecordStore.RecordSnapshot> snapshotsBeforeOperation = new ArrayList<>();
    private long fileLengthBeforeOperation = 0L;

    private OperationContext currentOperation;
    private FailureContext lastFailure;
    private int currentCrashIndex = -1;
    private List<String> currentStack = Collections.emptyList();

    enum OperationType {INSERT, UPDATE, DELETE, INITIALIZATION}

    private record OperationContext(OperationType type, ByteSequence key, byte[] data) {
    }

    private record FailureContext(OperationContext operation, Exception cause) {
    }

    private record StoreSnapshot(LinkedHashMap<ByteSequence, byte[]> data,
                                 List<FileRecordStore.RecordSnapshot> recordSnapshots,
                                 long fileLength) {
    }

    void reset() {
      expectedData = new LinkedHashMap<>();
      expectedSnapshots = new ArrayList<>();
      expectedFileLength = 0L;
      snapshotBeforeOperation = new LinkedHashMap<>();
      snapshotsBeforeOperation = new ArrayList<>();
      fileLengthBeforeOperation = 0L;
      currentOperation = null;
      lastFailure = null;
      currentCrashIndex = -1;
      currentStack = Collections.emptyList();
    }

    void prepareForCrashScenario(int crashIndex, List<String> stack) {
      reset();
      currentCrashIndex = crashIndex;
      currentStack = new ArrayList<>(stack);
    }

    void bootstrap(FileRecordStore store) throws IOException {
      StoreSnapshot snapshot = collectSnapshot(store);
      expectedData = snapshot.data();
      expectedSnapshots = snapshot.recordSnapshots();
      expectedFileLength = snapshot.fileLength();
    }

    void beginOperation(OperationType type, ByteSequence key, byte[] data, FileRecordStore store) throws IOException {
      Objects.requireNonNull(store, "recordsFile must not be null");
      if (expectedSnapshots.isEmpty() && expectedData.isEmpty() && expectedFileLength == 0L) {
        bootstrap(store);
      }
      snapshotBeforeOperation = deepCopyData(expectedData);
      snapshotsBeforeOperation = deepCopyRecordSnapshots(expectedSnapshots);
      fileLengthBeforeOperation = expectedFileLength;
      currentOperation = new OperationContext(type, key.copy(), data == null ? null : Arrays.copyOf(data, data.length));
    }

    void operationCommitted(FileRecordStore store) throws IOException {
      if (currentOperation == null) {
        return;
      }
      StoreSnapshot snapshot = collectSnapshot(store);
      expectedData = snapshot.data();
      expectedSnapshots = snapshot.recordSnapshots();
      expectedFileLength = snapshot.fileLength();
      currentOperation = null;
      snapshotBeforeOperation = new LinkedHashMap<>();
      snapshotsBeforeOperation = new ArrayList<>();
      lastFailure = null;
    }

    void operationFailed(Exception cause) {
      if (currentOperation == null) {
        return;
      }
      expectedData = deepCopyData(snapshotBeforeOperation);
      expectedSnapshots = deepCopyRecordSnapshots(snapshotsBeforeOperation);
      expectedFileLength = fileLengthBeforeOperation;
      lastFailure = new FailureContext(currentOperation, cause);
      currentOperation = null;
      snapshotBeforeOperation = new LinkedHashMap<>();
      snapshotsBeforeOperation = new ArrayList<>();
    }

    void flagInitializationFailure(Exception cause) {
      expectedData = new LinkedHashMap<>();
      expectedSnapshots = new ArrayList<>();
      expectedFileLength = -1L;
      snapshotBeforeOperation = new LinkedHashMap<>();
      snapshotsBeforeOperation = new ArrayList<>();
      fileLengthBeforeOperation = 0L;
      currentOperation = null;
      lastFailure = new FailureContext(
          new OperationContext(OperationType.INITIALIZATION, ByteSequence.copyOf(new byte[0]), null),
          cause);
    }

    void assertPostCrashState(FileRecordStore store) throws IOException {
      if (lastFailure == null) {
        throw failure("missing failure context to validate crash state");
      }
      StoreSnapshot actual = collectSnapshot(store);
      assertDataMatches(actual.data());
      assertRecordMetadata(actual.recordSnapshots(), store);
      assertFileLength(actual.fileLength());
    }

    RuntimeException wrapAsCorruption(Exception cause) {
      return new RuntimeException(describeScenario("corrupted file"), cause);
    }

    private void assertFileLength(long actualFileLength) {
      if (expectedFileLength >= 0 && actualFileLength < expectedFileLength) {
        throw failure(String.format("file length shrank expected at least %d actual %d", expectedFileLength, actualFileLength));
      }
    }

    private void assertDataMatches(LinkedHashMap<ByteSequence, byte[]> actualData) {
      if (actualData.size() != expectedData.size()) {
        throw failure(String.format("record count mismatch expected %d actual %d", expectedData.size(), actualData.size()));
      }
      for (Map.Entry<ByteSequence, byte[]> entry : expectedData.entrySet()) {
        byte[] actual = actualData.get(entry.getKey());
        if (actual == null) {
          throw failure(String.format("missing key %s", describeKey(entry.getKey())));
        }
        if (!Arrays.equals(entry.getValue(), actual)) {
          throw failure(String.format("data mismatch for key %s", describeKey(entry.getKey())));
        }
      }
      if (!actualData.keySet().equals(expectedData.keySet())) {
        Set<ByteSequence> extra = new HashSet<>(actualData.keySet());
        extra.removeAll(expectedData.keySet());
        throw failure(String.format("unexpected keys %s", extra));
      }
    }

    private void assertRecordMetadata(List<FileRecordStore.RecordSnapshot> actualSnapshots,
                                      FileRecordStore store) throws IOException {
      if (store.getNumRecords() != expectedData.size()) {
        throw failure(String.format("store numRecords mismatch expected %d actual %d", expectedData.size(), store.getNumRecords()));
      }
      if (actualSnapshots.size() != expectedSnapshots.size()) {
        throw failure(String.format("snapshot count mismatch expected %d actual %d", expectedSnapshots.size(), actualSnapshots.size()));
      }
      for (int i = 0; i < expectedSnapshots.size(); i++) {
        FileRecordStore.RecordSnapshot expected = expectedSnapshots.get(i);
        FileRecordStore.RecordSnapshot actual = actualSnapshots.get(i);
        if (expected.indexPosition() != actual.indexPosition()
            || expected.dataPointer() != actual.dataPointer()
            || expected.dataCapacity() != actual.dataCapacity()
            || expected.dataCount() != actual.dataCount()
            || expected.headerCrc32() != actual.headerCrc32()
            || !expected.key().equals(actual.key())) {
          throw failure(String.format("index mismatch at %d expected %s actual %s", i, expected, actual));
        }
        byte[] expectedValue = expectedData.get(actual.key());
        if (expectedValue == null) {
          throw failure(String.format("missing expected data for key %s", describeKey(actual.key())));
        }
        if (actual.dataCount() != expectedValue.length) {
          throw failure(String.format("payload length mismatch for key %s expected %d actual %d",
              describeKey(actual.key()), expectedValue.length, actual.dataCount()));
        }
        if (actual.dataCount() > actual.dataCapacity()) {
          throw failure(String.format("dataCount > dataCapacity for key %s", describeKey(actual.key())));
        }
      }
      ensureNoOverlap(actualSnapshots, store.getFileLength());
    }

    private void ensureNoOverlap(List<FileRecordStore.RecordSnapshot> snapshots, long fileLength) {
      List<FileRecordStore.RecordSnapshot> sorted = new ArrayList<>(snapshots);
      sorted.sort(Comparator.comparingLong(FileRecordStore.RecordSnapshot::dataPointer));
      long previousEnd = 0L;
      for (FileRecordStore.RecordSnapshot snapshot : sorted) {
        if (snapshot.dataPointer() < 0) {
          throw failure(String.format("negative data pointer %d for key %s", snapshot.dataPointer(), describeKey(snapshot.key())));
        }
        long recordEnd = snapshot.dataPointer() + snapshot.dataCapacity();
        if (recordEnd > fileLength) {
          throw failure(String.format("record for key %s extends beyond file length %d", describeKey(snapshot.key()), fileLength));
        }
        if (snapshot.dataPointer() < previousEnd) {
          throw failure(String.format("records overlap near key %s (pointer %d < %d)", describeKey(snapshot.key()), snapshot.dataPointer(), previousEnd));
        }
        previousEnd = Math.max(previousEnd, recordEnd);
      }
    }

    private StoreSnapshot collectSnapshot(FileRecordStore store) throws IOException {
      List<FileRecordStore.RecordSnapshot> rawSnapshots = store.snapshotRecords();
      List<FileRecordStore.RecordSnapshot> snapshots = new ArrayList<>(rawSnapshots.size());
      LinkedHashMap<ByteSequence, byte[]> data = new LinkedHashMap<>();
      for (FileRecordStore.RecordSnapshot snapshot : rawSnapshots) {
        ByteSequence keyCopy = snapshot.key().copy();
        byte[] value = store.readRecordData(snapshot.key());
        data.put(keyCopy, Arrays.copyOf(value, value.length));
        snapshots.add(new FileRecordStore.RecordSnapshot(
            snapshot.indexPosition(),
            keyCopy,
            snapshot.dataPointer(),
            snapshot.dataCapacity(),
            snapshot.dataCount(),
            snapshot.headerCrc32()));
      }
      return new StoreSnapshot(data, snapshots, store.getFileLength());
    }

    private LinkedHashMap<ByteSequence, byte[]> deepCopyData(Map<ByteSequence, byte[]> source) {
      LinkedHashMap<ByteSequence, byte[]> copy = new LinkedHashMap<>(source.size());
      for (Map.Entry<ByteSequence, byte[]> entry : source.entrySet()) {
        copy.put(entry.getKey().copy(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
      }
      return copy;
    }

    private List<FileRecordStore.RecordSnapshot> deepCopyRecordSnapshots(List<FileRecordStore.RecordSnapshot> source) {
      List<FileRecordStore.RecordSnapshot> copy = new ArrayList<>(source.size());
      for (FileRecordStore.RecordSnapshot snapshot : source) {
        copy.add(new FileRecordStore.RecordSnapshot(
            snapshot.indexPosition(),
            snapshot.key().copy(),
            snapshot.dataPointer(),
            snapshot.dataCapacity(),
            snapshot.dataCount(),
            snapshot.headerCrc32()));
      }
      return copy;
    }

    private AssertionError failure(String detail) {
      return new AssertionError(describeScenario(detail));
    }

    private String describeScenario(String detail) {
      StringBuilder sb = new StringBuilder();
      sb.append("crash index ").append(currentCrashIndex);
      if (lastFailure != null) {
        sb.append(" during ").append(lastFailure.operation.type());
        sb.append(" key=").append(describeKey(lastFailure.operation.key()));
        if (lastFailure.operation.data() != null) {
          sb.append(" dataLen=").append(lastFailure.operation.data().length);
        }
        if (lastFailure.cause() != null) {
          sb.append(" cause=").append(lastFailure.cause().getClass().getSimpleName());
        }
      }
      if (!currentStack.isEmpty()) {
        sb.append(" firstStack=").append(currentStack.get(0));
      }
      sb.append(": ").append(detail);
      return sb.toString();
    }

    private String describeKey(ByteSequence key) {
      try {
        return ByteSequence.utf8ToString(key);
      } catch (Exception ignored) {
        return Arrays.toString(key.bytes());
      }
    }
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
