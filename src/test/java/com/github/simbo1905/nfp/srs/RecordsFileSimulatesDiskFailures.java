package com.github.simbo1905.nfp.srs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RecordsFileSimulatesDiskFailures extends FileRecordStore {

  public RecordsFileSimulatesDiskFailures(
      String dbPath, int initialSize, WriteCallback wc, boolean disableCrc32) throws IOException {
    super(new File(dbPath), initialSize, getMaxKeyLengthOrDefault(), disableCrc32, false, "rw");
    this.fileOperations =
        new InterceptedRandomAccessFile(new RandomAccessFile(new File(dbPath), "rw"), wc);
  }
}
