package com.github.trex_paxos.srs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;

public class RecordsFileSimulatesDiskFailures extends FileRecordStore {

	public RecordsFileSimulatesDiskFailures(String dbPath, int initialSize, WriteCallback wc, boolean disableCrc32)
			throws IOException {
		super(new File(dbPath), initialSize, getMaxKeyLengthOrDefault(), disableCrc32, false, "rw");
		this.fileOperations = new InterceptedRandomAccessFile(new RandomAccessFile(new File(dbPath), "rw"),wc);
	}

}
