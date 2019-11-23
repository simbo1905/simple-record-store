package com.github.trex_paxos.srs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RecordsFileSimulatesDiskFailures extends FileRecordStore {

	public RecordsFileSimulatesDiskFailures(String dbPath, int initialSize, WriteCallback wc, boolean disableCrc32)
			throws IOException {
		super(dbPath, initialSize, getMaxKeyLengthOrDefault(), disableCrc32);
		File f = new File(dbPath);
		this.file = new InterceptedRandomAccessFile(new RandomAccessFile(f, "rw"),wc);
	}

}
