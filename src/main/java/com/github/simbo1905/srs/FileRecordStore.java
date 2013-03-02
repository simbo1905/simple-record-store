package com.github.simbo1905.srs;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileRecordStore extends BaseRecordStore {

	/**
	 * Hashtable which holds the in-memory index. For efficiency, the entire
	 * index is cached in memory. The hashtable maps a key of type String to a
	 * RecordHeader.
	 */
	protected Map<String, RecordHeader> memIndex;

	/**
	 * Creates a new database file. The initialSize parameter determines the
	 * amount of space which is allocated for the index. The index can grow
	 * dynamically, but the parameter is provide to increase efficiency.
	 */
	public FileRecordStore(String dbPath, int initialSize) throws IOException,
			RecordsFileException {
		super(dbPath, initialSize);
		memIndex = Collections
				.synchronizedMap(new HashMap<String, RecordHeader>(initialSize));
	}

	/**
	 * Opens an existing database and initializes the in-memory index.
	 */
	public FileRecordStore(String dbPath, String accessFlags) throws IOException,
			RecordsFileException {
		super(dbPath, accessFlags);
		int numRecords = readNumRecordsHeader();
		memIndex = Collections
				.synchronizedMap(new HashMap<String, RecordHeader>());
		for (int i = 0; i < numRecords; i++) {
			String key = readKeyFromIndex(i);
			RecordHeader header = readRecordHeaderFromIndex(i);
			header.setIndexPosition(i);
			memIndex.put(key, header);
		}
	}

	@Override
	public synchronized Iterable<String> keys() {
		return memIndex.keySet();
	}
	
	/**
	 * Returns the current number of records in the database.
	 */
	@Override
	public synchronized int getNumRecords() {
		return memIndex.size();
	}

	/**
	 * Checks if there is a record belonging to the given key.
	 */
	@Override
	public synchronized boolean recordExists(String key) {
		return memIndex.containsKey(key);
	}

	/**
	 * Maps a key to a record header by looking it up in the in-memory index.
	 */
	@Override
	protected RecordHeader keyToRecordHeader(String key)
			throws RecordsFileException {
		RecordHeader h = (RecordHeader) memIndex.get(key);
		if (h == null) {
			throw new RecordsFileException("Key not found: " + key);
		}
		return h;
	}

	/**
	 * This method searches the file for free space and then returns a
	 * RecordHeader which uses the space. (O(n) memory accesses)
	 */
	@Override
	protected RecordHeader allocateRecord(String key, int dataLength)
			throws RecordsFileException, IOException {
		// search for empty space
		RecordHeader newRecord = null;
		for (RecordHeader next : this.memIndex.values()) {
			int free = next.getFreeSpace();
			if (dataLength <= free) {
				newRecord = next.split();
				writeRecordHeaderToIndex(next);
				break;
			}
		}
		if (newRecord == null) {
			// append record to end of file - grows file to allocate space
			long fp = getFileLength();
			setFileLength(fp + dataLength);
			newRecord = new RecordHeader(fp, dataLength);
		}
		return newRecord;
	}

	/**
	 * Returns the record to which the target file pointer belongs - meaning the
	 * specified location in the file is part of the record data of the
	 * RecordHeader which is returned. Returns null if the location is not part
	 * of a record. (O(n) mem accesses)
	 */
	@Override
	protected RecordHeader getRecordAt(long targetFp)
			throws RecordsFileException {
		for( RecordHeader next : this.memIndex.values() ){
			if (targetFp >= next.dataPointer
					&& targetFp < next.dataPointer + (long) next.dataCapacity) {
				return next;
			}
		}
		return null;
	}

	/**
	 * Closes the database.
	 */
	@Override
	public synchronized void close() throws IOException, RecordsFileException {
		try {
			super.close();
		} finally {
			memIndex.clear();
			memIndex = null;
		}
	}

	/**
	 * Adds the new record to the in-memory index and calls the super class add
	 * the index entry to the file.
	 */
	@Override
	protected void addEntryToIndex(String key, RecordHeader newRecord,
			int currentNumRecords) throws IOException, RecordsFileException {
		super.addEntryToIndex(key, newRecord, currentNumRecords);
		memIndex.put(key, newRecord);
	}
	
	@Override
	protected void replaceEntryInIndex(String key, RecordHeader header,
			RecordHeader newRecord) {
		super.replaceEntryInIndex(key, header, newRecord);
		memIndex.put(key,newRecord);
	}

	/**
	 * Removes the record from the index. Replaces the target with the entry at
	 * the end of the index.
	 */
	@Override
	protected void deleteEntryFromIndex(String key, RecordHeader header,
			int currentNumRecords) throws IOException, RecordsFileException {
		super.deleteEntryFromIndex(key, header, currentNumRecords);
		RecordHeader deleted = (RecordHeader) memIndex.remove(key);
		assert header == deleted;
	}
	
	public static void main(String[] args) throws Exception {
		if( args.length < 1 ){
			err.println("not file passed");
		}
		final String filename = args[0];
		out.println("Reading from "+filename);
		final BaseRecordStore recordFile = new FileRecordStore(filename, "r");
		out.println(String.format("Records=%s, FileLength=%s, DataPointer=%s", recordFile.getNumRecords(), recordFile.getFileLength(), recordFile.dataStartPtr));
		for(int index = 0; index < recordFile.getNumRecords(); index++ ){
			final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
			final String key = recordFile.readKeyFromIndex(index);
			out.println(String.format("Key=%s, HeaderIndex=%s, HeaderCapacity=%s, HeaderActual=%s, HeaderPointer=%s", key, header.indexPosition, header.dataCapacity, header.dataCount, header.dataPointer));
			out.println(recordFile.readRecord(key).readObject());
		}
	}

}
