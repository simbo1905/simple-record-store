package com.github.simbo1905.srs;

import lombok.Synchronized;
import lombok.val;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.System.err;
import static java.lang.System.out;

public class FileRecordStore extends BaseRecordStore {

	/*
	 * Hashtable which holds the in-memory index. For efficiency, the entire
	 * index is cached in memory. The hashtable wraps the byte[] key as a String
	 * as you cannot use a raw byte[] as a key and Java doesn't have extension methods yet.
	 */
	protected Map<String, RecordHeader> memIndex;

	/*
	 * Creates a new database file. The initialSize parameter determines the
	 * amount of space which is allocated for the index. The index can grow
	 * dynamically, but the parameter is provide to increase efficiency.
	 */
	public FileRecordStore(String dbPath, int initialSize, boolean disableCrc32) throws IOException,
			RecordsFileException {
		super(dbPath, initialSize, disableCrc32);
		memIndex = Collections
				.synchronizedMap(new HashMap<String, RecordHeader>(initialSize));
	}

	/*
	 * Opens an existing database and initializes the in-memory index.
	 */
	public FileRecordStore(String dbPath, String accessFlags, boolean disableCrc32) throws IOException,
			RecordsFileException {
		super(dbPath, accessFlags, disableCrc32);
		int numRecords = readNumRecordsHeader();
		memIndex = Collections
				.synchronizedMap(new HashMap<String, RecordHeader>(numRecords));
		for (int i = 0; i < numRecords; i++) {
			byte[] key = readKeyFromIndex(i);
			val k = keyOf(key);
			RecordHeader header = readRecordHeaderFromIndex(i);
			header.setIndexPosition(i);
			memIndex.put(k, header);
		}
	}

	@Override
	@Synchronized
	public Iterable<String> keys() {
		return memIndex.keySet();
	}
	
	/*
	 * Returns the current number of records in the database.
	 */
	@Override
	@Synchronized
	public int getNumRecords() {
		return memIndex.size();
	}

	@Override
	@Synchronized
	public boolean isEmpty() {
		return memIndex.isEmpty();
	}

	/*
	 * Checks if there is a record belonging to the given key.
	 */
	@Override
	@Synchronized
	public boolean recordExists(byte[] key) {
		return memIndex.containsKey(keyOf(key));
	}

	/*
	 * Maps a key to a record header by looking it up in the in-memory index.
	 */
	@Override
	protected RecordHeader keyToRecordHeader(byte[] key)
			throws RecordsFileException {
		val k = keyOf(key);
		RecordHeader h = memIndex.get(k);
		if (h == null) {
			throw new RecordsFileException("Key not found: " + key);
		}
		return h;
	}

	Comparator<RecordHeader> compareRecordHeaderByFreeSpace = new Comparator<RecordHeader>() {
		@Override
		public int compare(RecordHeader o1, RecordHeader o2) {
			return (int)(o1.getFreeSpace() - o2.getFreeSpace());
		}
	};

	/*
	 * ConcurrentSkipListMap makes scanning by ascending values fast and is sorted by smallest free space first
	 */
	ConcurrentNavigableMap<RecordHeader,Integer> freeMap = new ConcurrentSkipListMap<>(compareRecordHeaderByFreeSpace);

	/*
	 * Updates a map of record headers to free space values.
	 *
	 * @param rh Record that has new free space.
	 */
	@Override
	protected void updateFreeSpaceIndex(RecordHeader rh) {
		int free = rh.getFreeSpace();
		if( free > 0 ){
			freeMap.put(rh,free);
		} else {
			freeMap.remove(rh);
		}
	}

	/*
	 * This method searches the file for free space and then returns a
	 * RecordHeader which uses the space. (O(n) memory accesses)
	 */
	@Override
	protected RecordHeader allocateRecord(byte[] key, int dataLength)
			throws IOException {
		// search for empty space
		RecordHeader newRecord = null;
		for (RecordHeader next : this.freeMap.keySet() ) {
			int free = next.getFreeSpace();
			if (dataLength <= free) {
				newRecord = next.split();
				updateFreeSpaceIndex(next);
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

	/*
	 * Returns the record to which the target file pointer belongs - meaning the
	 * specified location in the file is part of the record data of the
	 * RecordHeader which is returned. Returns null if the location is not part
	 * of a record. (O(n) mem accesses)
	 *
	 * ToDo speed this search up with an index into the map
	 */
	@Override
	protected RecordHeader getRecordAt(long targetFp)
			throws RecordsFileException {
		for( RecordHeader next : this.memIndex.values() ){
			if (targetFp >= next.dataPointer
					&& targetFp < next.dataPointer + (long) next.getDataCapacity()) {
				return next;
			}
		}
		return null;
	}

	/*
	 * Closes the database.
	 */
	@Override
	@Synchronized
	public void close() throws IOException, RecordsFileException {
		try {
			super.close();
		} finally {
			memIndex.clear();
			memIndex = null;
		}
	}

	/*
	 * Adds the new record to the in-memory index and calls the super class add
	 * the index entry to the file.
	 */
	@Override
	protected void addEntryToIndex(byte[] key, RecordHeader newRecord,
			int currentNumRecords) throws IOException, RecordsFileException {
		super.addEntryToIndex(key, newRecord, currentNumRecords);
		memIndex.put(keyOf(key), newRecord);
	}
	
	@Override
	protected void replaceEntryInIndex(byte[] key, RecordHeader header,
			RecordHeader newRecord) {
		super.replaceEntryInIndex(key, header, newRecord);
		memIndex.put(keyOf(key),newRecord);
	}

	/*
	 * Removes the record from the index. Replaces the target with the entry at
	 * the end of the index.
	 */
	@Override
	protected void deleteEntryFromIndex(byte[] key, RecordHeader header,
			int currentNumRecords) throws IOException, RecordsFileException {
		super.deleteEntryFromIndex(key, header, currentNumRecords);
		RecordHeader deleted = memIndex.remove(keyOf(key));
		assert header == deleted;
	}
	
	public static void main(String[] args) throws Exception {
		if( args.length < 1 ){
			err.println("not file passed");
		}
		final String filename = args[0];
		out.println("Reading from "+filename);
		final BaseRecordStore recordFile = new FileRecordStore(filename, "r", false);
		out.println(String.format("Records=%s, FileLength=%s, DataPointer=%s", recordFile.getNumRecords(), recordFile.getFileLength(), recordFile.dataStartPtr));
		for(int index = 0; index < recordFile.getNumRecords(); index++ ){
			final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
			final String key = keyOf(recordFile.readKeyFromIndex(index));
			out.println(String.format("Key=%s, HeaderIndex=%s, HeaderCapacity=%s, HeaderActual=%s, HeaderPointer=%s", key, header.indexPosition, header.getDataCapacity(), header.dataCount, header.dataPointer));
		}
	}
}
