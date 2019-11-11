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
import java.util.zip.CRC32;

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
	 * @param dbPath the location on disk to create the storage file.
	 * @param initialSize an optimisation to preallocate the header storage area expressed as number of records.
	 * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that will have a CRC check built in so you can safely disable here. =
	 */
	public FileRecordStore(String dbPath, int initialSize, boolean disableCrc32) throws IOException,
			RecordsFileException {
		super(dbPath, initialSize, disableCrc32);
		memIndex = Collections
				.synchronizedMap(new HashMap<String, RecordHeader>(initialSize));
	}

	/*
	 * Opens an existing database and initializes the in-memory index.
	 * @param dbPath the location of the database file on disk to open.
	 * @param accessFlags the access flags supported by the java java.io.RandomAccessFile e.g. "r" or "rw"
	 * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that will have a CRC check built in so you can safely disable here.
	 */
	public FileRecordStore(String dbPath, String accessFlags, boolean disableCrc32) throws IOException,
			RecordsFileException {
		super(dbPath, accessFlags, disableCrc32);
		int numRecords = readNumRecordsHeader();
		memIndex = Collections
				.synchronizedMap(new HashMap<>(numRecords));
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
	 * This method searches the free map for free space and then returns a
	 * RecordHeader which uses the space.
	 */
	@Override
	protected RecordHeader allocateRecord(byte[] key, int dataLength)
			throws IOException {
		// we pad the record to be at least the size of a header to avoid moving many values to expand the the index
		int dataLengthPadded = getDataLengthPadded(dataLength);
		// search for empty space
		RecordHeader newRecord = null;
		for (RecordHeader next : this.freeMap.keySet() ) {
			int free = next.getFreeSpace();
			if (dataLengthPadded <= free) {
				newRecord = next.split();
				updateFreeSpaceIndex(next);
				writeRecordHeaderToIndex(next);
				break;
			}
		}
		if (newRecord == null) {
			// append record to end of file - grows file to allocate space
			long fp = getFileLength();
			setFileLength(fp + dataLengthPadded);
			newRecord = new RecordHeader(fp, dataLengthPadded);
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
		boolean disableCrc32 = false;
		dumpFile(filename, disableCrc32);
	}

	public static void dumpFile(String filename, boolean disableCrc) throws IOException, RecordsFileException {
		final BaseRecordStore recordFile = new FileRecordStore(filename, "r", disableCrc);
		out.println(String.format("Records=%s, FileLength=%s, DataPointer=%s", recordFile.getNumRecords(), recordFile.getFileLength(), recordFile.dataStartPtr));
		for(int index = 0; index < recordFile.getNumRecords(); index++ ){
			final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
			final byte[] bk = recordFile.readKeyFromIndex(index);
			final String k = keyOf(bk);
			out.println(String.format("%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s, crc32tmp=%s",
					index,
					k,
					header.indexPosition,
					header.getDataCapacity(),
					header.dataPointer,
					header.dataCount,
					header.crc32,
					header.dataCountTmp,
					header.crc32tmp
			));
			final byte[] data = recordFile.readRecordData(bk);
			CRC32 crc32 = new CRC32();
			crc32.update(data, 0, header.dataCount);
			long crcActual = crc32.getValue();
			boolean good = header.crc32.longValue() == crcActual;
			if( good ){
				String d = deserializerString.apply(data);
				out.println(String.format("%d data  Data=%s, len=%d, crcActual=%s, crcGood=true", index, d, data.length, good));
			} else {
				crc32.reset();
				crc32.update(data, 0, header.dataCountTmp);
				long crcActualTmp = crc32.getValue();
				good = header.crc32tmp.longValue() == crcActualTmp;
				if( good ) {
					if( data.length == header.dataCountTmp ){
						String d = deserializerString.apply(data);
						out.println(String.format("%d data  Data=%s, len=%d, crcActual=%s, crcGood=true", index, d, data.length, good));
					} else {
						byte[] less = new byte[header.dataCountTmp];
						System.arraycopy(data, 0, less, 0, header.dataCountTmp);
						String d = deserializerString.apply(less);
						out.println(String.format("%d data  Data=%s, len=%d, crcActual=%s, crcGood=true", index, d, data.length, good));

					}
				} else {
					String d = deserializerString.apply(data);
					out.println(String.format("%d data  Data=%s, len=%d, crcActual=%s, crcGood=true", index, d, data.length, good));
				}
			}
		}
	}
}
