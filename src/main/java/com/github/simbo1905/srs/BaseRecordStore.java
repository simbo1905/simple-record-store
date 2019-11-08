package com.github.simbo1905.srs;

import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.val;

import java.io.*;
import java.util.function.Function;
import java.util.zip.CRC32;

public abstract class BaseRecordStore {

    /*default*/ RandomAccessFileInterface file;

    public void fsync() throws IOException {
        file.fsync();
    }

    // Current file pointer to the start of the record data.
    long dataStartPtr;

    // Total length in bytes of the global database headers.
    protected static final int FILE_HEADERS_REGION_LENGTH = 16;

    // Number of bytes in the record header.
    protected static final int RECORD_HEADER_LENGTH = 24;

    public static int getMaxKeyLengthOrDefault(String value) {
        final String key = String.format("%s.MAX_KEY_LENGTH", BaseRecordStore.class.getName());
        String keyLength = System.getenv(key) == null ? value : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Integer.valueOf(keyLength);
    }

    // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
    // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surragate key out of anything unique
    // about your data
    protected static final int MAX_KEY_LENGTH = getMaxKeyLengthOrDefault("64");

    // The total length of one index entry - the key length plus the record
    // header length.
    protected static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH
            + RECORD_HEADER_LENGTH;

    // File pointer to the num records header.
    protected static final long NUM_RECORDS_HEADER_LOCATION = 0;

    // File pointer to the data start pointer header.
    protected static final long DATA_START_HEADER_LOCATION = 4;

    /*
     * Creates a new database file, initializing the appropriate headers. Enough
     * space is allocated in the index for the specified initial size.
     */
    protected BaseRecordStore(String dbPath, int initialSize)
            throws IOException, RecordsFileException {
        File f = new File(dbPath);
        if (f.exists()) {
            throw new RecordsFileException("Database already exits: " + dbPath);
        }
        this.file = new DirectRandomAccessFile(new RandomAccessFile(f, "rw"));
        dataStartPtr = indexPositionToKeyFp(initialSize); // Record Data Region
        // starts were the
        setFileLength(dataStartPtr); // (i+1)th index entry would start.
        writeNumRecordsHeader(0);
        writeDataStartPtrHeader(dataStartPtr);
    }

    /*
     * Opens an existing database file and initializes the dataStartPtr. The
     * accessFlags parameter can be "r" or "rw" -- as defined in
     * RandomAccessFile.
     */
    protected BaseRecordStore(String dbPath, String accessFlags)
            throws IOException, RecordsFileException {
        File f = new File(dbPath);
        if (!f.exists()) {
            throw new RecordsFileException("Database not found: " + dbPath);
        }
        this.file = new DirectRandomAccessFile(new RandomAccessFile(f, accessFlags));
        dataStartPtr = readDataStartHeader();
    }

    /*
     * Returns an Iterable of the keys of all records in the database.
     */
    public abstract Iterable<String> keys();

    /*
     * Returns the number or records in the database.
     */
    public abstract int getNumRecords();

    /*
     * Checks there is a record with the given key.
     */
    public abstract boolean recordExists(String key);

    /*
     * Maps a key to a record header.
     */
    protected abstract RecordHeader keyToRecordHeader(String key)
            throws RecordsFileException;

    /*
     * Locates space for a new record of dataLength size and initializes a
     * RecordHeader.
     */
    protected abstract RecordHeader allocateRecord(String key, int dataLength)
            throws RecordsFileException, IOException;

    /*
     * Returns the record to which the target file pointer belongs - meaning the
     * specified location in the file is part of the record data of the
     * RecordHeader which is returned. Returns null if the location is not part
     * of a record.
     */
    protected abstract RecordHeader getRecordAt(long targetFp)
            throws RecordsFileException;

    protected long getFileLength() throws IOException {
        return file.length();
    }

    protected void setFileLength(long l) throws IOException {
        file.setLength(l);
    }

    /*
     * Reads the number of records header from the file.
     */
    protected int readNumRecordsHeader() throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        return file.readInt();
    }

    /*
     * Writes the number of records header to the file.
     */
    protected void writeNumRecordsHeader(int numRecords) throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        file.writeInt(numRecords);
    }

    /*
     * Reads the data start pointer header from the file.
     */
    protected long readDataStartHeader() throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        return file.readLong();
    }

    /*
     * Writes the data start pointer header to the file.
     */
    private void writeDataStartPtrHeader(long dataStartPtr)
            throws IOException {
        file.seek(DATA_START_HEADER_LOCATION);
        file.writeLong(dataStartPtr);
    }

    /*
     * Returns a file pointer in the index pointing to the first byte in the key
     * located at the given index position.
     */
    private long indexPositionToKeyFp(int pos) {
        return FILE_HEADERS_REGION_LENGTH + (INDEX_ENTRY_LENGTH * pos);
    }

    /*
     * Returns a file pointer in the index pointing to the first byte in the
     * record pointer located at the given index position.
     */
    private long indexPositionToRecordHeaderFp(int pos) {
        return indexPositionToKeyFp(pos) + MAX_KEY_LENGTH;
    }

    /*
     * Reads the ith key from the index.
     */
    String readKeyFromIndex(int position) throws IOException {
        file.seek(indexPositionToKeyFp(position));
        return file.readUTF();
    }

    /*
     * Reads the ith record header from the index.
     */
    RecordHeader readRecordHeaderFromIndex(int position) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(position));
        return RecordHeader.readHeader(file);
    }

    /*
     * Writes the ith record header to the index.
     */
    protected void writeRecordHeaderToIndex(RecordHeader header)
            throws IOException {
        file.seek(indexPositionToRecordHeaderFp(header.indexPosition));
        header.write(file);
    }

    /*
     * Appends an entry to end of index. Assumes that insureIndexSpace() has
     * already been called.
     */
    protected void addEntryToIndex(String key, RecordHeader newRecord,
                                   int currentNumRecords) throws IOException, RecordsFileException {
        DbByteArrayOutputStream keybuffer = new DbByteArrayOutputStream(
                MAX_KEY_LENGTH);
        (new DataOutputStream(keybuffer)).writeUTF(key);
        if (keybuffer.size() > MAX_KEY_LENGTH) {
            throw new RecordsFileException(
                    "Key is larger than permitted size of " + MAX_KEY_LENGTH
                            + " bytes");
        }
        file.seek(indexPositionToKeyFp(currentNumRecords));
        keybuffer.writeTo(file);
        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        newRecord.write(file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords + 1);
    }

    /*
     * Removes the record from the index. Replaces the target with the entry at
     * the end of the index.
     */
    protected void deleteEntryFromIndex(String key, RecordHeader header,
                                        int currentNumRecords) throws IOException, RecordsFileException {
        if (header.indexPosition != currentNumRecords - 1) {
            String lastKey = readKeyFromIndex(currentNumRecords - 1);
            RecordHeader last = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);
            file.seek(indexPositionToKeyFp(last.indexPosition));
            file.writeUTF(lastKey);
            file.seek(indexPositionToRecordHeaderFp(last.indexPosition));
            last.write(file);
        }
        writeNumRecordsHeader(currentNumRecords - 1);
    }

    /*
     * Adds the given record to the database.
     */
    @Synchronized
    public void insertRecord(RecordWriter rw)
            throws RecordsFileException, IOException {
        insertRecord0(rw);
    }

    /*
     * this method exposes more to the caller for junit testing
     */
    @Synchronized
    RecordHeader insertRecord0(RecordWriter rw)
            throws RecordsFileException, IOException {
        String key = rw.getKey();
        if (recordExists(key)) {
            throw new RecordsFileException("Key exists: " + key);
        }
        insureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(key, rw.getDataLength());
        long crc32 = writeRecordData(newRecord, rw);
        newRecord.setCrc32(crc32);
        addEntryToIndex(key, newRecord, getNumRecords());
        return newRecord;
    }

    /*
     * Updates an existing record. If the new contents do not fit in the
     * original record, then the update is handled by inserting the data
     */
    @Synchronized
    public void updateRecord(RecordWriter rw)
            throws RecordsFileException, IOException {
        String key = rw.getKey();
        RecordHeader oldHeader = keyToRecordHeader(key);
        if (rw.getDataLength() > oldHeader.getDataCapacity()) {
            RecordHeader newRecord = allocateRecord(key, rw.getDataLength());
            newRecord.indexPosition = oldHeader.indexPosition;
            newRecord.dataCount = oldHeader.dataCount;
            long crc32 = writeRecordData(newRecord, rw);
            newRecord.setCrc32(crc32);
            writeRecordHeaderToIndex(newRecord);
            replaceEntryInIndex(key, oldHeader, newRecord);
        } else {
            long crc32 = writeRecordData(oldHeader, rw);
            oldHeader.setCrc32(crc32);
            writeRecordHeaderToIndex(oldHeader);
        }
    }

    protected void replaceEntryInIndex(String key, RecordHeader header, RecordHeader newRecord) {
        // nothing to do but lets subclasses do additional bookwork
    }

    /*
     * Reads a record.
     */
    @Synchronized
	@SneakyThrows(ClassNotFoundException.class)
    public <T> T readRecord(String key, Function<byte[], T> deserializer)
            throws RecordsFileException, IOException {
        byte[] data = readRecordData(key);
		val recordReader = new RecordReader(key, data, deserializer);
        return (T)recordReader.readObject();
    }

    /*
     * Reads the data for the record with the given key.
     */
    private byte[] readRecordData(String key) throws IOException,
            RecordsFileException {
        return readRecordData(keyToRecordHeader(key));
    }

    /*
     * Reads the record data for the given record header.
     */
    private byte[] readRecordData(RecordHeader header) throws IOException {
        byte[] buf = new byte[header.dataCount];
        file.seek(header.dataPointer);
        file.readFully(buf);
        CRC32 crc32 = new CRC32();
        crc32.update(buf);
        if (header.crc32.longValue() != crc32.getValue()) {
            //throw new IllegalStateException(String.format("CRC32 check failed for %s", header.toString()));
        }
        return buf;
    }

    /*
     * Updates the contents of the given record. A RecordsFileException is
     * thrown if the new data does not fit in the space allocated to the record.
     * The header's data count is updated, but not written to the file.
     *
     * @ returns crc32 the CRC32 value of the written data.
     */
    private long writeRecordData(RecordHeader header, RecordWriter rw)
            throws IOException, RecordsFileException {
        if (rw.getDataLength() > header.getDataCapacity()) {
            throw new RecordsFileException("Record data does not fit");
        }
        header.dataCount = rw.getDataLength();
        file.seek(header.dataPointer);
        return rw.writeTo(file);
    }

    /*
     * Updates the contents of the given record. A RecordsFileException is
     * thrown if the new data does not fit in the space allocated to the record.
     * The header's data count is updated, but not written to the file.
     */
    private void writeRecordData(RecordHeader header, byte[] data)
            throws IOException, RecordsFileException {
        if (data.length > header.getDataCapacity()) {
            throw new RecordsFileException("Record data does not fit");
        }
        header.dataCount = data.length;
        file.seek(header.dataPointer);
        file.write(data, 0, data.length);
    }

    /*
     * When allocating storage we look for a record that has space due to deletions. This method allows
     * implementations to record fee space for fast lookup rather than by scanning all the headers. The
     * default implementation does nothing.
     * @param rh Record that has new free space.
     */
    protected void updateFreeSpaceIndex(RecordHeader rh) {
        return;
    }

    /*
     * Deletes a record.
     */
    @Synchronized
    public void deleteRecord(String key)
            throws RecordsFileException, IOException {
        RecordHeader delRec = keyToRecordHeader(key);
        RecordHeader previous = getRecordAt(delRec.dataPointer - 1);
        int currentNumRecords = getNumRecords();
        deleteEntryFromIndex(key, delRec, currentNumRecords);
        if (getFileLength() == delRec.dataPointer + delRec.getDataCapacity()) {
            // shrink file since this is the last record in the file
            setFileLength(delRec.dataPointer);
        } else {
            if (previous != null) {
                // append space of deleted record onto previous record
                previous.incrementDataCapacity(delRec.getDataCapacity());
                updateFreeSpaceIndex(previous);
                writeRecordHeaderToIndex(previous);
            } else {
                // target record is first in the file and is deleted by adding
                // its space to the second record.
                RecordHeader secondRecord = getRecordAt(delRec.dataPointer
                        + (long) delRec.getDataCapacity());
                byte[] data = readRecordData(secondRecord);

                long fp = getFileLength();
                if (secondRecord.dataCount > delRec.dataCount) {
                    // wont fit entirely in slot so risk of corrupting itself
                    // make a backup at the end of the file first
                    setFileLength(fp + secondRecord.dataCount);
                    RecordHeader tempRecord = secondRecord.move(fp);
                    writeRecordData(tempRecord, data);
                    writeRecordHeaderToIndex(tempRecord);
                }
                secondRecord.dataPointer = delRec.dataPointer;
                secondRecord.incrementDataCapacity(delRec.getDataCapacity());
                updateFreeSpaceIndex(secondRecord);
                writeRecordData(secondRecord, data);
                writeRecordHeaderToIndex(secondRecord);
                if (secondRecord.dataCount > delRec.dataCount) {
                    // delete backup at the end of the file
                    setFileLength(fp + secondRecord.dataCount);
                }
            }
        }
    }

    // Checks to see if there is space for and additional index entry. If
    // not, space is created by moving records to the end of the file.
    private void insureIndexSpace(int requiredNumRecords)
            throws RecordsFileException, IOException {
        int currentNumRecords = getNumRecords();
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if (endIndexPtr > getFileLength() && currentNumRecords == 0) {
            setFileLength(endIndexPtr);
            dataStartPtr = endIndexPtr;
            writeDataStartPtrHeader(dataStartPtr);
            return;
        }
        while (endIndexPtr > dataStartPtr) {
            RecordHeader first = getRecordAt(dataStartPtr);
            byte[] data = readRecordData(first);
            first.dataPointer = getFileLength();
            first.setDataCapacity(data.length);
            setFileLength(first.dataPointer + data.length);
            writeRecordData(first, data);
            writeRecordHeaderToIndex(first);
            dataStartPtr += first.getDataCapacity();
            writeDataStartPtrHeader(dataStartPtr);
        }
    }

    /*
     * Closes the file.
     */
    @Synchronized
    public void close() throws IOException, RecordsFileException {
        try {
            file.close();
        } finally {
            file = null;
        }
    }

    @Synchronized
    public void insertRecords(RecordWriter... writers) throws Exception {
        insureIndexSpace(getNumRecords() + writers.length);
        for (RecordWriter rw : writers) {
            String key = rw.getKey();
            if (recordExists(key)) {
                throw new RecordsFileException("Key exists: " + key);
            }
            long fp = getFileLength();
            setFileLength(fp + rw.getDataLength());
            RecordHeader newRecord = new RecordHeader(fp, rw.getDataLength());
            long crc32 = writeRecordData(newRecord, rw);
            newRecord.setCrc32(crc32);
            addEntryToIndex(key, newRecord, getNumRecords());
        }
    }

    @Synchronized // ToDo
    private <K, V> void insertRecord(Entry<K, V> entry
            , Function<K, byte[]> serializerKey
            , Function<V, byte[]> serializerValue)
            throws RecordsFileException, IOException {
        RecordWriter<V> rw = new RecordWriter<V>(entry.key.toString(), serializerValue);
        rw.writeObject(entry.value);
        insertRecord0(rw);
    }

    @Synchronized
    public <V> void insertRecord(Entry<String, V> entry
            , Function<V, byte[]> serializerValue)
            throws RecordsFileException, IOException {
        RecordWriter<V> rw = new RecordWriter<V>(entry.key.toString(), serializerValue);
        rw.writeObject(entry.value);
        insertRecord0(rw);
    }

    @SneakyThrows(UnsupportedEncodingException.class)
    public static final byte[] stringToBytes(String s) {
        return s.getBytes("UTF8");
    }

    @SneakyThrows(UnsupportedEncodingException.class)
    public static final String bytesToString(byte[] bytes) {
        return new String(bytes, "UTF8");
    }

    public static final Function<String, byte[]> serializerString = (string) -> stringToBytes(string);

    public static final Function<byte[], String> deserializerString = (bytes) -> bytesToString(bytes);


}
