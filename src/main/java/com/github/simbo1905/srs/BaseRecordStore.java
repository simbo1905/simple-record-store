package com.github.simbo1905.srs;

import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.val;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
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
    protected static final int RECORD_HEADER_LENGTH = 36;

    public static int getMaxKeyLengthOrDefault(String value) {
        final String key = String.format("%s.MAX_KEY_LENGTH", BaseRecordStore.class.getName());
        String keyLength = System.getenv(key) == null ? value : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Integer.valueOf(keyLength);
    }

    // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
    // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surragate key out of anything unique
    // about your data. Note we store binary keys with a header byte to indicate the real lenght of
    // the key so you need to +1 your max length
    protected static final int MAX_KEY_LENGTH = getMaxKeyLengthOrDefault("64");

    protected static final boolean PAD_DATA_TO_KEY_LENGTH = getPadDataToKeyLengthOrDefault(true);

    private static boolean getPadDataToKeyLengthOrDefault(boolean defaultValue) {
        final String key = String.format("%s.MAX_KEY_LENGTH", BaseRecordStore.class.getName());
        String keyLength = System.getenv(key) == null ? Boolean.valueOf(defaultValue).toString() : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Boolean.valueOf(keyLength);
    }

    // The total length of one index entry - the key length plus the record
    // header length.
    protected static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH
            + RECORD_HEADER_LENGTH;

    // File pointer to the num records header.
    protected static final long NUM_RECORDS_HEADER_LOCATION = 0;

    // File pointer to the data start pointer header.
    protected static final long DATA_START_HEADER_LOCATION = 4;

    public final boolean disableCrc32;

    /*
     * Creates a new database file, initializing the appropriate headers. Enough
     * space is allocated in the index for the specified initial size.
     * @param dbPath the location on disk to create the storage file.
     * @param initialSize an optimisation to preallocate the header storage area expressed as number of records.
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that will have a CRC check built in so you can safely disable here. =
     */
    protected BaseRecordStore(String dbPath, int initialSize, boolean disableCrc32)
            throws IOException, RecordsFileException {
        this.disableCrc32 = disableCrc32;
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
     * @param dbPath the location of the database file on disk to open.
     * @param accessFlags the access flags supported by the java java.io.RandomAccessFile e.g. "r" or "rw"
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that will have a CRC check built in so you can safely disable here.
     */
    protected BaseRecordStore(String dbPath, String accessFlags, boolean disableCrc32)
            throws IOException, RecordsFileException {
        this.disableCrc32 = disableCrc32;
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

    public boolean isEmpty() throws IOException {
        return readNumRecordsHeader() == 0;
    }

    /*
     * Checks there is a record with the given key.
     */
    public abstract boolean recordExists(byte[] key);

    /*
     * Maps a key to a record header.
     */
    protected abstract RecordHeader keyToRecordHeader(byte[] key)
            throws RecordsFileException;

    /*
     * Locates space for a new record of dataLength size and initializes a
     * RecordHeader.
     */
    protected abstract RecordHeader allocateRecord(byte[] key, int dataLength)
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
    byte[] readKeyFromIndex(int position) throws IOException {
        file.seek(indexPositionToKeyFp(position));
        byte len = file.readByte();
        byte[] key = new byte[len];
        file.read(key);
        return key;
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
    protected void addEntryToIndex(byte[] key, RecordHeader newRecord,
                                   int currentNumRecords) throws IOException, RecordsFileException {
        if (key.length > MAX_KEY_LENGTH) {
            throw new RecordsFileException(
                    "Key is larger than permitted size of " + MAX_KEY_LENGTH
                            + " bytes. Actual: "+key.length);
        }
        file.seek(indexPositionToKeyFp(currentNumRecords));
        file.write((byte)key.length);
        file.write(key);
        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        newRecord.write(file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords + 1);
    }

    /*
     * Removes the record from the index. Replaces the target with the entry at
     * the end of the index.
     */
    protected void deleteEntryFromIndex(byte[] key, RecordHeader header,
                                        int currentNumRecords) throws IOException, RecordsFileException {
        if (header.indexPosition != currentNumRecords - 1) {
            byte[] lastKey = readKeyFromIndex(currentNumRecords - 1);
            RecordHeader last = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);
            file.seek(indexPositionToKeyFp(last.indexPosition));
            file.write(lastKey.length);
            file.write(lastKey);
            file.seek(indexPositionToRecordHeaderFp(last.indexPosition));
            last.write(file);
        }
        writeNumRecordsHeader(currentNumRecords - 1);
    }

    @SneakyThrows
    public static String keyOf(byte[] key){
        return new String(key, "UTF8");
    }

    @SneakyThrows
    public static byte[] keyOf(String key){
        return key.getBytes("UTF8");
    }

    /*
     *
     */
    @Synchronized
    public RecordHeader insertRecord(byte[] key, byte[] value)
            throws RecordsFileException, IOException {
        if (recordExists(key)) {
            throw new RecordsFileException("Key exists: " + key);
        }
        ensureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(key, value.length);
        long crc32 = writeRecordData(newRecord, value);
        newRecord.setCrc32(crc32);
        addEntryToIndex(key, newRecord, getNumRecords());
        return newRecord;
    }

    /*
     * Updates an existing record. If the new contents do not fit in the
     * original record, then the update is handled by inserting the data
     */
    @Synchronized
    public void updateRecord(byte[] key, byte[] value)
            throws RecordsFileException, IOException {

        val updateMeHeader = keyToRecordHeader(key);

        // if can update in place
        if (value.length <= updateMeHeader.getDataCapacity() && disableCrc32 == false) {
            long crc = 0;
            CRC32 crc32 = new CRC32();
            crc32.update(value, 0, value.length);
            updateMeHeader.setTempCrc32(crc32.getValue());
            updateMeHeader.setDataCountTmp(value.length);
            // write with the backup crc so one of the two CRCs will be valid after a crash
            writeRecordHeaderToIndex(updateMeHeader);
            updateMeHeader.dataCount = value.length;
            updateFreeSpaceIndex(updateMeHeader);
            // write the main data
            writeRecordData(updateMeHeader, value);
            // update it main CRC
            updateMeHeader.setCrc32(crc);
            // write the header with the main CRC
            writeRecordHeaderToIndex(updateMeHeader);
            return;
        }

        // if last record expand or contract the file
        val endOfRecord = updateMeHeader.dataPointer + updateMeHeader.dataCount;
        val fileLength = getFileLength();
        if( endOfRecord == fileLength ){
            updateMeHeader.dataCount = value.length;
            setFileLength(fileLength + (value.length - updateMeHeader.getDataCapacity()) );
            updateMeHeader.setDataCapacity(value.length);
            updateFreeSpaceIndex(updateMeHeader);
            long crc32 = writeRecordData(updateMeHeader, value);
            updateMeHeader.setCrc32(crc32);
            writeRecordHeaderToIndex(updateMeHeader);
            return;
        }

        val originalHeader = new RecordHeader(updateMeHeader);

        // follow the insert logic
        if (value.length > updateMeHeader.getDataCapacity()) {
            // when we move we add capacity to the previous record
            RecordHeader previous = getRecordAt(updateMeHeader.dataPointer - 1);
            // allocate to next free space or expand the file
            RecordHeader newRecord = allocateRecord(key, value.length);
            // new record is expanded old record
            //newRecord.indexPosition = updateMeHeader.indexPosition;
            newRecord.dataCount = value.length;
            long crc32 = writeRecordData(newRecord, value);
            newRecord.setCrc32(crc32);
            writeRecordHeaderToIndex(newRecord);
            replaceEntryInIndex(key, updateMeHeader, newRecord);
            if( previous != null ){
                // append space of deleted record onto previous record
                previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
                updateFreeSpaceIndex(previous);
                writeRecordHeaderToIndex(previous);
            } else {
                // FIXME issue #12 this is really bad for a FIFO! Why don't we just expand the index area and have special free space logic?
                // target record is first in the file and is shrunk by moving the sec
                RecordHeader secondRecord = getRecordAt(originalHeader.dataPointer
                        + (long) originalHeader.getDataCapacity());
                byte[] movedBackwardsData = readRecordData(secondRecord);

                long fp = getFileLength();
                if (secondRecord.dataCount > originalHeader.dataCount) {
                    // wont fit entirely in slot so risk of corrupting itself
                    // make a backup at the end of the file first
                    setFileLength(fp + secondRecord.dataCount);
                    RecordHeader tempRecord = secondRecord.move(fp);
                    writeRecordData(tempRecord, movedBackwardsData);
                    writeRecordHeaderToIndex(tempRecord);
                }
                secondRecord.dataPointer = originalHeader.dataPointer;
                secondRecord.incrementDataCapacity(originalHeader.getDataCapacity());
                updateFreeSpaceIndex(secondRecord);
                writeRecordData(secondRecord, movedBackwardsData);
                updateFreeSpaceIndex(secondRecord);
                writeRecordHeaderToIndex(secondRecord);
                if (secondRecord.dataCount > originalHeader.dataCount) {
                    // delete backup at the end of the file
                    setFileLength(fp - secondRecord.dataCount);
                }
            }
            return;
        }

        throw new AssertionError("this line should be unreachable");
    }


    protected void replaceEntryInIndex(byte[] key, RecordHeader header, RecordHeader newRecord) {
        // nothing to do but lets subclasses do additional bookwork
    }

    /*
     * Reads the data for the record with the given key.
     */
    public byte[] readRecordData(byte[] key) throws IOException,
            RecordsFileException {
        val header = keyToRecordHeader(key);
        return readRecordData(header);
    }

    /*
     * Reads the record data for the given record header.
     */
    private byte[] readRecordData(RecordHeader header) throws IOException {

        val arrayOfPairs = new ArrayList<AbstractMap.SimpleImmutableEntry<Long, Integer>>();

        // must not attempt to read beyond the end of the file
        if( header.crc32.longValue() > -1 && header.dataCount+header.dataPointer <= getFileLength() ) {
            arrayOfPairs.add(new AbstractMap.SimpleImmutableEntry<>(header.crc32, header.dataCount));
        }

        // must not attempt to read beyond the end of the file
        if( header.crc32tmp.longValue() > -1 && header.dataCountTmp+header.dataPointer <= getFileLength() ) {
            arrayOfPairs.add(new AbstractMap.SimpleImmutableEntry<>(header.crc32tmp, header.dataCountTmp));
        }

        // if there are two possible lengths the we need to load the larger.
        arrayOfPairs.sort(Comparator.comparingInt(AbstractMap.SimpleImmutableEntry::getValue));
        val dataCountMax = arrayOfPairs.get(arrayOfPairs.size()-1).getValue().intValue();

        // read the data
        byte[] buf = new byte[dataCountMax];
        file.seek(header.dataPointer);
        file.readFully(buf);

        if( disableCrc32 ) {
            // if arrayOfPairs.size() > 0 then this is a guess. if we disable CRC32 we cannot reliably do update in place!
            return buf;
        }

        // try the shorter one
        val first = arrayOfPairs.get(0);

        CRC32 crc32 = new CRC32();
        crc32.update(buf, 0, first.getValue());
        val expectedCrc = first.getKey();

        if( expectedCrc.longValue() == crc32.getValue() ) {
            return buf;
        }

        if( arrayOfPairs.size() > 1){
            val last = arrayOfPairs.get(1);
            crc32.reset();
            crc32.update(buf, 0, last.getValue());
            val expectedCrcLast = crc32.getValue();
            if( expectedCrcLast == crc32.getValue() ){
                val length = last.getValue();
                if( length == buf.length ){
                    return buf;
                } else {
                    byte[] less = new byte[length];
                    System.arraycopy(buf, 0, less, 0, length);
                    return less;
                }
            }
        }

        throw new IllegalStateException(String.format("CRC32 check failed for data lenght %d with header %s", buf.length, header.toString()));
    }

    /*
     * Updates the contents of the given record. A RecordsFileException is
     * thrown if the new data does not fit in the space allocated to the record.
     * The header's data count is updated, but not written to the file.
     *
     * @ returns crc32 the CRC32 value of the written data.
     */
    private long writeRecordData(RecordHeader header, byte[] data)
            throws IOException, RecordsFileException {
        assert data.length <= header.getDataCapacity(): "Record data does not fit";
        header.dataCount = data.length;
        file.seek(header.dataPointer);
        file.write(data, 0, data.length);
        long crc = 0;
        if( !disableCrc32 ) {
            CRC32 crc32 = new CRC32();
            crc32.update(data, 0, data.length);
            crc = crc32.getValue();
        }
        return crc;
    }

    private void writeRecordDataNoCrc32(RecordHeader header, byte[] data)
            throws IOException, RecordsFileException {
        assert data.length <= header.getDataCapacity(): "Record data does not fit";
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
    public void deleteRecord(byte[] key)
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
                // FIXME this is really bad for a FIFO! Why don't we just expand the index area and have special free space logic?
                // target record is first in the file and is deleted by adding
                // its space to the second record.
                RecordHeader secondRecord = getRecordAt(delRec.dataPointer
                        + (long) delRec.getDataCapacity());
                byte[] data = readRecordData(secondRecord);

                final long fp = getFileLength();
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
                    setFileLength(fp);
                }
            }
        }
    }

    // Checks to see if there is space for and additional index entry. If
    // not, space is created by moving records to the end of the file.
    private void ensureIndexSpace(int requiredNumRecords)
            throws RecordsFileException, IOException {
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if ( isEmpty() && endIndexPtr > getFileLength()) {
            setFileLength(endIndexPtr);
            dataStartPtr = endIndexPtr;
            writeDataStartPtrHeader(dataStartPtr);
            return;
        }
        // move records to the back. if PAD_DATA_TO_KEY_LENGTH=true this should only move one record
        while (endIndexPtr > dataStartPtr) {
            RecordHeader first = getRecordAt(dataStartPtr);
            byte[] data = readRecordData(first);
            long fileLen = getFileLength();
            first.dataPointer = fileLen;
            int dataLength = data.length;
            int dataLengthPadded = getDataLengthPadded(dataLength);
            first.setDataCapacity(dataLengthPadded);
            setFileLength(fileLen + dataLengthPadded);
            writeRecordData(first, data);
            writeRecordHeaderToIndex(first);
            dataStartPtr += first.getDataCapacity();
            writeDataStartPtrHeader(dataStartPtr);
        }
    }

    protected static int getDataLengthPadded(int dataLength) {
        return (PAD_DATA_TO_KEY_LENGTH)?Math.max(INDEX_ENTRY_LENGTH, dataLength):dataLength;
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
