package com.github.simbo1905.srs;

import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.val;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.zip.CRC32;

public abstract class BaseRecordStore {

    /*default*/ RandomAccessFileInterface file;

    public void fsync() throws IOException {
        file.fsync();
    }

    // Current file pointer to the start of the record data.
    long dataStartPtr;

    // Total length in bytes of the global database headers. FIXME this should be long+int to 12?
    private static final int FILE_HEADERS_REGION_LENGTH = 16;

    // Number of bytes in the record header.
    static final int RECORD_HEADER_LENGTH = 28;

    private static final int DEFAULT_MAX_KEY_LENGTH = 64;

    private static int getMaxKeyLengthOrDefault() {
        final String key = String.format("%s.MAX_KEY_LENGTH", BaseRecordStore.class.getName());
        String keyLength = System.getenv(key) == null
                ? Integer.valueOf(DEFAULT_MAX_KEY_LENGTH).toString()
                : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Integer.parseInt(keyLength);
    }

    // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
    // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surragate key out of anything unique
    // about your data. Note we store binary keys with a header byte to indicate the real lenght of
    // the key so you need to +1 your max length
    private static final int MAX_KEY_LENGTH = getMaxKeyLengthOrDefault();

    private static final boolean PAD_DATA_TO_KEY_LENGTH = getPadDataToKeyLengthOrDefaultTrue();

    private static boolean getPadDataToKeyLengthOrDefaultTrue() {
        final String key = String.format("%s.MAX_KEY_LENGTH", BaseRecordStore.class.getName());
        String keyLength = System.getenv(key) == null ? Boolean.valueOf(true).toString() : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Boolean.parseBoolean(keyLength);
    }

    // The total length of one index entry - the key length plus the record
    // header length.
    private static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH
            + RECORD_HEADER_LENGTH;

    // File pointer to the num records header.
    private static final long NUM_RECORDS_HEADER_LOCATION = 0;

    // File pointer to the data start pointer header.
    private static final long DATA_START_HEADER_LOCATION = 4;

    // only change this when debugging in unit tests
    boolean disableCrc32;

    /*
     * Creates a new database file, initializing the appropriate headers. Enough
     * space is allocated in the index for the specified initial size.
     * @param dbPath the location on disk to create the storage file.
     * @param initialSize an optimisation to preallocate the header storage area expressed as number of records.
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that will have a CRC check built in so you can safely disable here. =
     */
    protected BaseRecordStore(String dbPath, int initialSize, boolean disableCrc32)
            throws IOException {
        this.disableCrc32 = disableCrc32;
        File f = new File(dbPath);
        if (f.exists()) {
            throw new IllegalArgumentException("Database already exits: " + dbPath);
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
            throws IOException {
        this.disableCrc32 = disableCrc32;
        File f = new File(dbPath);
        if (!f.exists()) {
            throw new IllegalArgumentException("Database not found: " + dbPath);
        }
        this.file = new DirectRandomAccessFile(new RandomAccessFile(f, accessFlags));
        dataStartPtr = readDataStartHeader();
    }

    /*
     * Returns an Iterable of the keys of all records in the database.
     *
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
            throws IOException;

    /*
     * Returns the record to which the target file pointer belongs - meaning the
     * specified location in the file is part of the record data of the
     * RecordHeader which is returned. Returns null if the location is not part
     * of a record.
     */
    protected abstract RecordHeader getRecordAt(long targetFp)
            throws IllegalArgumentException;

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
    long indexPositionToKeyFp(int pos) {
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
                                   int currentNumRecords) throws IOException {
        if (key.length > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException(
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
        return new String(key, StandardCharsets.UTF_8);
    }

    @SneakyThrows
    public static byte[] keyOf(String key){
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /*
     *
     */
    @Synchronized
    public RecordHeader insertRecord(byte[] key, byte[] value)
            throws IOException {
        if (recordExists(key)) {
            throw new IllegalArgumentException("Key exists: " + key);
        }
        ensureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(key, payloadLength(value.length));
        writeRecordData(newRecord, value);
        addEntryToIndex(key, newRecord, getNumRecords());
        return newRecord;
    }

    int payloadLength(int raw){
        int len = raw + 4; // for length prefix
        if(!disableCrc32) {
            len += 8;
        }
        return len;
    }

    /*
     * Updates an existing record. If the new contents do not fit in the
     * original record, then the update is handled by inserting the data
     */
    @Synchronized
    public void updateRecord(byte[] key, byte[] value)
            throws RecordsFileException, IOException {

        val updateMeHeader = keyToRecordHeader(key);
        val capacity = updateMeHeader.getDataCapacity();

        long crc = 0;
        if ( disableCrc32 == false) {
            CRC32 crc32 = new CRC32();
            crc32.update(value, 0, value.length);
        }

        val recordIsSameSize = value.length == capacity;
        val recordIsSmallerAndCrcEnabled = disableCrc32 == false && value.length < capacity;

        // can update in place if the record is same size no matter whether CRC32 is enabled.
        // if record is smaller then we can only update in place if we have a CRC32 to validate which data length is valid
        if( recordIsSameSize || recordIsSmallerAndCrcEnabled ){
            // write with the backup crc so one of the two CRCs will be valid after a crash
            writeRecordHeaderToIndex(updateMeHeader);
            updateMeHeader.dataCount = value.length;
            updateFreeSpaceIndex(updateMeHeader);
            // write the main data
            writeRecordData(updateMeHeader, value);
            // update it main CRC
            updateMeHeader.crc32 = crc;
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
            writeRecordData(updateMeHeader, value);
            updateMeHeader.crc32 = crc;
            writeRecordHeaderToIndex(updateMeHeader);
            return;
        }

        // follow the insert logic
        if (value.length > updateMeHeader.getDataCapacity()) {
            // when we move we add capacity to the previous record
            RecordHeader previous = getRecordAt(updateMeHeader.dataPointer - 1);
            // allocate to next free space or expand the file
            RecordHeader newRecord = allocateRecord(key, value.length);
            // new record is expanded old record
            newRecord.dataCount = value.length;
            writeRecordData(newRecord, value);
            newRecord.crc32 = crc;
            writeRecordHeaderToIndex(newRecord);
            replaceEntryInIndex(key, updateMeHeader, newRecord);
            if( previous != null ){
                // append space of deleted record onto previous record
                previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
                updateFreeSpaceIndex(previous);
                writeRecordHeaderToIndex(previous);
            } else {
                // make free space at the end of the index area
                writeDataStartPtrHeader(updateMeHeader.dataPointer);
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
    @Synchronized
    public byte[] readRecordData(byte[] key) throws IOException,
            RecordsFileException {
        val header = keyToRecordHeader(key);
        return readRecordData(header);
    }

    /*
     * Reads the record data for the given record header.
     */
    private byte[] readRecordData(RecordHeader header) throws IOException {
        // read the length
        file.seek(header.dataPointer);
        byte[] lenBytes = new byte[4];
        file.readFully(lenBytes);
        int len = (new DataInputStream(new ByteArrayInputStream(lenBytes))).readInt();

        // read the body
        byte[] buf = new byte[len];
        file.readFully(buf);

        if( disableCrc32 == false ) {
            byte[] crcBytes = new byte[8];
            file.readFully(crcBytes);
            val expectedCrc = (new DataInputStream(new ByteArrayInputStream(crcBytes))).readLong();
            CRC32 crc32 = new CRC32();
            crc32.update(buf, 0, buf.length);

            val actualCrc = crc32.getValue();

            //System.out.println(String.format("< in=crs:%d, len:%d %s bytes:%s", actualCrc, len, bytesToString(buf), print(buf)));

            if( actualCrc != expectedCrc ){
                throw new IllegalStateException(String.format("CRC32 check failed for data length %d with header %s", buf.length, header.toString()));
            }
        }

        return buf;
    }

    public static String print(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (byte b : bytes) {
            sb.append(String.format("0x%02X ", b));
        }
        sb.append("]");
        return sb.toString();
    }

    /*
     * Updates the contents of the given record. A RecordsFileException is
     * thrown if the new data does not fit in the space allocated to the record.
     * The header's data count is updated, but not written to the file.
     */
    private void writeRecordData(RecordHeader header, byte[] data)
            throws IOException {

        assert data.length <= header.getDataCapacity(): "Record data does not fit";
        header.dataCount = data.length;

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(header.dataCount);
        out.write(data);
        long crc = -1;
        if( !disableCrc32 ) {
            CRC32 crc32 = new CRC32();
            crc32.update(data, 0, data.length);
            crc = crc32.getValue();
            out.writeLong(crc);
        }
        out.close();
        val payload = bout.toByteArray();
        file.seek(header.dataPointer);
        file.write(payload, 0, payload.length);
        //System.out.println(String.format(">out=crs:%d, len:%d %s bytes:%s", crc, data.length, bytesToString(data), print(data)));
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
            throws IOException {
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
            int dataLength = payloadLength(data.length);
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

    public static final byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static final String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static final Function<String, byte[]> serializerString = (string) -> stringToBytes(string);

    public static final Function<byte[], String> deserializerString = (bytes) -> bytesToString(bytes);

    @SneakyThrows @Synchronized
    public void dumpHeaders(PrintStream out, boolean disableCrc32) {
        val oldDiableCrc32 = this.disableCrc32;
        try {
            this.disableCrc32 = disableCrc32;
            out.println(String.format("Records=%s, FileLength=%s, DataPointer=%s", getNumRecords(), getFileLength(), dataStartPtr));
            for(int index = 0; index < getNumRecords(); index++ ){
                final RecordHeader header = readRecordHeaderFromIndex(index);
                final byte[] bk = readKeyFromIndex(index);
                final String k = keyOf(bk);
                out.println(String.format("%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                        index,
                        k,
                        header.indexPosition,
                        header.getDataCapacity(),
                        header.dataCount,
                        header.dataPointer,
                        header.crc32
                ));
                final byte[] data = readRecordData(bk);

                String d = deserializerString.apply(data);
                out.println(String.format("%d data  len=%d data=%s", index, data.length, d));
            }
        } finally {
            this.disableCrc32 = oldDiableCrc32;
        }

    }
}
