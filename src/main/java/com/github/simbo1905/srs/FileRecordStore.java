package com.github.simbo1905.srs;

import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.val;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

public class FileRecordStore {

    final static Logger logger = Logger.getLogger(FileRecordStore.class.getName());

    // Number of bytes in the record header.
    static final int RECORD_HEADER_LENGTH = 20;

    // Total length in bytes of the global database headers.
    private static final int FILE_HEADERS_REGION_LENGTH = 12;
    private static final int DEFAULT_MAX_KEY_LENGTH = 64;
    private static final int CRC32_LENGTH = 4; // its an unsigned 32 that has to be in a long

    // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
    // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surrogate key out of anything
	// unique about your data. You can also set it to be a max of 248 bytes. Note we store binary keys with a header byte
    // and a CRC32 which is an unsigned 32 stored as a long.
    private final int MAX_KEY_LENGTH = getMaxKeyLengthOrDefault();
    private static final boolean PAD_DATA_TO_KEY_LENGTH = getPadDataToKeyLengthOrDefaultTrue();

    // The total length of one index entry - the key length plus the record
    // header length and the CRC of the key which is an unsigned 32 bits.
    private final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH + Integer.BYTES
            + RECORD_HEADER_LENGTH;

    // File pointer to the num records header.
    private static final long NUM_RECORDS_HEADER_LOCATION = 0;

    private static final long DATA_START_HEADER_LOCATION = 4;

    /*default*/ RandomAccessFileInterface file;

    /*
     * Hashtable which holds the in-memory index. For efficiency, the entire
     * index is cached in memory. The hashtable wraps the byte[] key as a String
     * as you cannot use a raw byte[] as a key and Java doesn't have extension methods yet.
     */
    protected Map<String, RecordHeader> memIndex;


    private Comparator<RecordHeader> compareRecordHeaderByFreeSpace = new Comparator<RecordHeader>() {
        @Override
        public int compare(RecordHeader o1, RecordHeader o2) {
            return (int) (o1.getFreeSpace(true) - o2.getFreeSpace(true));
        }
    };

    /*
     * ConcurrentSkipListMap makes scanning by ascending values fast and is sorted by smallest free space first
     */
    private ConcurrentNavigableMap<RecordHeader, Integer> freeMap =
            new ConcurrentSkipListMap<>(compareRecordHeaderByFreeSpace);

    // Current file pointer to the start of the record data.
	private long dataStartPtr;

    // only change this when debugging in unit tests
	private boolean disableCrc32;

    /*
     * Creates a new database file. The initialSize parameter determines the
     * amount of space which is allocated for the index. The index can grow
     * dynamically, but the parameter is provide to increase efficiency.
     * @param dbPath the location on disk to create the storage file.
     * @param initialSize an optimisation to preallocate the header storage area expressed as number of records.
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that
     * will have a CRC check built in so you can safely disable here. =
     */
    public FileRecordStore(String dbPath, int initialSize, boolean disableCrc32) throws IOException {
        this.disableCrc32 = disableCrc32;
        File f = new File(dbPath);
        if (f.exists()) {
            throw new IllegalArgumentException("Database already exits: " + dbPath);
        }
        this.file = new DirectRandomAccessFile(new RandomAccessFile(f, "rw"));
        FileRecordStore.this.dataStartPtr = indexPositionToKeyFp(initialSize); // Record Data Region starts were the
        setFileLength(FileRecordStore.this.dataStartPtr); // (i+1)th index entry would start.
        writeNumRecordsHeader(0);
        FileRecordStore.this.writeDataStartPtrHeader(FileRecordStore.this.dataStartPtr);
        memIndex = new HashMap<String, RecordHeader>(initialSize);
    }

    /*
     * Opens an existing database and initializes the in-memory index.
     * @param dbPath the location of the database file on disk to open.
     * @param accessFlags the access flags supported by the java java.io.RandomAccessFile e.g. "r" or "rw"
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that
     * will have a CRC check built in so you can safely disable here.
     */
    public FileRecordStore(String dbPath, String accessFlags, boolean disableCrc32) throws IOException {
        this.disableCrc32 = disableCrc32;
        File f = new File(dbPath);
        if (!f.exists()) {
            throw new IllegalArgumentException("Database not found: " + dbPath);
        }
        this.file = new DirectRandomAccessFile(new RandomAccessFile(f, accessFlags));
        FileRecordStore.this.dataStartPtr = readDataStartHeader();
        int numRecords = readNumRecordsHeader();
        memIndex = new HashMap<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            byte[] key = readKeyFromIndex(i);
            val k = keyOf(key);
            RecordHeader header = readRecordHeaderFromIndex(i);
            header.setIndexPosition(i);
            memIndex.put(k, header);
        }
    }

    private static int getMaxKeyLengthOrDefault() {
        final String key = String.format("%s.MAX_KEY_LENGTH", FileRecordStore.class.getName());
        String keyLength = System.getenv(key) == null
                ? Integer.valueOf(DEFAULT_MAX_KEY_LENGTH).toString()
                : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Integer.parseInt(keyLength);
    }

    private static boolean getPadDataToKeyLengthOrDefaultTrue() {
        final String key = String.format("%s.MAX_KEY_LENGTH", FileRecordStore.class.getName());
        String keyLength = System.getenv(key) == null ? Boolean.valueOf(true).toString() : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Boolean.parseBoolean(keyLength);
    }

    @SneakyThrows
	private static String keyOf(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    @SneakyThrows
    public static byte[] keyOf(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
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

    private int getDataLengthPadded(int dataLength) {
        return (PAD_DATA_TO_KEY_LENGTH) ? Math.max(INDEX_ENTRY_LENGTH, dataLength) : dataLength;
    }

    public static final byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static final String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Synchronized
	Iterable<String> keys() {
        return new HashSet(memIndex.keySet());
    }

    /*
     * Returns the current number of records in the database.
     */
    @Synchronized
	int getNumRecords() {
        return memIndex.size();
    }

    @Synchronized
    public boolean isEmpty() {
        return memIndex.isEmpty();
    }

    /*
     * Checks if there is a record belonging to the given key.
     */
    @Synchronized
    public boolean recordExists(byte[] key) {
        return memIndex.containsKey(keyOf(key));
    }

    /*
     * Maps a key to a record header by looking it up in the in-memory index.
     */
	private RecordHeader keyToRecordHeader(byte[] key) {
        val k = keyOf(key);
        RecordHeader h = memIndex.get(k);
        if (h == null) {
            throw new IllegalArgumentException(String.format("Key not found %s '%s'", print(key), bytesToString(key)));
        }
        return h;
    }

    /*
     * Updates a map of record headers to free space values.
     *
     * @param rh Record that has new free space.
     */
	private void updateFreeSpaceIndex(RecordHeader rh) {
        int free = rh.getFreeSpace(disableCrc32);
        if (free > 0) {
            freeMap.put(rh, free);
        } else {
            freeMap.remove(rh);
        }
    }

    /*
     * This method searches the free map for free space and then returns a
     * RecordHeader which uses the space.
     */
	private RecordHeader allocateRecord(byte[] key, int dataLength)
            throws IOException {

        // we needs space for the length int and the optional long crc32
        int payloadLength = payloadLength(dataLength);

        // we pad the record to be at least the size of a header to avoid moving many values to expand the the index
        int dataLengthPadded = getDataLengthPadded(payloadLength);

        // FIFO deletes cause free space after the index.
        long dataStart = readDataStartHeader();
        long endIndexPtr = indexPositionToKeyFp(getNumRecords());
        // we prefer speed overs space so we leave space for the header for this insert plus one for future use
        long available = dataStart - endIndexPtr - (2 * INDEX_ENTRY_LENGTH);

        RecordHeader newRecord = null;

        if (dataLengthPadded <= available) {
            newRecord = new RecordHeader(dataStart - dataLengthPadded, dataLengthPadded);
            return newRecord;
        }

        // search for empty space
        for (RecordHeader next : this.freeMap.keySet()) {
            int free = next.getFreeSpace(disableCrc32);
            if (dataLengthPadded <= free) {
                newRecord = next.split(disableCrc32, payloadLength(0));
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
	private RecordHeader getRecordAt(long targetFp) {
        for (RecordHeader next : this.memIndex.values()) {
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
    @Synchronized
    public void close() throws IOException  {
        try {
            try {
                file.close();
            } finally {
                file = null;
            }
        } finally {
            memIndex.clear();
            memIndex = null;
            freeMap.clear();
            freeMap = null;
        }
    }

    /*
     * Adds the new record to the in-memory index and calls the super class add
     * the index entry to the file.
     */
	private void addEntryToIndex(byte[] key, RecordHeader newRecord,
								 int currentNumRecords) throws IOException {
        if (key.length > MAX_KEY_LENGTH - CRC32_LENGTH) {
            throw new IllegalArgumentException(
                    String.format("Key of len %d is larger than permitted max size of %d bytes. You can increase this to 248 using env var or system property %s.MAX_KEY_LENGTH",
                            key.length,
                            MAX_KEY_LENGTH,
                            FileRecordStore.class.getName()));
        }

        val fp = indexPositionToRecordHeaderFp(currentNumRecords);
        val fpk = indexPositionToKeyFp(currentNumRecords);

        writeKeyToIndex(key, currentNumRecords);

        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        newRecord.write(file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords + 1);
        memIndex.put(keyOf(key), newRecord);
    }

    /*
     * Removes the record from the index. Replaces the target with the entry at
     * the end of the index.
     */
	private void deleteEntryFromIndex(byte[] key, RecordHeader header,
									  int currentNumRecords) throws IOException {
        if (header.indexPosition != currentNumRecords - 1) {
            byte[] lastKey = readKeyFromIndex(currentNumRecords - 1);
            RecordHeader last = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);

            writeKeyToIndex(lastKey, last.indexPosition);

            file.seek(this.indexPositionToRecordHeaderFp(last.indexPosition));
            last.write(file);
        }
        writeNumRecordsHeader(currentNumRecords - 1);
        RecordHeader deleted = memIndex.remove(keyOf(key));
        assert header == deleted;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("no file passed");
        }
        final String filename = args[0];
        logger.info("Reading from " + filename);
        boolean disableCrc32 = false;
        dumpFile(Level.INFO, filename, disableCrc32);
    }

    static void dumpFile(Level level, String filename, boolean disableCrc) throws IOException {
        final FileRecordStore recordFile = new FileRecordStore(filename, "r", disableCrc);
        logger.log(level, String.format("Records=%s, FileLength=%s, DataPointer=%s", recordFile.getNumRecords(), recordFile.getFileLength(), recordFile.dataStartPtr));
        for (int index = 0; index < recordFile.getNumRecords(); index++) {
            final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
            final byte[] bk = recordFile.readKeyFromIndex(index);
            final String k = keyOf(bk);
            logger.log(level, String.format("%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                    index,
                    k,
                    header.indexPosition,
                    header.getDataCapacity(),
                    header.dataCount,
                    header.dataPointer,
                    header.crc32
            ));
            final byte[] data = recordFile.readRecordData(bk);

            String d = bytesToString(data);
            logger.log(level, String.format("%d data  len=%d data=%s", index, data.length, d));
        }
    }

    @Synchronized
    public void fsync() throws IOException {
        file.fsync();
    }

    long getFileLength() throws IOException {
        return file.length();
    }

    private void setFileLength(long l) throws IOException {
        file.setLength(l);
    }

    /*
     * Reads the number of records header from the file.
     */
	private int readNumRecordsHeader() throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        return file.readInt();
    }

    /*
     * Writes the number of records header to the file.
     */
	private void writeNumRecordsHeader(int numRecords) throws IOException {
        file.seek(NUM_RECORDS_HEADER_LOCATION);
        file.writeInt(numRecords);
    }

    /*
     * Reads the data start pointer header from the file.
     */
	private long readDataStartHeader() throws IOException {
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

    private void writeKeyToIndex(byte[] key, int index) throws IOException {
        val len = (byte) key.length;
        val writeLen = key.length + 1 + CRC32_LENGTH;

        ByteBuffer buffer = ByteBuffer.allocate(writeLen);
        buffer.put(len);
        buffer.put(key, 0, key.length);

        // compute crc from the backing array with what we have written
        val array = buffer.array();
        CRC32 crc = new CRC32();
        crc.update(array, 1, key.length);
        int crc32 = (int) (crc.getValue() & 0xFFFFFFFFL);

        // add the crc which will write through to the backing array
        buffer.putInt(crc32);

        val fpk = indexPositionToKeyFp(index);
        file.seek(fpk);
        file.write(array, 0, writeLen);

        FileRecordStore.logger.log(Level.FINEST,
                ">k fp:{0} idx:{5} len:{1} end:{4} crc:{3} key:{6} bytes:{2}"
                , new Object[]{fpk, len, print(key), crc32, fpk+len, index, bytesToString(key) });
    }

    /*
     * Reads the ith key from the index.
     */
	private byte[] readKeyFromIndex(int position) throws IOException {
	    val fp = indexPositionToKeyFp(position);
        file.seek(fp);

        int len = file.readByte() & 0xFF; // interpret as unsigned byte https://stackoverflow.com/a/56052675/329496

        assert len < MAX_KEY_LENGTH;

        byte[] key = new byte[len];
        file.read(key);

        byte[] crcBytes = new byte[CRC32_LENGTH];
        file.read(crcBytes);
        ByteBuffer buffer = ByteBuffer.allocate(CRC32_LENGTH);
        buffer.put(crcBytes);
        buffer.flip();
        long crc32expected = buffer.getInt() & 0xffffffffL; // https://stackoverflow.com/a/22938125/329496

        CRC32 crc = new CRC32();
        crc.update(key, 0, key.length );
        val crc32actual = crc.getValue();

        FileRecordStore.logger.log(Level.FINEST,
                "<k fp:{0} idx:{5} len:{1} end:{4} crc:{3} key:{6} bytes:{2}",
                new Object[]{fp, len, print(key), crc32actual, fp+len, position, bytesToString(key)});

        if( crc32actual != crc32expected ){
            throw new IllegalStateException(
                    String.format("invalid key CRC32 expected %d and actual %s for len %d and fp %d found key %s with bytes %s",
                            crc32expected,
                            crc32actual,
                            len,
                            fp,
                            bytesToString(key),
                            print(key)
                    ));
        }

        return key;
    }

    /*
     * Reads the ith record header from the index.
     */
	private RecordHeader readRecordHeaderFromIndex(int index) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(index));
        return RecordHeader.readHeader(index, file);
    }

    /*
     * Writes the ith record header to the index.
     */
	private void writeRecordHeaderToIndex(RecordHeader header)
            throws IOException {
        file.seek(indexPositionToRecordHeaderFp(header.indexPosition));
        header.write(file);
    }

    /*
     *
     */
    @Synchronized
    public void insertRecord(byte[] key, byte[] value)
            throws IOException {
        if (recordExists(key)) {
            throw new IllegalArgumentException("Key exists: " + key);
        }
        ensureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(key, payloadLength(value.length));
        writeRecordData(newRecord, value);
        addEntryToIndex(key, newRecord, getNumRecords());
	}

    private int payloadLength(int raw) {
        int len = raw + 4; // for length prefix
        if (!disableCrc32) {
            len += 8; // for crc32 long
        }
        return len;
    }

    /*
     * Updates an existing record. If the new contents do not fit in the
     * original record, then the update is handled by inserting the data
     */
    @Synchronized
    public void updateRecord(byte[] key, byte[] value) throws IOException {

        val updateMeHeader = keyToRecordHeader(key);
        val capacity = updateMeHeader.getDataCapacity();

        val recordIsSameSize = value.length == capacity;
        val recordIsSmallerAndCrcEnabled = !disableCrc32 && value.length < capacity;

        // can update in place if the record is same size no matter whether CRC32 is enabled.
        // if record is smaller then we can only update in place if we have a CRC32 to validate which data length is valid
        if (recordIsSameSize || recordIsSmallerAndCrcEnabled) {
            // write with the backup crc so one of the two CRCs will be valid after a crash
            writeRecordHeaderToIndex(updateMeHeader);
            updateMeHeader.dataCount = value.length;
            updateFreeSpaceIndex(updateMeHeader);
            // write the main data
            writeRecordData(updateMeHeader, value);
            // write the header with the main CRC
            writeRecordHeaderToIndex(updateMeHeader);
            return;
        }

        // if last record expand or contract the file
        val endOfRecord = updateMeHeader.dataPointer + updateMeHeader.getDataCapacity();
        val fileLength = getFileLength();
        if (endOfRecord == fileLength) {
            updateMeHeader.dataCount = value.length;
            setFileLength(fileLength + (value.length - updateMeHeader.getDataCapacity()));
            updateMeHeader.setDataCapacity(value.length);
            updateFreeSpaceIndex(updateMeHeader);
            writeRecordData(updateMeHeader, value);
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
            writeRecordHeaderToIndex(newRecord);
            // nothing to do but lets subclasses do additional bookwork
            memIndex.put(keyOf(key), newRecord);
            if (previous != null) {
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

    /*
     * Reads the data for the record with the given key.
     */
    @Synchronized
    public byte[] readRecordData(byte[] key) throws IOException {
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
        logger.log(Level.FINEST,
                "<d fp:{0} len:{1} bytes:{2} ",
                new Object[]{header.dataPointer, len, print(lenBytes)});

        assert header.dataPointer + len < getFileLength():
                String.format("attempting to read up to %d beyond length of file %d",
                        (header.dataCount + len), getFileLength());

        // read the body
        byte[] buf = new byte[len];
        file.readFully(buf);

        if (!disableCrc32) {
            byte[] crcBytes = new byte[CRC32_LENGTH];
            file.readFully(crcBytes);
            val expectedCrc = (new DataInputStream(new ByteArrayInputStream(crcBytes))).readInt() & 0xffffffffL;
            CRC32 crc32 = new CRC32();
            crc32.update(buf, 0, buf.length);

            long actualCrc = crc32.getValue();

            logger.log(Level.FINEST,
                    "<d fp:{0} len:{1} crc:{2} data:{3} bytes:{4}",
                    new Object[]{header.dataPointer+4, len, actualCrc, bytesToString(buf), print(buf)});

            if (actualCrc != expectedCrc) {
                throw new IllegalStateException(String.format("CRC32 check failed expected %d got %d for data length %d with header %s",
                        expectedCrc, actualCrc, buf.length, header.toString()));
            }
        }

        return buf;
    }

    /*
     * Updates the contents of the given record. A RecordsFileException is
     * thrown if the new data does not fit in the space allocated to the record.
     * The header's data count is updated, but not written to the file.
     */
    private void writeRecordData(RecordHeader header, byte[] data)
            throws IOException {

        assert data.length <= header.getDataCapacity() : "Record data does not fit";
        header.dataCount = data.length;

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(header.dataCount);
        out.write(data);
        long crc = -1;
        if (!disableCrc32) {
            CRC32 crc32 = new CRC32();
            crc32.update(data, 0, data.length);
            int crcInt = (int) (crc32.getValue() & 0xFFFFFFFFL);
            out.writeInt(crcInt);
        }
        out.close();
        val payload = bout.toByteArray();
        file.seek(header.dataPointer);
        file.write(payload, 0, payload.length); // drop
        byte[] lenBytes = Arrays.copyOfRange(payload, 0, 4);
        val end = header.dataPointer+payload.length;
        logger.log(Level.FINEST, ">d fp:{0} len:{1} end:{3} bytes:{2}",
                new Object[]{header.dataPointer, payload.length, print(lenBytes), end });
        logger.log(Level.FINEST, ">d fp:{0} len:{1} end:{5} crc:{2} data:{3} bytes:{4}",
                new Object[]{header.dataPointer+4, payload.length, crc, bytesToString(payload), print(data), end});
    }

    /*
     * Deletes a record.
     */
    @Synchronized
    public void deleteRecord(byte[] key) throws IOException {

        RecordHeader delRec = keyToRecordHeader(key);
        int currentNumRecords = getNumRecords();
        deleteEntryFromIndex(key, delRec, currentNumRecords);

        if (getFileLength() == delRec.dataPointer + delRec.getDataCapacity()) {
            // shrink file since this is the last record in the file
            setFileLength(delRec.dataPointer);
            return;
        }

        RecordHeader previous = getRecordAt(delRec.dataPointer - 1);

        if (previous != null) {
            // append space of deleted record onto previous record
            previous.incrementDataCapacity(delRec.getDataCapacity());
            updateFreeSpaceIndex(previous);
            writeRecordHeaderToIndex(previous);
        } else {
            // make free space at the end of the index area
            writeDataStartPtrHeader(delRec.dataPointer + delRec.getDataCapacity());
        }
    }

    // Checks to see if there is space for and additional index entry. If
    // not, space is created by moving records to the end of the file.
    private void ensureIndexSpace(int requiredNumRecords)
            throws IOException {
        long endIndexPtr = indexPositionToKeyFp(requiredNumRecords);
        if (isEmpty() && endIndexPtr > getFileLength()) {
            setFileLength(endIndexPtr);
            dataStartPtr = endIndexPtr;
            writeDataStartPtrHeader(dataStartPtr);
            return;
        }
        // move records to the back. if PAD_DATA_TO_KEY_LENGTH=true this should only move one record
        while (endIndexPtr > dataStartPtr) {
            RecordHeader first = getRecordAt(dataStartPtr);
			assert first != null;
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

    @SneakyThrows
    @Synchronized
    public void logAll(Level level, boolean disableCrc32) {
        val oldDisableCdc32 = this.disableCrc32;
        try {
            this.disableCrc32 = disableCrc32;
            logger.log(level, String.format("Records=%s, FileLength=%s, DataPointer=%s", getNumRecords(), getFileLength(), dataStartPtr));
            for (int index = 0; index < getNumRecords(); index++) {
                final RecordHeader header = readRecordHeaderFromIndex(index);
                final byte[] bk = readKeyFromIndex(index);
                final String k = keyOf(bk);
                logger.log(level, String.format("%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                        index,
                        k,
                        header.indexPosition,
                        header.getDataCapacity(),
                        header.dataCount,
                        header.dataPointer,
                        header.crc32
                ));
                final byte[] data = readRecordData(bk);

                String d = bytesToString(data);
                logger.log(level, String.format("%d data  len=%d data=%s", index, data.length, d));
            }
        } finally {
            this.disableCrc32 = oldDisableCdc32;
        }
    }

    public int size() {
        return getNumRecords();
    }
}
