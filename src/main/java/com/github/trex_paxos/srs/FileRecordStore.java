package com.github.trex_paxos.srs;

import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.val;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static java.util.Optional.of;

public class FileRecordStore implements AutoCloseable {

    private final static Logger logger = Logger.getLogger(FileRecordStore.class.getName());

    // Number of bytes in the record header.
    private static final int RECORD_HEADER_LENGTH = 20;

    // File index to the num records header.
    private static final long NUM_RECORDS_HEADER_LOCATION = 1;

    // File index to the start of the data region beyond the index region
    private static final long DATA_START_HEADER_LOCATION = 5;

    /**
     * Total length in bytes of the global database headers:
     * 1 byte is the key length that the file was created with. cannot be changed you can copy to a new store with a new length.
     * 4 byte int is the number of records
     * 8 byte long is the index to the start of the data region
     */
    private static final int FILE_HEADERS_REGION_LENGTH = 13;

    // this can be overridden up to 2^8 - 4
    public static final int DEFAULT_MAX_KEY_LENGTH = 64;

    // its an unsigned 32 int
    private static final int CRC32_LENGTH = 4;

    public static final int MAX_KEY_LENGTH_THEORETICAL = Double.valueOf(Math.pow(2, 8)).intValue() - Integer.BYTES;

    // The length of a key in the index. This is an arbitrary size. UUID strings are only 36.
    // A base64 sha245 would be about 42 bytes. So you can create a 64 byte surrogate key out of anything
    // unique about your data. You can also set it to be a max of 248 bytes. Note we store binary keys with a header byte
    // and a CRC32 which is an unsigned 32 stored as a long.
    public final int maxKeyLength;

    private static final boolean PAD_DATA_TO_KEY_LENGTH = getPadDataToKeyLengthOrDefaultTrue();

    // The total length of one index entry - the key length plus the record
    // header length and the CRC of the key which is an unsigned 32 bits.
    private final int indexEntryLength;

    /*default*/ RandomAccessFileInterface file;

    /*
     * Hashtable which holds the in-memory index. For efficiency, the entire
     * index is cached in memory. The hashtable wraps the byte[] key as a String
     * as you cannot use a raw byte[] as a key and Java doesn't have extension methods yet.
     */
    private Map<ByteSequence, RecordHeader> memIndex;


    /**
     * TreeMap of headers by file index
     */
    private TreeMap<Long, RecordHeader> positionIndex;


    private Comparator<RecordHeader> compareRecordHeaderByFreeSpace = new Comparator<RecordHeader>() {
        @Override
        public int compare(RecordHeader o1, RecordHeader o2) {
            return o1.getFreeSpace(true) - o2.getFreeSpace(true);
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
     * @param initialSize an optimisation to preallocate the file length in bytes.
     */
    public FileRecordStore(String dbPath, int initialSize) throws IOException {
        this(dbPath, initialSize, getMaxKeyLengthOrDefault(), false);
    }

    /*
     * Creates a new database file. The initialSize parameter determines the
     * amount of space which is allocated for the index. The index can grow
     * dynamically, but the parameter is provide to increase efficiency.
     * @param dbPath the location on disk to create the storage file.
     * @param initialSize an optimisation to preallocate the file length in bytes.
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped it
     * has a CRC check built in so you can safely disable here. Writes of keys and record header data will be unaffected.
     */
    public FileRecordStore(String dbPath, int initialSize, int maxKeyLength, boolean disableCrc32) throws IOException {
        logger.log(Level.FINE, () -> String.format("creating %s, %d, %d, %s, %s", dbPath, initialSize, maxKeyLength, Boolean.valueOf(disableCrc32), this));
        this.disableCrc32 = disableCrc32;
        this.maxKeyLength = maxKeyLength;
        this.indexEntryLength = maxKeyLength + Integer.BYTES
                + RECORD_HEADER_LENGTH;

        File f = new File(dbPath);
        if (f.exists()) {
            throw new IllegalArgumentException("Database already exits: " + dbPath);
        }
        file = new DirectRandomAccessFile(new RandomAccessFile(f, "rw"));
        dataStartPtr = initialSize; // set data region start index
        setFileLength(dataStartPtr);
        writeNumRecordsHeader(0);
        writeKeyLengthHeader();
        writeDataStartPtrHeader(dataStartPtr);
        memIndex = new HashMap<>(initialSize);
        positionIndex = new TreeMap<>();
    }

    /*
     * Opens an existing database and initializes the in-memory index.
     * @param dbPath the location of the database file on disk to open.
     * @param accessFlags the access flags supported by the java java.io.RandomAccessFile e.g. "r" or "rw"
     */
    public FileRecordStore(String dbPath, String accessFlags) throws IOException {
        this(dbPath, accessFlags, false);
    }

    /*
     * Opens an existing database and initializes the in-memory index.
     * @param dbPath the location of the database file on disk to open.
     * @param accessFlags the access flags supported by the java java.io.RandomAccessFile e.g. "r" or "rw"
     * @param disableCrc32 whether to disable explicit CRC32 of record data. If you are writing data you zipped that
     * will have a CRC check built in so you can safely disable here.
     */
    public FileRecordStore(String dbPath, String accessFlags, boolean disableCrc32) throws IOException {
        logger.log(Level.FINE, () -> String.format("opening %s, %s, %s, %s", dbPath, accessFlags, Boolean.valueOf(disableCrc32), this));
        File f = new File(dbPath);
        if (!f.exists()) {
            throw new IllegalArgumentException("Database not found: " + dbPath);
        }
        file = new DirectRandomAccessFile(new RandomAccessFile(f, accessFlags));
        this.disableCrc32 = disableCrc32;
        // load the max key length from first byte
        file.seek(0);
        val b = file.readByte();
        this.maxKeyLength = b & 0xFF;
        this.indexEntryLength = maxKeyLength + Integer.BYTES
                + RECORD_HEADER_LENGTH;

        dataStartPtr = readDataStartHeader();
        int numRecords = readNumRecordsHeader();
        memIndex = new HashMap<>(numRecords);
        positionIndex = new TreeMap<>();

        for (int i = 0; i < numRecords; i++) {
            val key = readKeyFromIndex(i);
            RecordHeader header = readRecordHeaderFromIndex(i);
            logger.log(Level.FINEST, () -> String.format("header:%s, key:%s", header.toString(), print(key.bytes)));
            header.setIndexPosition(i);
            val duplicate = memIndex.put(key, header);
            // duplicates are due to crashes where the later write can replace the earlier one
            if (duplicate != null) positionIndex.remove(duplicate.dataPointer);
            positionIndex.put(header.dataPointer, header);
            assert memIndex.size() == positionIndex.size() :
                    String.format("memIndex:%d positionIndex:%d", memIndex.size(), positionIndex.size());
        }
    }

    public static String MAX_KEY_LENGTH_PROPERTY = "MAX_KEY_LENGTH";

    static int getMaxKeyLengthOrDefault() {
        final String key = String.format("%s.%s", FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY);
        String keyLength = System.getenv(key) == null
                ? Integer.valueOf(DEFAULT_MAX_KEY_LENGTH).toString()
                : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Integer.parseInt(keyLength);
    }

    private static boolean getPadDataToKeyLengthOrDefaultTrue() {
        final String key = String.format("%s.%s", FileRecordStore.class.getName(), MAX_KEY_LENGTH_PROPERTY);
        String keyLength = System.getenv(key) == null ? Boolean.valueOf(true).toString() : System.getenv(key);
        keyLength = System.getProperty(key, keyLength);
        return Boolean.parseBoolean(keyLength);
    }

    private static String print(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (byte b : bytes) {
            sb.append(String.format("0x%02X ", b));
        }
        sb.append("]");
        return sb.toString();
    }

    @SneakyThrows
    static String print(File f) {
        RandomAccessFile file = new RandomAccessFile(f.getAbsolutePath(), "r");
        val len = file.length();
        assert len < Integer.MAX_VALUE;
        val bytes = new byte[(int) len];
        file.readFully(bytes);
        return print(bytes);
    }

    private int getDataLengthPadded(int dataLength) {
        return (PAD_DATA_TO_KEY_LENGTH) ? Math.max(indexEntryLength, dataLength) : dataLength;
    }

    @Synchronized
    private Set<ByteSequence> snapshotKeys() {
        return new HashSet(memIndex.keySet());
    }

    /**
     * This generates a defensive copy of all the keys in a thread safe manner.
     */
    public Iterable<ByteSequence> keys() {
        val snapshot = snapshotKeys();
        return snapshot.stream().map(ByteSequence::copy).collect(Collectors.toSet());
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
    public boolean recordExists(ByteSequence key) {
        return memIndex.containsKey(key);
    }

    /*
     * Maps a key to a record header by looking it up in the in-memory index.
     */
    private RecordHeader keyToRecordHeader(ByteSequence key) {
        RecordHeader h = memIndex.get(key);
        if (h == null) {
            throw new IllegalArgumentException(String.format("Key not found %s '%s'", print(key.bytes), new String(key.bytes)));
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
    private RecordHeader allocateRecord(int dataLength)
            throws IOException {

        // we needs space for the length int and the optional long crc32
        int payloadLength = payloadLength(dataLength);

        // we pad the record to be at least the size of a header to avoid moving many values to expand the the index
        int dataLengthPadded = getDataLengthPadded(payloadLength);

        // FIFO deletes cause free space after the index.
        long dataStart = readDataStartHeader();
        long endIndexPtr = indexPositionToKeyFp(getNumRecords());
        // we prefer speed overs space so we leave space for the header for this insert plus one for future use
        long available = dataStart - endIndexPtr - (2 * indexEntryLength);

        RecordHeader newRecord = null;

        if (dataLengthPadded <= available) {
            newRecord = new RecordHeader(dataStart - dataLengthPadded, dataLengthPadded);
            dataStartPtr = dataStart - dataLengthPadded;
            writeDataStartPtrHeader(dataStartPtr);
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
     * RecordHeader which is returned.
     */
    private Optional<RecordHeader> getRecordAt(long targetFp) {
        val floor = positionIndex.floorEntry(targetFp);
        Optional<Map.Entry<Long, RecordHeader>> before = (floor != null) ? of(floor) : Optional.empty();
        return before.map(entry -> {
            val rh = entry.getValue();
            if (targetFp >= rh.dataPointer
                    && targetFp < rh.dataPointer + (long) rh.getDataCapacity()) {
                return rh;
            } else {
                return null;
            }
        });
    }

    /*
     * Closes the database.
     */
    @Synchronized
    public void close() throws IOException {
        logger.log(Level.FINE, () -> String.format("closed called on %s", this));
        try {
            try {
                if (file != null) file.fsync();
                if (file != null) file.close();
            } finally {
                file = null;
            }
        } finally {
            if (memIndex != null) memIndex.clear();
            memIndex = null;
            if (positionIndex != null) positionIndex.clear();
            positionIndex = null;
            if (freeMap != null) freeMap.clear();
            freeMap = null;
        }
    }

    /*
     * Adds the new record to the in-memory index and calls the super class add
     * the index entry to the file.
     */
    private void addEntryToIndex(ByteSequence key, RecordHeader newRecord,
                                 int currentNumRecords) throws IOException {
        if (key.length() > maxKeyLength) {
            throw new IllegalArgumentException(
                    String.format("Key of len %d is larger than permitted max size of %d bytes. You can increase this to %d using env var or system property %s.MAX_KEY_LENGTH",
                            key.length(),
                            maxKeyLength,
                            MAX_KEY_LENGTH_THEORETICAL,
                            FileRecordStore.class.getName()));
        }

        writeKeyToIndex(key, currentNumRecords);

        file.seek(indexPositionToRecordHeaderFp(currentNumRecords));
        write(newRecord, file);
        newRecord.setIndexPosition(currentNumRecords);
        writeNumRecordsHeader(currentNumRecords + 1);

        logger.log(Level.FINEST, ()->String.format("before maps: %s | %s", memIndex.toString(), positionIndex.toString()));

        val duplicate = memIndex.put(key, newRecord);
        if( duplicate != null) positionIndex.remove(duplicate.dataPointer);
        positionIndex.put(newRecord.dataPointer, newRecord);

        logger.log(Level.FINEST, ()->String.format("after maps: %s | %s", memIndex.toString(), positionIndex.toString()));

        assert memIndex.size() == positionIndex.size() :
                String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
    }

    private void write(RecordHeader rh, RandomAccessFileInterface out) throws IOException {
        if (rh.dataCount < 0) {
            throw new IllegalStateException("dataCount has not been initialized " + this.toString());
        }
        val fp = out.getFilePointer();
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
        buffer.putLong(rh.dataPointer);
        buffer.putInt(rh.dataCapacity);
        buffer.putInt(rh.dataCount);
        val array = buffer.array();
        CRC32 crc = new CRC32();
        crc.update(array, 0, 8 + 4 + 4);
        rh.crc32 = crc.getValue();
        int crc32int = (int) (rh.crc32 & 0xFFFFFFFFL);
        buffer.putInt(crc32int);
        out.write(buffer.array(), 0, RECORD_HEADER_LENGTH);

        logger.log(Level.FINEST, () -> String.format(">h fp:%d idx:%d len:%d end:%d bytes:%s",
                fp, rh.indexPosition, array.length, fp + array.length, print(array)));
    }

    private static RecordHeader read(int index, RandomAccessFileInterface in) throws IOException {
        byte[] header = new byte[RECORD_HEADER_LENGTH];
        val fp = in.getFilePointer();
        in.readFully(header);

        logger.log(Level.FINEST, () -> String.format("<h fp:%d idx:%d len:%d bytes:%s",
                fp, index, header.length, print(header)));

        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
        buffer.put(header);
        buffer.flip();

        RecordHeader rh = new RecordHeader(buffer.getLong(), buffer.getInt());
        rh.dataCount = buffer.getInt();
        rh.crc32 = buffer.getInt() & 0xFFFFFFFFL;

        val array = buffer.array();
        CRC32 crc = new CRC32();
        crc.update(array, 0, 8 + 4 + 4);
        long crc32expected = crc.getValue();
        if (rh.crc32 != crc32expected) {
            throw new IllegalStateException(String.format("invalid header CRC32 expected %d for %s", crc32expected, rh));
        }
        return rh;
    }

    /*
     * Removes the record from the index. Replaces the target with the entry at
     * the end of the index.
     */
    private void deleteEntryFromIndex(RecordHeader header,
                                      int currentNumRecords) throws IOException {
        if (header.indexPosition != currentNumRecords - 1) {
            val lastKey = readKeyFromIndex(currentNumRecords - 1);
            RecordHeader last = keyToRecordHeader(lastKey);
            last.setIndexPosition(header.indexPosition);

            writeKeyToIndex(lastKey, last.indexPosition);

            file.seek(this.indexPositionToRecordHeaderFp(last.indexPosition));
            write(last, file);
        }
        writeNumRecordsHeader(currentNumRecords - 1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("no file passed");
            System.exit(1);
        }
        if (args.length < 2) {
            System.err.println("no comamnd passed");
            System.exit(2);
        }
        final String filename = args[0];
        final String command = args[1];
        logger.info("Reading from " + filename);

        boolean disableCrc32 = false;
        dumpFile(Level.INFO, filename, disableCrc32);
    }

    static void dumpFile(Level level, String filename, boolean disableCrc) throws IOException {
        final FileRecordStore recordFile = new FileRecordStore(filename, "r", disableCrc);
        val len = recordFile.getFileLength();
        logger.log(level, () -> String.format("Records=%s, FileLength=%s, DataPointer=%s", recordFile.getNumRecords(), len, recordFile.dataStartPtr));
        for (int index = 0; index < recordFile.getNumRecords(); index++) {
            final RecordHeader header = recordFile.readRecordHeaderFromIndex(index);
            val bk = recordFile.readKeyFromIndex(index);
            final String k = new String(bk.bytes);
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

            String d = new String(data);
            int finalIndex = index;
            logger.log(level, () -> String.format("%d data  len=%d data=%s", finalIndex, data.length, d));
        }
    }

    @Synchronized
    public void fsync() throws IOException {
        logger.log(Level.FINE, () -> String.format("fsync called on %s", this));
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
     * Writes the max key length to the beginning of the file
     */
    private void writeKeyLengthHeader()
            throws IOException {
        file.seek(0);
        val keyLength = (byte) maxKeyLength;
        file.write(keyLength);
    }

    /*
     * Returns a file pointer in the index pointing to the first byte in the key
     * located at the given index position.
     */
    private long indexPositionToKeyFp(int pos) {
        return FILE_HEADERS_REGION_LENGTH + (indexEntryLength * pos);
    }

    /*
     * Returns a file pointer in the index pointing to the first byte in the
     * record pointer located at the given index position.
     */
    private long indexPositionToRecordHeaderFp(int pos) {
        return indexPositionToKeyFp(pos) + maxKeyLength;
    }

    private void writeKeyToIndex(ByteSequence key, int index) throws IOException {
        val len = (byte) key.length();
        val writeLen = (int) key.length() + 1 + CRC32_LENGTH;

        ByteBuffer buffer = ByteBuffer.allocate(writeLen);
        buffer.put(len);
        buffer.put(key.bytes, 0, (int) key.length());

        // compute crc from the backing array with what we have written
        val array = buffer.array();
        CRC32 crc = new CRC32();
        crc.update(array, 1, (int) key.length());
        int crc32 = (int) (crc.getValue() & 0xFFFFFFFFL);

        // add the crc which will write through to the backing array
        buffer.putInt(crc32);

        val fpk = indexPositionToKeyFp(index);
        file.seek(fpk);
        file.write(array, 0, writeLen);

        FileRecordStore.logger.log(Level.FINEST, () ->
                String.format(">k fp:%d idx:%d len:%d end:%d crc:%d key:%s bytes:%s",
                        fpk, index, len & 0xFF, fpk + (len & 0xFF), crc32, new String(key.bytes), print(key.bytes)));
    }

    /*
     * Reads the ith key from the index.
     */
    private ByteSequence readKeyFromIndex(int position) throws IOException {
        val fp = indexPositionToKeyFp(position);
        file.seek(fp);

        int len = file.readByte() & 0xFF; // interpret as unsigned byte https://stackoverflow.com/a/56052675/329496

        assert len <= maxKeyLength : String.format("%d > %d", len, maxKeyLength);

        byte[] key = new byte[len];
        file.read(key);

        byte[] crcBytes = new byte[CRC32_LENGTH];
        file.readFully(crcBytes);
        ByteBuffer buffer = ByteBuffer.allocate(CRC32_LENGTH);
        buffer.put(crcBytes);
        buffer.flip();
        long crc32expected = buffer.getInt() & 0xffffffffL; // https://stackoverflow.com/a/22938125/329496

        CRC32 crc = new CRC32();
        crc.update(key, 0, key.length);
        val crc32actual = crc.getValue();

        FileRecordStore.logger.log(Level.FINEST, () ->
                String.format("<k fp:%d idx:%d len:%d end:%d crc:%d key:%s bytes:%s",
                        fp, position, len, fp + len, crc32actual, new String(key), print(key)));

        if (crc32actual != crc32expected) {
            throw new IllegalStateException(
                    String.format("invalid key CRC32 expected %d and actual %s for len %d and fp %d found key %s with bytes %s",
                            crc32expected,
                            crc32actual,
                            len,
                            fp,
                            new String(key),
                            print(key)
                    ));
        }

        return ByteSequence.of(key);
    }

    /*
     * Reads the ith record header from the index.
     */
    private RecordHeader readRecordHeaderFromIndex(int index) throws IOException {
        file.seek(indexPositionToRecordHeaderFp(index));
        return read(index, file);
    }

    /*
     * Writes the ith record header to the index.
     */
    private void writeRecordHeaderToIndex(RecordHeader header)
            throws IOException {
        file.seek(indexPositionToRecordHeaderFp(header.indexPosition));
        write(header, file);
    }

    /*
     * Inserts a new record. It tries to insert into free space at the end of the index space, or free space between
     * records, then finally extends the file. If the file has been set to a large initial file it will initially all
     * be considered space at the end of the index space such that inserts will be prepended into the back of the
     * file. When there is no more space in the index area the file will be expanded and record(s) will be into the new
     * space to make space for heders.
     */
    @Synchronized
    public void insertRecord(ByteSequence key, byte[] value)
            throws IOException {
        logger.log(Level.FINE, () -> String.format("insertRecord value.len:%d key:%s ", value.length, print(key.bytes)));
        if (recordExists(key)) {
            throw new IllegalArgumentException("Key exists: " + key);
        }
        ensureIndexSpace(getNumRecords() + 1);
        RecordHeader newRecord = allocateRecord(payloadLength(value.length));
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
     * Updates an existing record. If the new contents do not fit in the original record then the update is handled
     * like an insert.
     */
    @Synchronized
    public void updateRecord(ByteSequence key, byte[] value) throws IOException {
        logger.log(Level.FINE, () -> String.format("updateRecord value.len:%d key:%s", value.length, print(key.bytes)));
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

        // perform a move. insert data to the end of the file then overwrite header.
        if (value.length > updateMeHeader.getDataCapacity()) {
            // allocate to next free space or expand the file
            RecordHeader newRecord = allocateRecord(value.length);
            // new record is expanded old record
            newRecord.dataCount = value.length;
            writeRecordData(newRecord, value);
            writeRecordHeaderToIndex(newRecord);
            memIndex.put(key, newRecord);
            positionIndex.remove(updateMeHeader.dataPointer);
            positionIndex.put(newRecord.dataPointer, newRecord);
            assert memIndex.size() == positionIndex.size() :
                    String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());

            // if there is a previous record add space to it
            val previousIndex = updateMeHeader.dataPointer - 1;
            val previousOptional = getRecordAt(previousIndex);

            if (previousOptional.isPresent()) {
                RecordHeader previous = previousOptional.get();
                // append space of deleted record onto previous record
                previous.incrementDataCapacity(updateMeHeader.getDataCapacity());
                updateFreeSpaceIndex(previous);
                writeRecordHeaderToIndex(previous);
            } else {
                // record free space at the end of the index area
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
    public byte[] readRecordData(ByteSequence key) throws IOException {
        logger.log(Level.FINE, () -> String.format("updateRecord key:%s", print(key.bytes)));
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

        logger.log(Level.FINEST, () ->
                String.format("<d fp:%d len:%d bytes:%s ",
                        header.dataPointer, len, print(lenBytes)));

        assert header.dataPointer + len < getFileLength() :
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

            logger.log(Level.FINEST, () ->
                    String.format("<d fp:%d len:%d crc:%d bytes:%s",
                            header.dataPointer + 4, len, actualCrc, print(buf)));

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

        logger.log(Level.FINEST, () -> String.format(">d fp:%d len:%d end:%d bytes:%s",
                header.dataPointer, payload.length, header.dataPointer + payload.length, print(lenBytes)));

        logger.log(Level.FINEST, () -> String.format(">d fp:%d len:%d end:%d crc:%d data:%s",
                header.dataPointer + 4, payload.length, header.dataPointer + payload.length, crc, print(data)));
    }

    /*
     * Deletes a record.
     */
    @Synchronized
    public void deleteRecord(ByteSequence key) throws IOException {
        logger.log(Level.FINE, () -> String.format("deleteRecord key:%s", print(key.bytes)));
        RecordHeader delRec = keyToRecordHeader(key);
        int currentNumRecords = getNumRecords();
        deleteEntryFromIndex(delRec, currentNumRecords);
        val memDeleted = memIndex.remove(key);
        assert delRec == memDeleted;
        val posDeleted = positionIndex.remove(delRec.dataPointer);
        assert delRec == posDeleted;
        assert memIndex.size() == positionIndex.size() :
                String.format("memIndex:%d, positionIndex:%d", memIndex.size(), positionIndex.size());
        freeMap.remove(delRec);

        if (getFileLength() == delRec.dataPointer + delRec.getDataCapacity()) {
            // shrink file since this is the last record in the file
            setFileLength(delRec.dataPointer);
            return;
        }

        val previousOptional = getRecordAt(delRec.dataPointer - 1);

        if (previousOptional.isPresent()) {
            // append space of deleted record onto previous record
            val previous = previousOptional.get();
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
            val firstOptional = getRecordAt(dataStartPtr);
            val first = firstOptional.get();
            positionIndex.remove(first.dataPointer);
            freeMap.remove(first);
            byte[] data = readRecordData(first);
            long fileLen = getFileLength();
            first.dataPointer = fileLen;
            int dataLength = payloadLength(data.length);
            int dataLengthPadded = getDataLengthPadded(dataLength);
            first.setDataCapacity(dataLengthPadded);
            setFileLength(fileLen + dataLengthPadded);
            writeRecordData(first, data);
            writeRecordHeaderToIndex(first);
            positionIndex.put(first.dataPointer, first);
            dataStartPtr = positionIndex.ceilingEntry(dataStartPtr).getValue().dataPointer;
            writeDataStartPtrHeader(dataStartPtr);
        }
    }

    @SneakyThrows
    @Synchronized
    public void logAll(Level level, boolean disableCrc32) {
        val oldDisableCdc32 = this.disableCrc32;
        try {
            this.disableCrc32 = disableCrc32;
            val len = getFileLength();
            logger.log(level, () -> String.format("Records=%s, FileLength=%s, DataPointer=%s", getNumRecords(), len, dataStartPtr));
            for (int index = 0; index < getNumRecords(); index++) {
                final RecordHeader header = readRecordHeaderFromIndex(index);
                val bk = readKeyFromIndex(index);
                final String k = new String(bk.bytes);
                int finalIndex = index;
                logger.log(level, () -> String.format("%d header Key=%s, indexPosition=%s, getDataCapacity()=%s, dataCount=%s, dataPointer=%s, crc32=%s",
                        finalIndex,
                        k,
                        header.indexPosition,
                        header.getDataCapacity(),
                        header.dataCount,
                        header.dataPointer,
                        header.crc32
                ));
                final byte[] data = readRecordData(bk);

                String d = new String(data);
                int finalIndex1 = index;
                logger.log(level, () -> String.format("%d data  len=%d data=%s", finalIndex1, data.length, d));
            }
        } finally {
            this.disableCrc32 = oldDisableCdc32;
        }
    }

    public int size() {
        return getNumRecords();
    }
}
