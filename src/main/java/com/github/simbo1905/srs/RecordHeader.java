package com.github.simbo1905.srs;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.zip.CRC32;

import static com.github.simbo1905.srs.FileRecordStore.RECORD_HEADER_LENGTH;
import static com.github.simbo1905.srs.FileRecordStore.print;

@ToString
@EqualsAndHashCode
class RecordHeader {

	/*
	 * File pointer to the first byte of record data (8 bytes).
	 */
	protected long dataPointer;

	/*
	 * Actual number of bytes of data held in this record (4 bytes).
	 */
	protected int dataCount;

	/*
	 * Number of bytes of data that this record can hold (4 bytes).
	 */
	private int dataCapacity;

	public int getDataCapacity() {
		return dataCapacity;
	}

	public void setDataCapacity(int dataCapacity) {
		this.dataCapacity = dataCapacity;
	}

	/*
	 * Indicates this header's position in the file index.
	 */
	protected int indexPosition;

	long crc32 = -1;

	protected RecordHeader(){}

	protected RecordHeader(long dataPointer, int dataCapacity) {
		this.dataPointer = dataPointer;
		this.dataCapacity = dataCapacity;
		this.dataCount = -1;
	}

	protected void setIndexPosition(int indexPosition) {
		this.indexPosition = indexPosition;
	}

	protected int getFreeSpace(boolean disableCrc32) {
		int len = dataCount + 4; // for length prefix
		if(!disableCrc32) {
			len += 8;
		}
		return dataCapacity - len;
	}

	/*
	 * Read as a single operation to avoid corruption
	 */
	protected void read(RandomAccessFileInterface in) throws IOException {
		byte[] header = new byte[RECORD_HEADER_LENGTH];
		val fp = in.getFilePointer();
		in.readFully(header);
		FileRecordStore.logger.log(Level.FINEST, "<h fp:{0} len:{1} bytes:{2}", new Object[]{fp, header.length, print(header) });

		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.put(header);
		buffer.flip();

		dataPointer = buffer.getLong();
		dataCapacity = buffer.getInt();
		dataCount = buffer.getInt();
		crc32 = buffer.getLong();

		val array = buffer.array();
		CRC32 crc = new CRC32();
		crc.update(array, 0, 8 + 4  + 4);
		val crc32expected = crc.getValue();
		if( crc32 != crc32expected) {
			throw new IllegalStateException(String.format("invalid header CRC32 expected %d for %s", crc32expected, this));
		}
	}

	/*
	 * in order to improve the likelihood of not corrupting the header write as
	 * a single operation
	 */
	protected void write(RandomAccessFileInterface out) throws IOException {
		if( dataCount < 0) {
			throw new IllegalStateException("dataCount has not been initialized "+this.toString());
		}
		val fp = out.getFilePointer();
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.putLong(dataPointer);
		buffer.putInt(dataCapacity);
		buffer.putInt(dataCount);
		val array = buffer.array();
		CRC32 crc = new CRC32();
		crc.update(array, 0, 8 + 4  + 4);
		crc32 = crc.getValue();
		buffer.putLong(crc32);
		out.write(buffer.array(), 0, RECORD_HEADER_LENGTH);
		FileRecordStore.logger.log(Level.FINEST, ">h fp:{0} len:{1} bytes:{2}", new Object[]{fp, array.length, print(array) });
	}

	protected static RecordHeader readHeader(RandomAccessFileInterface in) throws IOException {
		RecordHeader r = new RecordHeader();
		r.read(in);
		return r;
	}

	/*
	 * Returns a new record header which occupies the free space of this record.
	 * Shrinks this record size by the size of its free space.
	 */
	protected RecordHeader split(boolean disableCrc32, int padding) {
		long newFp = dataPointer + dataCount + padding;
		RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace(disableCrc32));
		dataCapacity = dataCount;
		return newRecord;
	}

	public void incrementDataCapacity(int dataCapacity) {
		this.dataCapacity += dataCapacity;
	}
}
