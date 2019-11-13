package com.github.simbo1905.srs;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.github.simbo1905.srs.FileRecordStore.RECORD_HEADER_LENGTH;

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

	protected RecordHeader(RecordHeader copyMe) {
		this.dataPointer = copyMe.dataPointer;
		this.dataCount = copyMe.dataCount;
		this.dataCapacity = copyMe.dataCapacity;
		this.indexPosition = copyMe.indexPosition;
		this.crc32 = copyMe.crc32;
	}

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
		in.readFully(header);
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.put(header);
		buffer.flip();

		dataPointer = buffer.getLong();
		dataCapacity = buffer.getInt();
		dataCount = buffer.getInt();
		this.crc32 = buffer.getLong();
	}

	/*
	 * in order to improve the likelihood of not corrupting the header write as
	 * a single operation
	 */
	protected void write(RandomAccessFileInterface out) throws IOException {
		if( dataCount < 0) {
			throw new IllegalStateException("dataCount has not been initialized "+this.toString());
		}
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.putLong(dataPointer);
		buffer.putInt(dataCapacity);
		buffer.putInt(dataCount);
		buffer.putLong(crc32);
		out.write(buffer.array(), 0, RECORD_HEADER_LENGTH);
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

	public RecordHeader move(long fp) {
		RecordHeader moved = new RecordHeader(this);
		moved.dataPointer = fp;
		return moved;
	}

	public void incrementDataCapacity(int dataCapacity) {
		this.dataCapacity += dataCapacity;
	}
}
