package com.github.simbo1905.srs;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.github.simbo1905.srs.BaseRecordStore.RECORD_HEADER_LENGTH;

@ToString
@EqualsAndHashCode
public class RecordHeader {

	/*
	 * File pointer to the first byte of record data (8 bytes).
	 */
	protected long dataPointer;

	/*
	 * Actual number of bytes of data held in this record (4 bytes).
	 */
	protected int dataCount;

	protected int dataCountTmp;

	public int getDataCountTmp() {
		return dataCountTmp;
	}

	public void setDataCountTmp(int dataCountTmp) {
		this.dataCountTmp = dataCountTmp;
	}

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

	protected Long crc32 = Long.valueOf(-1);

	public void setCrc32(long crc32) {
		this.crc32 = crc32;
	}

	protected Long crc32tmp = Long.valueOf(-1);

	public void setTempCrc32(long crc32tmp) {
		this.crc32tmp = crc32tmp;
	}

	protected RecordHeader(){}

	protected RecordHeader(RecordHeader copyMe) {
		this.dataPointer = copyMe.dataPointer;
		this.dataCount = copyMe.dataCount;
		this.dataCapacity = copyMe.dataCapacity;
		this.indexPosition = copyMe.indexPosition;
		this.setCrc32(copyMe.crc32.longValue());
		this.setTempCrc32(copyMe.crc32tmp.longValue());
		this.setDataCountTmp(copyMe.getDataCountTmp());
	}

	protected RecordHeader(long dataPointer, int dataCapacity) {
		this.dataPointer = dataPointer;
		this.dataCapacity = dataCapacity;
		this.dataCount = -1;
	}

	protected void setIndexPosition(int indexPosition) {
		this.indexPosition = indexPosition;
	}

	protected int getFreeSpace() {
		return dataCapacity - dataCount;
	}

	/*
	 * Read as a single operation to avoid corruption
	 */
	protected void read(DataInput in) throws IOException {
		byte[] header = new byte[RECORD_HEADER_LENGTH];
		in.readFully(header);
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.put(header);
		buffer.flip();

		dataPointer = buffer.getLong();
		dataCapacity = buffer.getInt();
		dataCount = buffer.getInt();
		long crc32 = buffer.getLong();
		this.setCrc32(crc32);
		long crc32tmp = buffer.getLong();
		this.setTempCrc32(crc32tmp);
		int dataCountTmp = buffer.getInt();
		this.setDataCountTmp(dataCountTmp);
	}

	/*
	 * in order to improve the likelihood of not corrupting the header write as
	 * a single operation
	 */
	protected void write(DataOutput out) throws IOException {
		if( dataCount < 0) {
			throw new IllegalStateException("dataCount has not been initialized "+this.toString());
		}
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
		buffer.putLong(dataPointer);
		buffer.putInt(dataCapacity);
		buffer.putInt(dataCount);
		buffer.putLong(crc32.longValue());
		buffer.putLong(crc32tmp.longValue());
		buffer.putInt(dataCountTmp);
		out.write(buffer.array(), 0, RECORD_HEADER_LENGTH);
	}

	protected static RecordHeader readHeader(DataInput in) throws IOException {
		RecordHeader r = new RecordHeader();
		r.read(in);
		return r;
	}

	/*
	 * Returns a new record header which occupies the free space of this record.
	 * Shrinks this record size by the size of its free space.
	 */
	protected RecordHeader split() {
		long newFp = dataPointer + (long) dataCount;
		RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace());
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
