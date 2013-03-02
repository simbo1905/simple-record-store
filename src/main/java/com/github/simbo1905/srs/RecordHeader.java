package com.github.simbo1905.srs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordHeader {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataCapacity;
		result = prime * result + dataCount;
		result = prime * result + (int) (dataPointer ^ (dataPointer >>> 32));
		result = prime * result + indexPosition;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecordHeader other = (RecordHeader) obj;
		if (dataCapacity != other.dataCapacity)
			return false;
		if (dataCount != other.dataCount)
			return false;
		if (dataPointer != other.dataPointer)
			return false;
		if (indexPosition != other.indexPosition)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RecordHeader [dataPointer=" + dataPointer + ", dataCount="
				+ dataCount + ", dataCapacity=" + dataCapacity
				+ ", indexPosition=" + indexPosition + "]";
	}

	/**
	 * File pointer to the first byte of record data (8 bytes).
	 */
	protected long dataPointer;

	/**
	 * Actual number of bytes of data held in this record (4 bytes).
	 */
	protected int dataCount;

	/**
	 * Number of bytes of data that this record can hold (4 bytes).
	 */
	protected int dataCapacity;

	/**
	 * Indicates this header's position in the file index.
	 */
	protected int indexPosition;

	protected RecordHeader() {
	}

	protected RecordHeader(long dataPointer, int dataCapacity) {
		if (dataCapacity < 1) {
			throw new IllegalArgumentException("Bad record size: "
					+ dataCapacity);
		}
		this.dataPointer = dataPointer;
		this.dataCapacity = dataCapacity;
		this.dataCount = 0;
	}

	protected int getIndexPosition() {
		return indexPosition;
	}

	protected void setIndexPosition(int indexPosition) {
		this.indexPosition = indexPosition;
	}

	protected int getDataCapacity() {
		return dataCapacity;
	}

	protected int getFreeSpace() {
		return dataCapacity - dataCount;
	}

	/**
	 * Read as a single operation to avoid corruption
	 */
	protected void read(DataInput in) throws IOException {
		byte[] header = new byte[16];
		in.readFully(header);
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.put(header);
		buffer.flip();

		dataPointer = buffer.getLong();
		dataCapacity = buffer.getInt();
		dataCount = buffer.getInt();
	}

	/**
	 * in order to improve the likelihood of not corrupting the header write as
	 * a single operation
	 */
	protected void write(DataOutput out) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.putLong(dataPointer);
		buffer.putInt(dataCapacity);
		buffer.putInt(dataCount);
		out.write(buffer.array(), 0, 16);
	}

	protected static RecordHeader readHeader(DataInput in) throws IOException {
		RecordHeader r = new RecordHeader();
		r.read(in);
		return r;
	}

	/**
	 * Returns a new record header which occupies the free space of this record.
	 * Shrinks this record size by the size of its free space.
	 */
	protected RecordHeader split() throws RecordsFileException {
		long newFp = dataPointer + (long) dataCount;
		RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace());
		dataCapacity = dataCount;
		return newRecord;
	}

}
