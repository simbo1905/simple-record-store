package com.github.simbo1905.srs;

import lombok.EqualsAndHashCode;
import lombok.ToString;

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
	int dataCapacity;

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
