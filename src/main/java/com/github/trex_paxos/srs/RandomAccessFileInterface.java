package com.github.trex_paxos.srs;

import java.io.IOException;

/*
 * easier to mock final native class by wrapping it in an interface
 */
interface RandomAccessFileInterface {

	/**
	 * Forces the file data to be flushed. This won't use flags to flush file meta-data.
	 * @throws IOException
	 */
	void fsync() throws IOException;

	long getFilePointer() throws IOException;

	int hashCode();

	boolean equals(Object obj);

	String toString();

	int read(byte[] b) throws IOException;

	void readFully(byte[] b) throws IOException;

	void write(int b) throws IOException;

	void write(byte[] b) throws IOException;

	void write(byte[] b, int off, int len) throws IOException;

	void seek(long pos) throws IOException;

	long length() throws IOException;

	void setLength(long newLength) throws IOException;

	void close() throws IOException;

	byte readByte() throws IOException;

	int readInt() throws IOException;

	long readLong() throws IOException;

	void writeInt(int v) throws IOException;

	void writeLong(long v) throws IOException;
}
