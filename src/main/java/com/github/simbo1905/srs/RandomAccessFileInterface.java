package com.github.simbo1905.srs;

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

	public abstract int hashCode();

	public abstract boolean equals(Object obj);

	public abstract String toString();

	public abstract int read(byte[] b) throws IOException;

	public abstract void readFully(byte[] b) throws IOException;

	public abstract void write(int b) throws IOException;

	public abstract void write(byte[] b) throws IOException;

	public abstract void write(byte[] b, int off, int len) throws IOException;

	public abstract void seek(long pos) throws IOException;

	public abstract long length() throws IOException;

	public abstract void setLength(long newLength) throws IOException;

	public abstract void close() throws IOException;

	public abstract byte readByte() throws IOException;

	public abstract int readInt() throws IOException;

	public abstract long readLong() throws IOException;

	public abstract void writeInt(int v) throws IOException;

	public abstract void writeLong(long v) throws IOException;
}
