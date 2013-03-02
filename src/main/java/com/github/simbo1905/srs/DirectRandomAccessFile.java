package com.github.simbo1905.srs;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * easier to mock final native class by wrapping it in an interface
 */
final public class DirectRandomAccessFile implements RandomAccessFileInterface {

	final private RandomAccessFile randomAccessFile;

	public int hashCode() {
		return randomAccessFile.hashCode();
	}

	public boolean equals(Object obj) {
		return randomAccessFile.equals(obj);
	}

	public String toString() {
		return randomAccessFile.toString();
	}

	public final FileDescriptor getFD() throws IOException {
		return randomAccessFile.getFD();
	}

	public final FileChannel getChannel() {
		return randomAccessFile.getChannel();
	}

	public int read() throws IOException {
		return randomAccessFile.read();
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return randomAccessFile.read(b, off, len);
	}

	public int read(byte[] b) throws IOException {
		return randomAccessFile.read(b);
	}

	public final void readFully(byte[] b) throws IOException {
		randomAccessFile.readFully(b);
	}

	public final void readFully(byte[] b, int off, int len) throws IOException {
		randomAccessFile.readFully(b, off, len);
	}

	public int skipBytes(int n) throws IOException {
		return randomAccessFile.skipBytes(n);
	}

	public void write(int b) throws IOException {
		randomAccessFile.write(b);
	}

	public void write(byte[] b) throws IOException {
		randomAccessFile.write(b);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		randomAccessFile.write(b, off, len);
	}

	public long getFilePointer() throws IOException {
		return randomAccessFile.getFilePointer();
	}

	public void seek(long pos) throws IOException {
		randomAccessFile.seek(pos);
	}

	public long length() throws IOException {
		return randomAccessFile.length();
	}

	public void setLength(long newLength) throws IOException {
		randomAccessFile.setLength(newLength);
	}

	public void close() throws IOException {
		randomAccessFile.close();
	}

	public final boolean readBoolean() throws IOException {
		return randomAccessFile.readBoolean();
	}

	public final byte readByte() throws IOException {
		return randomAccessFile.readByte();
	}

	public final int readUnsignedByte() throws IOException {
		return randomAccessFile.readUnsignedByte();
	}

	public final short readShort() throws IOException {
		return randomAccessFile.readShort();
	}

	public final int readUnsignedShort() throws IOException {
		return randomAccessFile.readUnsignedShort();
	}

	public final char readChar() throws IOException {
		return randomAccessFile.readChar();
	}

	public final int readInt() throws IOException {
		return randomAccessFile.readInt();
	}

	public final long readLong() throws IOException {
		return randomAccessFile.readLong();
	}

	public final float readFloat() throws IOException {
		return randomAccessFile.readFloat();
	}

	public final double readDouble() throws IOException {
		return randomAccessFile.readDouble();
	}

	public final String readLine() throws IOException {
		return randomAccessFile.readLine();
	}

	public final String readUTF() throws IOException {
		return randomAccessFile.readUTF();
	}

	public final void writeBoolean(boolean v) throws IOException {
		randomAccessFile.writeBoolean(v);
	}

	public final void writeByte(int v) throws IOException {
		randomAccessFile.writeByte(v);
	}

	public final void writeShort(int v) throws IOException {
		randomAccessFile.writeShort(v);
	}

	public final void writeChar(int v) throws IOException {
		randomAccessFile.writeChar(v);
	}

	public final void writeInt(int v) throws IOException {
		randomAccessFile.writeInt(v);
	}

	public final void writeLong(long v) throws IOException {
		randomAccessFile.writeLong(v);
	}

	public final void writeFloat(float v) throws IOException {
		randomAccessFile.writeFloat(v);
	}

	public final void writeDouble(double v) throws IOException {
		randomAccessFile.writeDouble(v);
	}

	public final void writeBytes(String s) throws IOException {
		randomAccessFile.writeBytes(s);
	}

	public final void writeChars(String s) throws IOException {
		randomAccessFile.writeChars(s);
	}

	public final void writeUTF(String str) throws IOException {
		randomAccessFile.writeUTF(str);
	}

	public DirectRandomAccessFile(RandomAccessFile randomAccessFile) {
		this.randomAccessFile = randomAccessFile;
	}


}
