package com.github.simbo1905.srs;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.github.simbo1905.srs.RandomAccessFileInterface;

public final class InterceptedRandomAccessFile implements RandomAccessFileInterface {
	final private RandomAccessFile file;
	final private WriteCallback wc;

	InterceptedRandomAccessFile(final RandomAccessFile file, final WriteCallback wc) {
		this.file = file;		
		this.wc = wc;
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#hashCode()
	 */
	@Override
	public int hashCode() {
		return file.hashCode();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return file.equals(obj);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#toString()
	 */
	@Override
	public String toString() {
		return file.toString();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#getFD()
	 */
	@Override
	public final FileDescriptor getFD() throws IOException {
		wc.onWrite();
		return file.getFD();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#getChannel()
	 */
	@Override
	public final FileChannel getChannel() {
		return file.getChannel();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#read()
	 */
	@Override
	public int read() throws IOException {
		wc.onWrite();
		return file.read();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#read(byte[], int, int)
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		wc.onWrite();
		return file.read(b, off, len);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#read(byte[])
	 */
	@Override
	public int read(byte[] b) throws IOException {
		wc.onWrite();
		return file.read(b);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readFully(byte[])
	 */
	@Override
	public final void readFully(byte[] b) throws IOException {
		wc.onWrite();
		file.readFully(b);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readFully(byte[], int, int)
	 */
	@Override
	public final void readFully(byte[] b, int off, int len) throws IOException {
		wc.onWrite();
		file.readFully(b, off, len);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#skipBytes(int)
	 */
	@Override
	public int skipBytes(int n) throws IOException {
		wc.onWrite();
		return file.skipBytes(n);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#write(int)
	 */
	@Override
	public void write(int b) throws IOException {
		wc.onWrite();
		file.write(b);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#write(byte[])
	 */
	@Override
	public void write(byte[] b) throws IOException {
		wc.onWrite();
		file.write(b);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#write(byte[], int, int)
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		wc.onWrite();
		file.write(b, off, len);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#getFilePointer()
	 */
	@Override
	public long getFilePointer() throws IOException {
		wc.onWrite();
		return file.getFilePointer();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#seek(long)
	 */
	@Override
	public void seek(long pos) throws IOException {
		wc.onWrite();
		file.seek(pos);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#length()
	 */
	@Override
	public long length() throws IOException {
		wc.onWrite();
		return file.length();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#setLength(long)
	 */
	@Override
	public void setLength(long newLength) throws IOException {
		wc.onWrite();
		file.setLength(newLength);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#close()
	 */
	@Override
	public void close() throws IOException {
		wc.onWrite();
		file.close();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readBoolean()
	 */
	@Override
	public final boolean readBoolean() throws IOException {
		wc.onWrite();
		return file.readBoolean();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readByte()
	 */
	@Override
	public final byte readByte() throws IOException {
		wc.onWrite();
		return file.readByte();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readUnsignedByte()
	 */
	@Override
	public final int readUnsignedByte() throws IOException {
		wc.onWrite();
		return file.readUnsignedByte();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readShort()
	 */
	@Override
	public final short readShort() throws IOException {
		wc.onWrite();
		return file.readShort();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readUnsignedShort()
	 */
	@Override
	public final int readUnsignedShort() throws IOException {
		wc.onWrite();
		return file.readUnsignedShort();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readChar()
	 */
	@Override
	public final char readChar() throws IOException {
		wc.onWrite();
		return file.readChar();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readInt()
	 */
	@Override
	public final int readInt() throws IOException {
		wc.onWrite();
		return file.readInt();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readLong()
	 */
	@Override
	public final long readLong() throws IOException {
		wc.onWrite();
		return file.readLong();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readFloat()
	 */
	@Override
	public final float readFloat() throws IOException {
		wc.onWrite();
		return file.readFloat();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readDouble()
	 */
	@Override
	public final double readDouble() throws IOException {
		wc.onWrite();
		return file.readDouble();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readLine()
	 */
	@Override
	public final String readLine() throws IOException {
		wc.onWrite();
		return file.readLine();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readUTF()
	 */
	@Override
	public final String readUTF() throws IOException {
		wc.onWrite();
		return file.readUTF();
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeBoolean(boolean)
	 */
	@Override
	public final void writeBoolean(boolean v) throws IOException {
		wc.onWrite();
		file.writeBoolean(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeByte(int)
	 */
	@Override
	public final void writeByte(int v) throws IOException {
		wc.onWrite();
		file.writeByte(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeShort(int)
	 */
	@Override
	public final void writeShort(int v) throws IOException {
		wc.onWrite();
		file.writeShort(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeChar(int)
	 */
	@Override
	public final void writeChar(int v) throws IOException {
		wc.onWrite();
		file.writeChar(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeInt(int)
	 */
	@Override
	public final void writeInt(int v) throws IOException {
		wc.onWrite();
		file.writeInt(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeLong(long)
	 */
	@Override
	public final void writeLong(long v) throws IOException {
		wc.onWrite();
		file.writeLong(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeFloat(float)
	 */
	@Override
	public final void writeFloat(float v) throws IOException {
		wc.onWrite();
		file.writeFloat(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeDouble(double)
	 */
	@Override
	public final void writeDouble(double v) throws IOException {
		wc.onWrite();
		file.writeDouble(v);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeBytes(java.lang.String)
	 */
	@Override
	public final void writeBytes(String s) throws IOException {
		wc.onWrite();
		file.writeBytes(s);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeChars(java.lang.String)
	 */
	@Override
	public final void writeChars(String s) throws IOException {
		wc.onWrite();
		file.writeChars(s);
	}

	/* (non-Javadoc)
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeUTF(java.lang.String)
	 */
	@Override
	public final void writeUTF(String str) throws IOException {
		wc.onWrite();
		file.writeUTF(str);
	}

}
