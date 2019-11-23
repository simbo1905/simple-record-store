package com.github.trex_paxos.srs;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public final class InterceptedRandomAccessFile implements RandomAccessFileInterface {
	final private RandomAccessFile file;
	final private WriteCallback wc;

	InterceptedRandomAccessFile(final RandomAccessFile file, final WriteCallback wc) {
		this.file = file;		
		this.wc = wc;
	}

	@Override
	public void fsync() throws IOException {
	}

	@Override
	public long getFilePointer() throws IOException {
		return file.getFilePointer();
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

	public final FileDescriptor getFD() throws IOException {
		wc.onWrite();
		return file.getFD();
	}

	public final FileChannel getChannel() {
		return file.getChannel();
	}

	public int read() throws IOException {
		wc.onWrite();
		return file.read();
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
	 * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readByte()
	 */
	@Override
	public final byte readByte() throws IOException {
		wc.onWrite();
		return file.readByte();
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


}
