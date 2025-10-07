package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.io.RandomAccessFile;

@SuppressWarnings("ClassEscapesDefinedScope")
public record InterceptedRandomAccessFile(RandomAccessFile file, WriteCallback wc)
    implements FileOperations {

  @Override
  public void sync() {}

  @Override
  public long getFilePointer() throws IOException {
    return file.getFilePointer();
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
  public void readFully(byte[] b) throws IOException {
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
  public byte readByte() throws IOException {
    wc.onWrite();
    return file.readByte();
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readShort()
   */
  @Override
  public short readShort() throws IOException {
    wc.onRead();
    return file.readShort();
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readInt()
   */
  @Override
  public int readInt() throws IOException {
    wc.onWrite();
    return file.readInt();
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#readLong()
   */
  @Override
  public long readLong() throws IOException {
    wc.onWrite();
    return file.readLong();
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeShort(short)
   */
  @Override
  public void writeShort(short v) throws IOException {
    wc.onWrite();
    file.writeShort(v);
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeInt(int)
   */
  @Override
  public void writeInt(int v) throws IOException {
    wc.onWrite();
    file.writeInt(v);
  }

  /* (non-Javadoc)
   * @see com.github.simbo1905.chronicle.db.IRandomAccessFile#writeLong(long)
   */
  @Override
  public void writeLong(long v) throws IOException {
    wc.onWrite();
    file.writeLong(v);
  }

  @Override
  public RecordHeader readRecordHeader(int indexPosition) throws IOException {
    return RecordHeader.readFrom(this, indexPosition);
  }

  @Override
  public void writeRecordHeader(RecordHeader header) throws IOException {
    RecordHeader.writeTo(this, header);
  }
}
