package com.github.trex_paxos.srs;

import java.io.IOException;
import java.io.RandomAccessFile;

/*
 * easier to mock final native class by wrapping it in an interface
 */
record DirectRandomAccessFile(RandomAccessFile randomAccessFile) implements RandomAccessFileInterface {

  @Override
  public long getFilePointer() throws IOException {
    return randomAccessFile.getFilePointer();
  }

  @Override
  public void fsync() throws IOException {
    randomAccessFile.getChannel().force(false);
  }

  public int read(byte[] b) throws IOException {
    return randomAccessFile.read(b);
  }

  public void readFully(byte[] b) throws IOException {
    randomAccessFile.readFully(b);
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

  public byte readByte() throws IOException {
    return randomAccessFile.readByte();
  }

  public int readInt() throws IOException {
    return randomAccessFile.readInt();
  }

  public long readLong() throws IOException {
    return randomAccessFile.readLong();
  }

  public void writeInt(int v) throws IOException {
    randomAccessFile.writeInt(v);
  }

  public void writeLong(long v) throws IOException {
    randomAccessFile.writeLong(v);
  }

}
