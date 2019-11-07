package com.github.simbo1905.srs;

import sun.jvm.hotspot.utilities.AssertionFailure;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.CRC32;

/*
 * Extends ByteArrayOutputStream to provide a way of writing the buffer to
 * a DataOutput without re-allocating it.
 */
public class DbByteArrayOutputStream extends ByteArrayOutputStream {

  public DbByteArrayOutputStream() {
    super();
  }

  public DbByteArrayOutputStream(int size) {
    super(size);
  }

  /*
   * Writes the full contents of the buffer a DataOutput stream.
   */
  public synchronized long writeTo (DataOutput dstr) throws IOException {
    CRC32 crc32 = new CRC32();
    byte[] data = super.buf;
    int l = super.size();
    dstr.write(data, 0, l);
    crc32.update(data, 0, l);
    return crc32.getValue();
  }

}
