package com.github.simbo1905.srs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
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

  /**
   * Writes the full contents of the buffer a DataOutput stream.
   */
  public synchronized void writeTo (DataOutput dstr) throws IOException {
    byte[] data = super.buf;
    int l = super.size();
    dstr.write(data, 0, l);
  }

}
