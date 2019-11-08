package com.github.simbo1905.srs;

import java.io.*;
import java.util.function.Function;

public class RecordReader<T> {

  private final Function<byte[], T> deserializer;
  String key;
  byte[] data;

  public RecordReader(String key, byte[] data, Function<byte[], T> deserializer) {
    this.key = key;
    this.data = data;
    this.deserializer = deserializer;
  }

  public String getKey() {
    return key;
  }

  public byte[] getData() {
    return data;
  }

  /*
   * Reads the next object in the record using an ObjectInputStream.
   */
  public T readObject() throws IOException, ClassNotFoundException {
    return deserializer.apply(data);
  }

}






