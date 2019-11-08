package com.github.simbo1905.srs;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Function;

class RecordWriter<T> {

    private final Function<T, byte[]> serializer;
    String key;
    DbByteArrayOutputStream out = new DbByteArrayOutputStream();

    public RecordWriter(String key, Function<T, byte[]> serializer) {
        this.key = key;
        this.serializer = serializer;
    }

    public String getKey() {
        return key;
    }

    public void writeObject(T o) throws IOException {
        byte[] b = serializer.apply(o);
        out.write(b);
        out.flush();
    }

    /*
     * Returns the number of bytes in the data.
     */
    public int getDataLength() {
        return out.size();
    }

    /*
     * Writes the data out to the stream without re-allocating the buffer.
     */
    public long writeTo(DataOutput str) throws IOException {
        return out.writeTo(str);
    }

    public void clear() throws IOException {
        out.reset();
    }

}






