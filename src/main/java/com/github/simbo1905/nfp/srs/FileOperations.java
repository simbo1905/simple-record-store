package com.github.simbo1905.nfp.srs;

import java.io.IOException;

/// Interface for crash-safe file operations with explicit synchronization control.
/// Wraps file I/O operations to support both direct and memory-mapped implementations
/// while maintaining crash safety guarantees through coordinated write patterns.
interface FileOperations {

  /// Forces all buffered modifications to be written to the storage device.
  /// For direct I/O, calls FileChannel.force(false). For memory-mapped I/O,
  /// calls MappedByteBuffer.force() on all mapped regions.
	void sync() throws IOException;

	long getFilePointer() throws IOException;

	@SuppressWarnings("UnusedReturnValue")
  int read(byte[] b) throws IOException;

	void readFully(byte[] b) throws IOException;

	void write(int b) throws IOException;

  @SuppressWarnings("unused")
	void write(byte[] b) throws IOException;

	void write(byte[] b, int off, int len) throws IOException;

	void seek(long pos) throws IOException;

	long length() throws IOException;

	void setLength(long newLength) throws IOException;

	void close() throws IOException;

	byte readByte() throws IOException;

	int readInt() throws IOException;

	long readLong() throws IOException;

	void writeInt(int v) throws IOException;

	void writeLong(long v) throws IOException;
}
