package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Memory-mapped implementation of FileOperations that reduces write amplification
/// by batching writes in memory and deferring disk flushes. Preserves crash safety guarantees
/// through the same dual-write patterns used by RandomAccessFile.
///
/// This implementation maps the entire file into memory in chunks and performs all writes
/// through the memory-mapped buffers. The force operation is only called on close()
/// or explicit sync(), giving the host application control over durability timing.
class MemoryMappedFile implements FileOperations {

  static final long MAPPING_CHUNK_SIZE = 128 * 1024 * 1024; // 128 MB per chunk
  static final Logger logger = Logger.getLogger(MemoryMappedFile.class.getName());

  final RandomAccessFile randomAccessFile;
  final FileChannel channel;
  volatile Epoch currentEpoch; // current mapping state
  private long position = 0;

  /// Immutable holder for a complete memory mapping epoch.
  /// Allows atomic swapping of entire mapping state.
  record Epoch(List<MappedByteBuffer> buffers, long[] regionStarts, long mappedSize) {
    Epoch(List<MappedByteBuffer> buffers, long[] regionStarts, long mappedSize) {
      this.buffers = List.copyOf(buffers);
      this.regionStarts = regionStarts.clone();
      this.mappedSize = mappedSize;
    }
  }

  /// Creates a new memory-mapped file wrapper.
  /// @param file The underlying RandomAccessFile
  /// @throws IOException if mapping fails
  public MemoryMappedFile(java.io.RandomAccessFile file) throws IOException {
    this.randomAccessFile = file;
    this.channel = file.getChannel();
    mapFile();
  }

  private void mapFile() throws IOException {
    long fileSize = channel.size();
    List<Long> starts = new ArrayList<>();
    List<MappedByteBuffer> mappedBuffers = new ArrayList<>();

    // Start with READ_WRITE mode; will switch to READ_ONLY if channel is not writable
    FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    boolean readOnlyMode = false;

    // Map the file in chunks to avoid issues with very large files
    long pos = 0;
    while (pos < fileSize) {
      long chunkSize = Math.min(MAPPING_CHUNK_SIZE, fileSize - pos);
      try {
        MappedByteBuffer buffer = channel.map(mapMode, pos, chunkSize);
        mappedBuffers.add(buffer);
        starts.add(pos);
        pos += chunkSize;
      } catch (java.nio.channels.NonWritableChannelException e) {
        // File is read-only, switch to READ_ONLY mode and retry
        if (!readOnlyMode) {
          mapMode = FileChannel.MapMode.READ_ONLY;
          readOnlyMode = true;
          // Retry current chunk with read-only mode
          MappedByteBuffer buffer = channel.map(mapMode, pos, chunkSize);
          mappedBuffers.add(buffer);
          starts.add(pos);
          pos += chunkSize;
        } else {
          // Already in read-only mode or other error - throw exception
          throw new IOException(
              "Failed to map file chunk at position " + pos + " in " + mapMode + " mode", e);
        }
      }
    }

    long[] regionStarts = starts.stream().mapToLong(Long::longValue).toArray();
    currentEpoch = new Epoch(mappedBuffers, regionStarts, fileSize);
  }

  /// Finds the mapped buffer and offset for a given file position.
  private BufferLocation locate(long pos) {
    Epoch epoch = currentEpoch;
    if (pos < 0 || pos >= epoch.mappedSize) {
      throw new IllegalArgumentException(
          "Position " + pos + " out of range [0, " + epoch.mappedSize + ")");
    }

    // Binary search for the correct buffer
    for (int i = 0; i < epoch.regionStarts.length; i++) {
      long start = epoch.regionStarts[i];
      long end = (i + 1 < epoch.regionStarts.length) ? epoch.regionStarts[i + 1] : epoch.mappedSize;

      if (pos >= start && pos < end) {
        int offset = (int) (pos - start);
        return new BufferLocation(epoch.buffers.get(i), offset);
      }
    }

    throw new IllegalArgumentException("Could not locate position " + pos);
  }

  private record BufferLocation(MappedByteBuffer buffer, int offset) {}

  @Override
  public long getFilePointer() {
    return position;
  }

  @Override
  public void sync() throws IOException {
    // Force all mapped buffers to disk - use current epoch, not initial mappedBuffers
    Epoch epoch = currentEpoch;
    for (MappedByteBuffer buffer : epoch.buffers) {
      buffer.force();
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    Epoch epoch = currentEpoch;
    if (position >= epoch.mappedSize) {
      return -1;
    }

    int toRead = (int) Math.min(b.length, epoch.mappedSize - position);
    locate(position);
    BufferLocation loc;

    // Handle reads that span multiple buffers
    int remaining = toRead;
    int offset = 0;
    long currentPos = position;

    while (remaining > 0) {
      loc = locate(currentPos);
      int available = Math.min(remaining, loc.buffer.limit() - loc.offset);

      // Save and restore position
      int oldPos = loc.buffer.position();
      loc.buffer.position(loc.offset);
      loc.buffer.get(b, offset, available);
      loc.buffer.position(oldPos);

      remaining -= available;
      offset += available;
      currentPos += available;
    }

    position = currentPos;
    return toRead;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    int bytesRead = read(b);
    if (bytesRead < b.length) {
      throw new IOException("EOF reached before reading " + b.length + " bytes");
    }
  }

  @Override
  public void write(int b) throws IOException {
    ensureCapacity(position + 1);
    BufferLocation loc = locate(position);
    loc.buffer.put(loc.offset, (byte) b);
    position++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureCapacity(position + len);

    // Handle writes that span multiple buffers
    int remaining = len;
    int srcOffset = off;
    long currentPos = position;

    while (remaining > 0) {
      BufferLocation loc = locate(currentPos);
      int available = Math.min(remaining, loc.buffer.limit() - loc.offset);

      // Save and restore position
      int oldPos = loc.buffer.position();
      loc.buffer.position(loc.offset);
      loc.buffer.put(b, srcOffset, available);
      loc.buffer.position(oldPos);

      remaining -= available;
      srcOffset += available;
      currentPos += available;
    }

    position = currentPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("Negative seek position: " + pos);
    }
    position = pos;
  }

  @Override
  public long length() throws IOException {
    return currentEpoch.mappedSize;
  }

  @Override
  public synchronized void setLength(long newLength) throws IOException {
    Epoch current = currentEpoch;
    if (newLength == current.mappedSize) {
      return;
    }

    // Validate new length
    if (newLength < 0) {
      throw new IllegalArgumentException("New length must be non-negative: " + newLength);
    }

    try {
      // Build new epoch first
      Epoch newEpoch = buildNewEpoch(newLength);

      // Ensure position is still valid before publishing
      if (position > newLength) {
        position = newLength;
      }

      // Publish new epoch atomically (readers will now use new buffers)
      currentEpoch = newEpoch;

      // Clean up old epoch buffers after publishing (safe to unmap now)
      unmapEpoch(current);

    } catch (Exception e) {
      // If anything goes wrong, don't leave object in inconsistent state
      // Current epoch remains valid
      throw new IOException("Failed to set file length to " + newLength, e);
    }
  }

  /// Builds a new epoch with the specified file length.
  /// This method creates a completely new mapping state without modifying the current one.
  private Epoch buildNewEpoch(long newLength) throws IOException {
    // Force all changes to disk before remapping
    Epoch current = currentEpoch;
    for (MappedByteBuffer buffer : current.buffers) {
      buffer.force();
    }

    // Resize the underlying file
    randomAccessFile.setLength(newLength);

    // Create new mapping for the resized file
    List<MappedByteBuffer> newBuffers = new ArrayList<>();
    List<Long> newStarts = new ArrayList<>();

    long fileSize = channel.size();
    FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    boolean readOnlyMode = false;

    long pos = 0;
    while (pos < fileSize) {
      long chunkSize = Math.min(MAPPING_CHUNK_SIZE, fileSize - pos);
      try {
        MappedByteBuffer buffer = channel.map(mapMode, pos, chunkSize);
        newBuffers.add(buffer);
        newStarts.add(pos);
        pos += chunkSize;
      } catch (java.nio.channels.NonWritableChannelException e) {
        if (!readOnlyMode) {
          mapMode = FileChannel.MapMode.READ_ONLY;
          readOnlyMode = true;
          MappedByteBuffer buffer = channel.map(mapMode, pos, chunkSize);
          newBuffers.add(buffer);
          newStarts.add(pos);
          pos += chunkSize;
        } else {
          throw new IOException(
              "Failed to map file chunk at position " + pos + " in " + mapMode + " mode", e);
        }
      }
    }

    long[] regionStarts = newStarts.stream().mapToLong(Long::longValue).toArray();
    return new Epoch(newBuffers, regionStarts, fileSize);
  }

  /// Explicitly unmaps all buffers in an epoch to prevent native memory leaks.
  /// Uses reflection to access the Cleaner for proper cleanup.
  private void unmapEpoch(Epoch epoch) {
    for (MappedByteBuffer buffer : epoch.buffers) {
      unmapBuffer(buffer);
    }
  }

  /// Explicitly unmaps a single MappedByteBuffer to prevent native memory leaks.
  /// This is package-private for testing purposes.
  static void unmapBuffer(MappedByteBuffer buffer) {
    try {
      // Try to use Cleaner for explicit unmapping
      // Use reflection to access the cleaner to avoid module system issues
      java.lang.reflect.Method cleanerMethod = buffer.getClass().getMethod("cleaner");
      cleanerMethod.setAccessible(true);
      Object cleaner = cleanerMethod.invoke(buffer);
      if (cleaner != null) {
        java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
        cleanMethod.setAccessible(true);
        cleanMethod.invoke(cleaner);
        logger.log(Level.FINEST, "Explicitly unmapped buffer");
      }
    } catch (Exception e) {
      // Fall back to relying on GC if Cleaner is not available
      logger.log(
          Level.FINEST, "Could not explicitly unmap buffer, relying on GC: " + e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    try {
      // Force all changes to disk before closing
      sync();
    } finally {
      // Clean up mapped buffers from current epoch
      if (currentEpoch != null) {
        unmapEpoch(currentEpoch);
      }

      // Close the underlying file
      randomAccessFile.close();
    }
  }

  @Override
  public byte readByte() throws IOException {
    Epoch epoch = currentEpoch;
    if (position >= epoch.mappedSize) {
      throw new IOException("EOF");
    }
    BufferLocation loc = locate(position);
    byte b = loc.buffer.get(loc.offset);
    position++;
    return b;
  }

  @Override
  public short readShort() throws IOException {
    byte[] bytes = new byte[2];
    readFully(bytes);
    return ByteBuffer.wrap(bytes).getShort();
  }

  @Override
  public int readInt() throws IOException {
    byte[] bytes = new byte[4];
    readFully(bytes);
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  public long readLong() throws IOException {
    byte[] bytes = new byte[8];
    readFully(bytes);
    return ByteBuffer.wrap(bytes).getLong();
  }

  @Override
  public void writeShort(short v) throws IOException {
    byte[] bytes = new byte[2];
    ByteBuffer.wrap(bytes).putShort(v);
    write(bytes);
  }

  @Override
  public void writeInt(int v) throws IOException {
    byte[] bytes = new byte[4];
    ByteBuffer.wrap(bytes).putInt(v);
    write(bytes);
  }

  @Override
  public void writeLong(long v) throws IOException {
    byte[] bytes = new byte[8];
    ByteBuffer.wrap(bytes).putLong(v);
    write(bytes);
  }

  /// Ensures that the mapped region is large enough to accommodate the given position.
  /// If not, extends the file and remaps.
  private void ensureCapacity(long requiredSize) throws IOException {
    Epoch epoch = currentEpoch;
    if (requiredSize > epoch.mappedSize) {
      setLength(requiredSize);
    }
  }
}
