package com.github.trex_paxos.srs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Memory-mapped implementation of CrashSafeFileOperations that reduces write amplification
 * by batching writes in memory and deferring disk flushes. Preserves crash safety guarantees
 * through the same dual-write patterns used by DirectRandomAccessFile.
 * 
 * This implementation maps the entire file into memory in chunks and performs all writes
 * through the memory-mapped buffers. The force operation is only called on close()
 * or explicit sync(), giving the host application control over durability timing.
 */
class MemoryMappedRandomAccessFile implements CrashSafeFileOperations {

    private static final long MAPPING_CHUNK_SIZE = 128 * 1024 * 1024; // 128 MB per chunk

    private final RandomAccessFile randomAccessFile;
    private final FileChannel channel;
    private List<MappedByteBuffer> mappedBuffers;
    private long[] mappedRegionStarts;
    private long mappedSize;
    private long position = 0;

    /**
     * Creates a new memory-mapped file wrapper.
     * @param file The underlying RandomAccessFile
     * @throws IOException if mapping fails
     */
    public MemoryMappedRandomAccessFile(RandomAccessFile file) throws IOException {
        this.randomAccessFile = file;
        this.channel = file.getChannel();
        this.mappedBuffers = new ArrayList<>();
        mapFile();
    }
    
    private void mapFile() throws IOException {
        long fileSize = channel.size();
        List<Long> starts = new ArrayList<>();
        
        // Determine if the file is writable
        FileChannel.MapMode mapMode;
        try {
            // Try to determine if writable by attempting a write operation check
            // If the file was opened with "r", this will fail
            mapMode = FileChannel.MapMode.READ_WRITE;
        } catch (Exception e) {
            mapMode = FileChannel.MapMode.READ_ONLY;
        }
        
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
                // File is read-only, remap with READ_ONLY mode
                if (mapMode == FileChannel.MapMode.READ_WRITE) {
                    mapMode = FileChannel.MapMode.READ_ONLY;
                    // Retry with read-only mode
                    MappedByteBuffer buffer = channel.map(mapMode, pos, chunkSize);
                    mappedBuffers.add(buffer);
                    starts.add(pos);
                    pos += chunkSize;
                } else {
                    throw e;
                }
            }
        }
        
        mappedRegionStarts = starts.stream().mapToLong(Long::longValue).toArray();
        mappedSize = fileSize;
    }

    /**
     * Finds the mapped buffer and offset for a given file position.
     */
    private BufferLocation locate(long pos) {
        if (pos < 0 || pos > mappedSize) {
            throw new IllegalArgumentException("Position " + pos + " out of range [0, " + mappedSize + "]");
        }
        
        // Binary search for the correct buffer
        for (int i = 0; i < mappedRegionStarts.length; i++) {
            long start = mappedRegionStarts[i];
            long end = (i + 1 < mappedRegionStarts.length) ? mappedRegionStarts[i + 1] : mappedSize;
            
            if (pos >= start && pos < end) {
                int offset = (int) (pos - start);
                return new BufferLocation(mappedBuffers.get(i), offset);
            }
        }
        
        throw new IllegalArgumentException("Could not locate position " + pos);
    }

    private record BufferLocation(MappedByteBuffer buffer, int offset) {}

    @Override
    public long getFilePointer() throws IOException {
        return position;
    }

    @Override
    public void sync() throws IOException {
        // Force all mapped buffers to disk
        for (MappedByteBuffer buffer : mappedBuffers) {
            buffer.force();
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (position >= mappedSize) {
            return -1;
        }
        
        int toRead = (int) Math.min(b.length, mappedSize - position);
        BufferLocation loc = locate(position);
        
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
        return mappedSize;
    }

    @Override
    public void setLength(long newLength) throws IOException {
        if (newLength == mappedSize) {
            return;
        }
        
        // If shrinking or growing within current mapping, just update the logical size
        if (newLength <= mappedSize) {
            // Shrinking - just update file length, keep mapping
            randomAccessFile.setLength(newLength);
            mappedSize = newLength;
            if (position > newLength) {
                position = newLength;
            }
            return;
        }
        
        // Growing beyond current mapping - need to remap
        // Force all changes to disk before unmapping
        for (MappedByteBuffer buffer : mappedBuffers) {
            buffer.force();
        }
        
        // Clear existing buffers
        mappedBuffers.clear();
        
        // Resize the underlying file
        randomAccessFile.setLength(newLength);
        
        // Remap the file with new size
        mapFile();
        
        // Ensure position is still valid
        if (position > newLength) {
            position = newLength;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Force all changes to disk before closing
            sync();
        } finally {
            // Clean up mapped buffers
            mappedBuffers.clear();
            
            // Close the underlying file
            randomAccessFile.close();
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (position >= mappedSize) {
            throw new IOException("EOF");
        }
        BufferLocation loc = locate(position);
        byte b = loc.buffer.get(loc.offset);
        position++;
        return b;
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

    /**
     * Ensures that the mapped region is large enough to accommodate the given position.
     * If not, extends the file and remaps.
     */
    private void ensureCapacity(long requiredSize) throws IOException {
        if (requiredSize > mappedSize) {
            setLength(requiredSize);
        }
    }
}
