package com.github.trex_paxos.srs.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Utility to expand the maximum key size of a Simple Record Store file.
 * This creates a new file with the expanded key size and copies all data in a single forward pass.
 */
public class ExpandKeySize {

    // File format constants (matching FileRecordStore)
    private static final int RECORD_HEADER_LENGTH = 20;
    private static final long NUM_RECORDS_HEADER_LOCATION = 1;
    private static final long DATA_START_HEADER_LOCATION = 5;
    private static final int FILE_HEADERS_REGION_LENGTH = 13;
    private static final int CRC32_LENGTH = 4;
    private static final int MAX_KEY_LENGTH_THEORETICAL = 252;

    /**
     * Expands the key size of a database file.
     *
     * @param oldFilePath Path to the existing database file
     * @param newFilePath Path for the new database file (must not exist)
     * @param newMaxKeyLength New maximum key length (must be larger than current)
     * @throws IOException if file operations fail
     * @throws IllegalArgumentException if validation fails
     */
    public static void expandKeySize(String oldFilePath, String newFilePath, int newMaxKeyLength) 
            throws IOException {
        
        // Validation
        File oldFile = new File(oldFilePath);
        if (!oldFile.exists()) {
            throw new IllegalArgumentException("Source file does not exist: " + oldFilePath);
        }
        
        File newFile = new File(newFilePath);
        if (newFile.exists()) {
            throw new IllegalArgumentException("Destination file already exists: " + newFilePath);
        }
        
        if (newMaxKeyLength < 1 || newMaxKeyLength > MAX_KEY_LENGTH_THEORETICAL) {
            throw new IllegalArgumentException(
                String.format("New key length must be between 1 and %d, got: %d", 
                    MAX_KEY_LENGTH_THEORETICAL, newMaxKeyLength));
        }

        try (RandomAccessFile oldRaf = new RandomAccessFile(oldFile, "r");
             RandomAccessFile newRaf = new RandomAccessFile(newFile, "rw")) {
            
            // Read old file header
            oldRaf.seek(0);
            int oldMaxKeyLength = oldRaf.readByte() & 0xFF;
            
            if (newMaxKeyLength <= oldMaxKeyLength) {
                throw new IllegalArgumentException(
                    String.format("New key length (%d) must be larger than current key length (%d)",
                        newMaxKeyLength, oldMaxKeyLength));
            }
            
            oldRaf.seek(NUM_RECORDS_HEADER_LOCATION);
            int numRecords = oldRaf.readInt();
            
            oldRaf.seek(DATA_START_HEADER_LOCATION);
            long oldDataStartPtr = oldRaf.readLong();
            
            // Calculate new dimensions
            int oldIndexEntryLength = oldMaxKeyLength + Integer.BYTES + RECORD_HEADER_LENGTH;
            int newIndexEntryLength = newMaxKeyLength + Integer.BYTES + RECORD_HEADER_LENGTH;
            
            long newDataStartPtr = FILE_HEADERS_REGION_LENGTH + 
                                   ((long) newIndexEntryLength * numRecords);
            
            // Calculate offset adjustment for data pointers
            long dataPointerOffset = newDataStartPtr - oldDataStartPtr;
            
            // Write new file header
            newRaf.seek(0);
            newRaf.writeByte((byte) newMaxKeyLength);
            newRaf.seek(NUM_RECORDS_HEADER_LOCATION);
            newRaf.writeInt(numRecords);
            newRaf.seek(DATA_START_HEADER_LOCATION);
            newRaf.writeLong(newDataStartPtr);
            
            // Process each index entry
            List<HeaderInfo> headers = new ArrayList<>(numRecords);
            
            for (int i = 0; i < numRecords; i++) {
                // Read key from old file
                long oldKeyFp = FILE_HEADERS_REGION_LENGTH + ((long) oldIndexEntryLength * i);
                oldRaf.seek(oldKeyFp);
                
                int keyLen = oldRaf.readByte() & 0xFF;
                byte[] key = new byte[keyLen];
                oldRaf.readFully(key);
                
                byte[] keyCrcBytes = new byte[CRC32_LENGTH];
                oldRaf.readFully(keyCrcBytes);
                
                // Seek to record header (skip any padding after key+crc)
                long oldHeaderFp = oldKeyFp + oldMaxKeyLength;
                oldRaf.seek(oldHeaderFp);
                
                // Read old record header
                byte[] headerBytes = new byte[RECORD_HEADER_LENGTH];
                oldRaf.readFully(headerBytes);
                
                ByteBuffer headerBuffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
                headerBuffer.put(headerBytes);
                headerBuffer.flip();
                
                long oldDataPointer = headerBuffer.getLong();
                int dataCapacity = headerBuffer.getInt();
                int dataCount = headerBuffer.getInt();
                long headerCrc32 = headerBuffer.getInt() & 0xFFFFFFFFL;
                
                // Validate header CRC
                CRC32 crc = new CRC32();
                crc.update(headerBytes, 0, 16);
                long expectedCrc = crc.getValue();
                if (expectedCrc != headerCrc32) {
                    throw new IOException(
                        String.format("Invalid header CRC32 at index %d: expected %d, got %d",
                            i, expectedCrc, headerCrc32));
                }
                
                // Calculate new data pointer
                long newDataPointer = oldDataPointer + dataPointerOffset;
                
                // Write key to new file
                long newKeyFp = FILE_HEADERS_REGION_LENGTH + ((long) newIndexEntryLength * i);
                newRaf.seek(newKeyFp);
                
                newRaf.writeByte((byte) keyLen);
                newRaf.write(key);
                newRaf.write(keyCrcBytes);
                
                // Seek to new record header position (skip padding)
                long newHeaderFp = newKeyFp + newMaxKeyLength;
                newRaf.seek(newHeaderFp);
                
                // Write new record header
                ByteBuffer newHeaderBuffer = ByteBuffer.allocate(RECORD_HEADER_LENGTH);
                newHeaderBuffer.putLong(newDataPointer);
                newHeaderBuffer.putInt(dataCapacity);
                newHeaderBuffer.putInt(dataCount);
                
                byte[] newHeaderBytes = newHeaderBuffer.array();
                CRC32 newCrc = new CRC32();
                newCrc.update(newHeaderBytes, 0, 16);
                int newCrc32 = (int) (newCrc.getValue() & 0xFFFFFFFFL);
                newHeaderBuffer.putInt(newCrc32);
                
                newRaf.write(newHeaderBuffer.array());
                
                // Store header info for data copy phase
                headers.add(new HeaderInfo(oldDataPointer, newDataPointer, dataCapacity));
            }
            
            // Copy data region in a single forward pass
            byte[] buffer = new byte[8192]; // 8KB buffer for efficient copying
            
            for (int i = 0; i < numRecords; i++) {
                HeaderInfo info = headers.get(i);
                
                // Position both files
                oldRaf.seek(info.oldDataPointer);
                newRaf.seek(info.newDataPointer);
                
                // Copy data
                int remaining = info.dataCapacity;
                while (remaining > 0) {
                    int toRead = Math.min(remaining, buffer.length);
                    int bytesRead = oldRaf.read(buffer, 0, toRead);
                    if (bytesRead < 0) {
                        throw new IOException(
                            String.format("Unexpected EOF while copying data for record %d", i));
                    }
                    newRaf.write(buffer, 0, bytesRead);
                    remaining -= bytesRead;
                }
            }
            
            // Ensure data is written to disk
            newRaf.getFD().sync();
        }
    }

    /**
     * Internal class to hold header information during copy.
     */
    private static class HeaderInfo {
        final long oldDataPointer;
        final long newDataPointer;
        final int dataCapacity;
        
        HeaderInfo(long oldDataPointer, long newDataPointer, int dataCapacity) {
            this.oldDataPointer = oldDataPointer;
            this.newDataPointer = newDataPointer;
            this.dataCapacity = dataCapacity;
        }
    }

    /**
     * Main entry point for command-line usage.
     *
     * @param args Command line arguments: expand_key_size <new_key_size> <old_file> <new_file>
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: expand_key_size <new_key_size> <old_file> <new_file>");
            System.err.println("  new_key_size: The new maximum key size (must be larger than current)");
            System.err.println("  old_file: Path to the existing database file");
            System.err.println("  new_file: Path for the new database file (must not exist)");
            System.exit(1);
        }

        String command = args[0];
        if (!"expand_key_size".equals(command)) {
            System.err.println("Unknown command: " + command);
            System.err.println("Expected: expand_key_size");
            System.exit(1);
        }

        try {
            int newKeySize = Integer.parseInt(args[1]);
            String oldFile = args[2];
            String newFile = args[3];

            System.out.println("Expanding key size...");
            System.out.println("  New key size: " + newKeySize);
            System.out.println("  Source file: " + oldFile);
            System.out.println("  Destination file: " + newFile);

            expandKeySize(oldFile, newFile, newKeySize);

            System.out.println("Success! Database expanded to new key size.");

        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid key size: " + args[1]);
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
