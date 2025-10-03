package com.github.trex_paxos.srs;

import lombok.Getter;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/// A delegating CrashSafeFileOperations that counts operations and can halt at a specific count.
/// This allows controlled testing of file operations without throwing exceptions.
@Getter
class DelegatingHaltOperations extends AbstractDelegatingFileOperations {
    
    private static final Logger logger = Logger.getLogger(DelegatingHaltOperations.class.getName());

  /**
   */
  private boolean halted = false;
    
    public DelegatingHaltOperations(CrashSafeFileOperations delegate, int haltAtOperation) {
        super(delegate, haltAtOperation);
        logger.log(Level.FINE, () -> String.format("Created DelegatingHaltOperations with haltAtOperation=%d", haltAtOperation));
    }
    
    @Override
    protected void handleTargetOperation() {
        halted = true;
        logger.log(Level.FINE, () -> String.format("HALTING at operation %d - all subsequent operations will be silently ignored", operationCount));
    }

  @Override
    public long getFilePointer() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: getFilePointer() returning 0");
            return 0;
        }
        return delegate.getFilePointer();
    }
    
    @Override
    public void seek(long pos) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: seek(%d) ignored", pos));
            return;
        }
        delegate.seek(pos);
    }
    
    @Override
    public long length() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: length() returning 0");
            return 0;
        }
        return delegate.length();
    }
    
    @Override
    public void setLength(long newLength) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: setLength(%d) ignored", newLength));
            return;
        }
        delegate.setLength(newLength);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: write(%d bytes) ignored", len));
            return;
        }
        delegate.write(b, off, len);
    }
    
    @Override
    public void writeInt(int v) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: writeInt(%d) ignored", v));
            return;
        }
        delegate.writeInt(v);
    }
    
    @Override
    public void writeLong(long v) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: writeLong(%d) ignored", v));
            return;
        }
        delegate.writeLong(v);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: write(%d bytes) ignored", b.length));
            return;
        }
        delegate.write(b);
    }
    
    @Override
    public void write(int b) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: writeByte(%d) ignored", b));
            return;
        }
        delegate.write(b);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, () -> String.format("HALTED: read(%d bytes) returning -1", b.length));
            return -1;
        }
        return delegate.read(b);
    }
    
    @Override
    public void readFully(byte[] b) throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: readFully() ignored");
            return;
        }
        delegate.readFully(b);
    }
    
    @Override
    public int readInt() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: readInt() returning 0");
            return 0;
        }
        return delegate.readInt();
    }
    
    @Override
    public long readLong() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: readLong() returning 0");
            return 0;
        }
        return delegate.readLong();
    }
    
    @Override
    public void sync() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: sync() ignored");
            return;
        }
        delegate.sync();
    }
    
    @Override
    public void close() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: close() ignored");
            return;
        }
        delegate.close();
    }
    
    @Override
    public byte readByte() throws IOException {
        checkOperation();
        if (halted) {
            logger.log(Level.FINEST, "HALTED: readByte() returning 0");
            return 0;
        }
        return delegate.readByte();
    }
}
