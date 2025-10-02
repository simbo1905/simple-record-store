package com.github.trex_paxos.srs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/// Abstract base class for delegating file operations with operation counting and logging.
/// Provides the foundation for both exception injection and operation halting scenarios.
public abstract class AbstractDelegatingFileOperations implements CrashSafeFileOperations {
    
    private static final Logger logger = Logger.getLogger(AbstractDelegatingFileOperations.class.getName());
    
    protected final CrashSafeFileOperations delegate;
    protected int operationCount = 0;
    protected final int targetOperation;
    
    public AbstractDelegatingFileOperations(CrashSafeFileOperations delegate, int targetOperation) {
        this.delegate = delegate;
        this.targetOperation = targetOperation;
        logger.log(Level.FINE, () -> String.format("Created %s with targetOperation=%d", 
            this.getClass().getSimpleName(), targetOperation));
    }
    
    /// Hook method for subclasses to implement their specific behavior at target operation
    protected abstract void handleTargetOperation() throws IOException;
    
    /// Count operations and trigger target operation behavior when reached
    protected void checkOperation() throws IOException {
        operationCount++;
        logger.log(Level.FINEST, () -> String.format("Operation %d: checking if should trigger behavior (target=%d)", 
            operationCount, targetOperation));
        
        if (operationCount == targetOperation) {
            logger.log(Level.FINE, () -> String.format("TRIGGERING BEHAVIOR at operation %d", operationCount));
            handleTargetOperation();
        }
    }
    
    @Override
    public long getFilePointer() throws IOException {
        checkOperation();
        return delegate.getFilePointer();
    }
    
    @Override
    public void seek(long pos) throws IOException {
        checkOperation();
        delegate.seek(pos);
    }
    
    @Override
    public long length() throws IOException {
        checkOperation();
        return delegate.length();
    }
    
    @Override
    public void setLength(long newLength) throws IOException {
        checkOperation();
        delegate.setLength(newLength);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkOperation();
        delegate.write(b, off, len);
    }
    
    @Override
    public void writeInt(int v) throws IOException {
        checkOperation();
        delegate.writeInt(v);
    }
    
    @Override
    public void writeLong(long v) throws IOException {
        checkOperation();
        delegate.writeLong(v);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        checkOperation();
        delegate.write(b);
    }
    
    @Override
    public void write(int b) throws IOException {
        checkOperation();
        delegate.write(b);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        checkOperation();
        return delegate.read(b);
    }
    
    @Override
    public void readFully(byte[] b) throws IOException {
        checkOperation();
        delegate.readFully(b);
    }
    
    @Override
    public int readInt() throws IOException {
        checkOperation();
        return delegate.readInt();
    }
    
    @Override
    public long readLong() throws IOException {
        checkOperation();
        return delegate.readLong();
    }
    
    @Override
    public void sync() throws IOException {
        checkOperation();
        delegate.sync();
    }
    
    @Override
    public void close() throws IOException {
        checkOperation();
        delegate.close();
    }
    
    @Override
    public byte readByte() throws IOException {
        checkOperation();
        return delegate.readByte();
    }
    
    /// Get the current operation count
    public int getOperationCount() {
        return operationCount;
    }
    
    /// Get the target operation that triggers behavior
    public int getTargetOperation() {
        return targetOperation;
    }
    
    /// Check if the target operation has been reached
    public boolean hasReachedTarget() {
        return operationCount >= targetOperation;
    }
}