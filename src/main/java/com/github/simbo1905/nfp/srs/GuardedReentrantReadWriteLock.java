package com.github.simbo1905.nfp.srs;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/// A guarded wrapper around ReentrantReadWriteLock that returns AutoCloseable guards,
/// making it impossible to forget to unlock. Encapsulates the lock implementation
/// to prevent direct access and potential misuse.
public final class GuardedReentrantReadWriteLock {
  private final ReentrantReadWriteLock lock;

  public GuardedReentrantReadWriteLock(boolean fair) {
    this.lock = new ReentrantReadWriteLock(fair);
  }

  public GuardedReentrantReadWriteLock() {
    this(false);
  }

  /// Returns an AutoCloseable read lock guard. Usage:
  /// <pre>
  /// try (var ignored = guardedLock.readLock()) {
  ///   // read operations here
  /// }
  /// </pre>
  public ReadLockGuard readLock() {
    return new ReadLockGuard(lock.readLock());
  }

  /// Returns an AutoCloseable write lock guard. Usage:
  /// <pre>
  /// try (var ignored = guardedLock.writeLock()) {
  ///   // write operations here
  /// }
  /// </pre>
  public WriteLockGuard writeLock() {
    return new WriteLockGuard(lock.writeLock());
  }

  /// A read lock guard that automatically releases the lock when closed.
  public static final class ReadLockGuard implements AutoCloseable {
    private final Lock lock;

    ReadLockGuard(Lock lock) {
      this.lock = lock;
      lock.lock();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }

  /// A write lock guard that automatically releases the lock when closed.
  public static final class WriteLockGuard implements AutoCloseable {
    private final Lock lock;

    WriteLockGuard(Lock lock) {
      this.lock = lock;
      lock.lock();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
