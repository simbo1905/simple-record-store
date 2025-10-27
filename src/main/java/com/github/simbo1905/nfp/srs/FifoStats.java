package com.github.simbo1905.nfp.srs;

/// Statistics and metadata for FIFO queue operations.
/// Immutable value object capturing queue state at a point in time.
public class FifoStats {
  private final long highWaterMark;
  private final long lowWaterMark;
  private final long lastPutTime;
  private final long lastTakeTime;
  private final long totalPutCount;
  private final long totalTakeCount;
  private final long currentSize;
  private final long nextCounter;

  /// Constructor for FifoStats
  public FifoStats(
      long highWaterMark,
      long lowWaterMark,
      long lastPutTime,
      long lastTakeTime,
      long totalPutCount,
      long totalTakeCount,
      long currentSize,
      long nextCounter) {
    this.highWaterMark = highWaterMark;
    this.lowWaterMark = lowWaterMark;
    this.lastPutTime = lastPutTime;
    this.lastTakeTime = lastTakeTime;
    this.totalPutCount = totalPutCount;
    this.totalTakeCount = totalTakeCount;
    this.currentSize = currentSize;
    this.nextCounter = nextCounter;
  }

  public long getHighWaterMark() {
    return highWaterMark;
  }

  public long getLowWaterMark() {
    return lowWaterMark;
  }

  public long getLastPutTime() {
    return lastPutTime;
  }

  public long getLastTakeTime() {
    return lastTakeTime;
  }

  public long getTotalPutCount() {
    return totalPutCount;
  }

  public long getTotalTakeCount() {
    return totalTakeCount;
  }

  public long getCurrentSize() {
    return currentSize;
  }

  public long getNextCounter() {
    return nextCounter;
  }

  /// Calculate average delay in milliseconds
  public long getAverageDelay() {
    if (totalTakeCount == 0) {
      return 0;
    }
    // Simplified average based on time range
    if (lastTakeTime > lastPutTime) {
      return (lastTakeTime - lastPutTime) / Math.max(1, totalTakeCount);
    }
    return 0;
  }

  @Override
  public String toString() {
    return String.format(
        "FifoStats[size=%d, hwm=%d, lwm=%d, put=%d, take=%d, avgDelay=%dms]",
        currentSize, highWaterMark, lowWaterMark, totalPutCount, totalTakeCount, getAverageDelay());
  }
}
