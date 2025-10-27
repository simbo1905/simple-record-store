package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

/// FIFO wrapper around FileRecordStore providing queue semantics with crash-safe guarantees.
/// Single-threaded design suitable for actor/isolate environments.
///
/// Key features:
/// - 128-bit keys (64-bit counter + 64-bit timestamp) for strict FIFO ordering
/// - Genesis record stores queue metadata (high/low water marks, counters)
/// - Crash-safe batch operations with proper write ordering
/// - Automatic recovery from genesis record on restart
public class FifoRecordStore implements AutoCloseable {

  private static final byte[] GENESIS_KEY = new byte[16]; // All zeros sentinel key
  private static final int KEY_SIZE = 16; // 128 bits

  private final FileRecordStore store;
  private FifoStats stats;

  /// Private constructor - use builder pattern
  private FifoRecordStore(FileRecordStore store) throws IOException {
    this.store = store;
    recoverState();
  }

  /// Creates a new FifoRecordStore wrapping the given FileRecordStore.
  /// Recovers state from genesis record if it exists, otherwise initializes new queue.
  public static FifoRecordStore open(Path filePath) throws IOException {
    FileRecordStore store =
        new FileRecordStoreBuilder()
            .path(filePath)
            .maxKeyLength(KEY_SIZE)
            .preallocatedRecords(1024) // Pre-allocate space for better performance
            .open();
    return new FifoRecordStore(store);
  }

  /// Recover queue state from genesis record or initialize new queue
  private void recoverState() throws IOException {
    if (store.recordExists(GENESIS_KEY)) {
      // Genesis record exists - recover state
      byte[] genesisData = store.readRecordData(GENESIS_KEY);
      this.stats = parseGenesisRecord(genesisData);
    } else {
      // Genesis record doesn't exist - initialize new queue
      initializeNewQueue();
    }
  }

  /// Initialize a new empty queue
  private void initializeNewQueue() throws IOException {
    this.stats = new FifoStats(0, 0, 0, 0, 0, 0, 0, 1);
    persistGenesisRecord();
  }

  /// Parse genesis record JSON format
  private FifoStats parseGenesisRecord(byte[] data) {
    String json = new String(data, StandardCharsets.UTF_8);
    // Simple JSON parsing without external dependencies
    long highWaterMark = extractLong(json, "highWaterMark");
    long lowWaterMark = extractLong(json, "lowWaterMark");
    long lastPutTime = extractLong(json, "lastPutTime");
    long lastTakeTime = extractLong(json, "lastTakeTime");
    long totalPutCount = extractLong(json, "totalPutCount");
    long totalTakeCount = extractLong(json, "totalTakeCount");
    long nextCounter = extractLong(json, "nextCounter");
    long queueSize = extractLong(json, "queueSize");

    return new FifoStats(
        highWaterMark,
        lowWaterMark,
        lastPutTime,
        lastTakeTime,
        totalPutCount,
        totalTakeCount,
        queueSize,
        nextCounter);
  }

  /// Simple JSON field extractor
  private long extractLong(String json, String field) {
    String pattern = "\"" + field + "\":";
    int start = json.indexOf(pattern);
    if (start == -1) return 0;
    start += pattern.length();
    int end = start;
    while (end < json.length()
        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
      end++;
    }
    if (start == end) return 0;
    return Long.parseLong(json.substring(start, end).trim());
  }

  /// Serialize genesis record to JSON
  private byte[] serializeGenesisRecord(FifoStats stats) {
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"version\":1,");
    json.append("\"highWaterMark\":").append(stats.getHighWaterMark()).append(",");
    json.append("\"lowWaterMark\":").append(stats.getLowWaterMark()).append(",");
    json.append("\"lastPutTime\":").append(stats.getLastPutTime()).append(",");
    json.append("\"lastTakeTime\":").append(stats.getLastTakeTime()).append(",");
    json.append("\"totalPutCount\":").append(stats.getTotalPutCount()).append(",");
    json.append("\"totalTakeCount\":").append(stats.getTotalTakeCount()).append(",");
    json.append("\"nextCounter\":").append(stats.getNextCounter()).append(",");
    json.append("\"queueSize\":").append(stats.getCurrentSize());
    json.append("}");
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }

  /// Persist current stats to genesis record
  private void persistGenesisRecord() throws IOException {
    byte[] data = serializeGenesisRecord(stats);
    if (store.recordExists(GENESIS_KEY)) {
      store.updateRecord(GENESIS_KEY, data);
    } else {
      store.insertRecord(GENESIS_KEY, data);
    }
  }

  /// Generate next key with counter and timestamp
  private byte[] generateKey() {
    long counter = stats.getNextCounter();
    long timestamp = System.currentTimeMillis();
    ByteBuffer buffer = ByteBuffer.allocate(KEY_SIZE);
    buffer.putLong(counter);
    buffer.putLong(timestamp);
    return buffer.array();
  }

  /// Extract counter from key
  private long getCounterFromKey(byte[] key) {
    ByteBuffer buffer = ByteBuffer.wrap(key);
    return buffer.getLong();
  }

  /// Add item to queue
  public void put(byte[] data) throws IOException {
    byte[] key = generateKey();

    // Step 1: Write item to store first
    store.insertRecord(key, data);

    // Step 2: Update genesis record (commit point) - only after item is written
    long newSize = stats.getCurrentSize() + 1;
    long newHighWaterMark = Math.max(stats.getHighWaterMark(), newSize);
    long newLowWaterMark =
        (stats.getLowWaterMark() == 0) ? newSize : Math.min(stats.getLowWaterMark(), newSize);

    this.stats =
        new FifoStats(
            newHighWaterMark,
            newLowWaterMark,
            System.currentTimeMillis(),
            stats.getLastTakeTime(),
            stats.getTotalPutCount() + 1,
            stats.getTotalTakeCount(),
            newSize,
            stats.getNextCounter() + 1);

    persistGenesisRecord();
  }

  /// Batch put operation with correct write ordering
  public void putBatch(List<byte[]> items) throws IOException {
    if (items.isEmpty()) return;

    List<byte[]> keys = new ArrayList<>();

    // Step 1: Write all items first
    for (byte[] item : items) {
      byte[] key = generateKey();
      keys.add(key);
      store.insertRecord(key, item);
      // Increment counter for next key
      this.stats =
          new FifoStats(
              stats.getHighWaterMark(),
              stats.getLowWaterMark(),
              stats.getLastPutTime(),
              stats.getLastTakeTime(),
              stats.getTotalPutCount(),
              stats.getTotalTakeCount(),
              stats.getCurrentSize(),
              stats.getNextCounter() + 1);
    }

    // Step 2: Update genesis record AFTER all items are written (commit point)
    long newSize = stats.getCurrentSize() + items.size();
    long newHighWaterMark = Math.max(stats.getHighWaterMark(), newSize);
    long newLowWaterMark =
        (stats.getLowWaterMark() == 0) ? newSize : Math.min(stats.getLowWaterMark(), newSize);

    this.stats =
        new FifoStats(
            newHighWaterMark,
            newLowWaterMark,
            System.currentTimeMillis(),
            stats.getLastTakeTime(),
            stats.getTotalPutCount() + items.size(),
            stats.getTotalTakeCount(),
            newSize,
            stats.getNextCounter());

    persistGenesisRecord();
  }

  /// Remove and return item from head of queue
  public byte[] take() throws IOException {
    if (isEmpty()) {
      throw new NoSuchElementException("Queue is empty");
    }

    // Find smallest counter key (FIFO head)
    byte[] headKey = findHeadKey();
    if (headKey == null) {
      throw new NoSuchElementException("Queue is empty");
    }

    // Read data
    byte[] data = store.readRecordData(headKey);

    // Delete record
    store.deleteRecord(headKey);

    // Update genesis record
    long newSize = stats.getCurrentSize() - 1;
    long newLowWaterMark = (newSize == 0) ? 0 : Math.min(stats.getLowWaterMark(), newSize);

    this.stats =
        new FifoStats(
            stats.getHighWaterMark(),
            newLowWaterMark,
            stats.getLastPutTime(),
            System.currentTimeMillis(),
            stats.getTotalPutCount(),
            stats.getTotalTakeCount() + 1,
            newSize,
            stats.getNextCounter());

    persistGenesisRecord();

    return data;
  }

  /// Peek at head of queue without removing
  public byte[] peek() throws IOException {
    if (isEmpty()) {
      return null;
    }

    byte[] headKey = findHeadKey();
    if (headKey == null) {
      return null;
    }

    return store.readRecordData(headKey);
  }

  /// Find the key with smallest counter (FIFO head)
  private byte[] findHeadKey() {
    long minCounter = Long.MAX_VALUE;
    byte[] headKey = null;

    for (byte[] key : store.keysBytes()) {
      // Skip genesis key
      if (Arrays.equals(key, GENESIS_KEY)) {
        continue;
      }

      long counter = getCounterFromKey(key);
      if (counter < minCounter) {
        minCounter = counter;
        headKey = key;
      }
    }

    return headKey;
  }

  /// Check if queue is empty
  public boolean isEmpty() {
    return stats.getCurrentSize() == 0;
  }

  /// Get current queue size
  public int size() {
    return (int) stats.getCurrentSize();
  }

  /// Get current statistics
  public FifoStats getStats() {
    return stats;
  }

  @Override
  public void close() throws Exception {
    store.close();
  }
}
