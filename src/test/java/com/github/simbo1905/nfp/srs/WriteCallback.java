package com.github.simbo1905.nfp.srs;

import java.io.IOException;

public interface WriteCallback {

  void onWrite() throws IOException;

  /**
   * Default implementation for onRead that does nothing.
   * Implementations can override this to handle read operations.
   */
  default void onRead() {
    // Default: do nothing
  }
}
