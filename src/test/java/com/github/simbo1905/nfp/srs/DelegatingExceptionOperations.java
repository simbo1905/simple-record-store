package com.github.simbo1905.nfp.srs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/// A delegating FileOperations that throws exceptions at a specific operation count.
/// This allows controlled testing of exception handling and closed state behavior.
class DelegatingExceptionOperations extends AbstractDelegatingFileOperations {

  private static final Logger logger =
      Logger.getLogger(DelegatingExceptionOperations.class.getName());
  private boolean didThrow = false;

  public DelegatingExceptionOperations(FileOperations delegate, int throwAtOperation) {
    super(delegate, throwAtOperation);
    logger.log(
        Level.FINE,
        () ->
            String.format(
                "Created DelegatingExceptionOperations with throwAtOperation=%d",
                throwAtOperation));
  }

  @Override
  protected void handleTargetOperation() throws IOException {
    didThrow = true;
    logger.log(
        Level.FINE, () -> String.format("THROWING EXCEPTION at operation %d", operationCount));
    throw new IOException("Simulated exception at operation " + operationCount);
  }

  /// Check if an exception was thrown
  public boolean didThrow() {
    return didThrow;
  }
}
