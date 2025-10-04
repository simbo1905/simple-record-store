package com.github.simbo1905.nfp.srs;

import java.io.OutputStream;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/// Sets up JUL console logging to use stdout so Maven doesn't flag FINE/FINEST
/// output as warnings. All tests extend this class to inherit the configuration.
public abstract class JulLoggingConfig {

    protected final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(getClass().getName());

    static {
        configureJulLogging();
    }

    private static void configureJulLogging() {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tT %4$s %2$s %5$s%6$s%n");

        String desiredLevel = System.getProperty("com.github.trex_paxos.srs.testLogLevel", "INFO");
        Level targetLevel;
        try {
            targetLevel = Level.parse(desiredLevel.toUpperCase());
        } catch (IllegalArgumentException ex) {
            targetLevel = Level.INFO;
        }

        Logger root = Logger.getLogger("");
        root.setUseParentHandlers(false);
        for (Handler handler : root.getHandlers()) {
            root.removeHandler(handler);
        }

        Handler stdoutHandler = new StdoutHandler(System.out);
        stdoutHandler.setLevel(targetLevel);
        stdoutHandler.setFormatter(new SimpleFormatter());
        root.addHandler(stdoutHandler);
        root.setLevel(targetLevel);
    }

    private static final class StdoutHandler extends StreamHandler {
        StdoutHandler(OutputStream stream) {
            super(stream, new SimpleFormatter());
        }

        @Override
        public synchronized void publish(LogRecord record) {
            super.publish(record);
            flush();
        }

        @Override
        public synchronized void close() throws SecurityException {
            flush();
        }
    }
}
