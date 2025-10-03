package com.github.trex_paxos.srs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class TracePlayback {
    private static final Logger logger = Logger.getLogger(TracePlayback.class.getName());
    static final String TMP = System.getProperty("java.io.tmpdir")+ FileSystems.getDefault().getSeparator();

    //static Pattern insertPattern = Pattern.compile("value.len:([0-9]+) key:\\[ ([x0-9A-F ]+) \\]");
    static Pattern keyPattern = Pattern.compile("key:\\[ ([x0-9A-F ]+) ]");
    static Pattern lenPattern = Pattern.compile("value.len:([0-9]+)");

    private static int toDigit(char hexChar) {
        int digit = Character.digit(hexChar, 16);
        if(digit == -1) {
            throw new IllegalArgumentException(
                    "Invalid Hexadecimal Character: "+ hexChar);
        }
        return digit;
    }

    public static void main(String[] args) throws Exception {
        final var initialLength = Integer.parseInt(args[0]);
        final var keyMax = Integer.parseInt(args[1]);
        final var file = args[2];
        final var f = new File(TMP+"playback");
        if( f.exists() ) //noinspection ResultOfMethodCallIgnored
          f.delete();
        logger.log(Level.FINE, String.format("initialLength: %d, keyMax: %d, file: %s%n", initialLength, keyMax, file));
        final var counter = new AtomicInteger(0);
        try (FileRecordStore recordStore = new FileRecordStore.Builder()
                .path(TMP + "playback")
                .preallocatedRecords(initialLength)
                .maxKeyLength(keyMax)
                .open();
             Scanner in = new Scanner(new FileInputStream(file))) {
            while (in.hasNextLine()) {
                String l = in.nextLine();
                if (l.startsWith("FINE: ")) {
                    final var keyMatcher = keyPattern.matcher(l);
                    if (keyMatcher.find()) {
                        final var octalString = keyMatcher.group(1);
                        final var octalValues = octalString.split(" ");
                        byte[] key = new byte[octalValues.length];
                        int index = 0;
                        for (String o : octalValues) {
                            final var o2 = o.substring(2);
                            int firstDigit = toDigit(o2.charAt(0));
                            int secondDigit = toDigit(o2.charAt(1));
                            final var b = (byte) ((firstDigit << 4) + secondDigit);
                            key[index] = b;
                            index++;
                        }
                        final var k = ByteSequence.of(key);
                        try {
                            if (l.startsWith("FINE: deleteRecord")) {
                                recordStore.deleteRecord(k);
                                return;
                            }
                            final var lenMatcher = lenPattern.matcher(l);
                            if (lenMatcher.find()) {
                                final var len = lenMatcher.group(1);
                                byte[] value = new byte[Integer.parseInt(len)];
                                if (l.startsWith("FINE: insertRecord"))
                                    recordStore.insertRecord(ByteSequence.of(key), value);
                                else if (l.startsWith("FINE: updateRecord")) {
                                    recordStore.updateRecord(ByteSequence.of(key), value);
                                }
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    }
                    counter.incrementAndGet();
                }
            }
            logger.log(Level.FINE, "fine counter "+counter.get());
        }
    }
}
