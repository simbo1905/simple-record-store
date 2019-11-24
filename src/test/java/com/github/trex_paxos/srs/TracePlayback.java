package com.github.trex_paxos.srs;

import lombok.val;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TracePlayback {
    static final String TMP = System.getProperty("java.io.tmpdir")+System.getProperty("file.separator");

    //static Pattern insertPattern = Pattern.compile("value.len:([0-9]+) key:\\[ ([x0-9A-F ]+) \\]");
    static Pattern keyPattern = Pattern.compile("key:\\[ ([x0-9A-F ]+) \\]");
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
        val initialLength = Integer.parseInt(args[0]);
        val keyMax = Integer.parseInt(args[1]);
        val file = args[2];
        val f = new File(TMP+"playback");
        if( f.exists() ) f.delete();
        System.out.println(String.format("initialLength: %d, keyMax: %d, file: %s", initialLength, keyMax, file));
        val recordStore = new FileRecordStore(TMP+"playback", initialLength, keyMax, false);
        Scanner in = new Scanner(new FileInputStream(file));
        val counter = new AtomicInteger(0);
        try {
            Stream.generate(in::nextLine).forEach(l -> {
                if (l.startsWith("FINE: ")) {
                    val keyMatcher = keyPattern.matcher(l);
                    if (keyMatcher.find()) {
                        val octalString = keyMatcher.group(1);
                        val octals = octalString.split(" ");
                        byte[] key = new byte[octals.length];
                        int index = 0;
                        for (String o : octals) {
                            val o2 = o.substring(2);
                            int firstDigit = toDigit(o2.charAt(0));
                            int secondDigit = toDigit(o2.charAt(1));
                            val b = (byte) ((firstDigit << 4) + secondDigit);
                            key[index] = b;
                            index++;
                        }
                        val k = ByteSequence.of(key);
                        try {
                            if (l.startsWith("FINE: deleteRecord")) {
                                recordStore.deleteRecord(k);
                                return;
                            }
                            val lenMatcher = lenPattern.matcher(l);
                            if (lenMatcher.find()) {
                                val len = lenMatcher.group(1);
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
            });
        } finally {
            System.err.println("fine counter "+counter.get());
        }
    }
}
