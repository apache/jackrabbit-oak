/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.threadDump;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * A tool that removes uninteresting lines from stack traces.
 */
public class ThreadDumpCleaner {

    private static final String[] PATTERN_ARRAY = {
        "\"Concurrent Mark-Sweep GC Thread\".*\n",

        "\"Exception Catcher Thread\".*\n",

        "JNI global references:.*\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\n",

        "\".*?\".*\n\n",

        "\\$\\$YJP\\$\\$",

        "\"(Attach|Service|VM|GC|DestroyJavaVM|Signal|AWT|AppKit|C2 |Low Mem|" +
                "process reaper|YJPAgent-).*?\"(?s).*?\n\n",

        "   Locked ownable synchronizers:(?s).*?\n\n",

        "   Locked synchronizers:(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State: (TIMED_)?WAITING(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.KQueueArrayWrapper.kevent0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.io.FileInputStream.readBytes(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.ServerSocketChannelImpl.accept(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.DualStackPlainSocketImpl.accept0(?s).*\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.EPollArrayWrapper.epollWait(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.lang.Object.wait(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.PlainSocketImpl.socketAccept(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.SocketInputStream.socketRead0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.WindowsSelectorImpl\\$SubSelector.poll0(?s).*?\n\n",
                
        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.management.ThreadImpl.dumpThreads0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.misc.Unsafe.park(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.PlainSocketImpl.socketClose0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.PlainSocketImpl.socketAvailable(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.PlainSocketImpl.socketConnect(?s).*?\n\n",

        "<EndOfDump>\n\n",

    };

    private static ArrayList<Pattern> PATTERNS = new ArrayList<Pattern>();

    static {
        for (String s : PATTERN_ARRAY) {
            PATTERNS.add(Pattern.compile(s));
        }
    }
    
    public static File process(File file) throws IOException {
        String fileName = file.getName();
        if (fileName.endsWith(".txt")) {
            fileName = fileName.substring(0, fileName.length() - ".txt".length());
        }
        File target = new File(file.getParentFile(), fileName + ".filtered.txt");
        PrintWriter writer = new PrintWriter(new BufferedWriter(
                new FileWriter(target)));       
        try {
            processFile(file, writer);
        } finally {
            writer.close();
        }
        return target;
    }
    
    private static void processFile(File file, PrintWriter writer) throws IOException {
        LineNumberReader r = new LineNumberReader(new BufferedReader(
                new FileReader(file)));
        try {
            process(r, writer);
        } finally {
            r.close();
        }
    }

    private static void process(LineNumberReader reader, PrintWriter writer) throws IOException {
        StringBuilder buff = new StringBuilder();
        int activeThreadCount = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("Full thread dump") || line.startsWith("Full Java thread dump")) {
                if (activeThreadCount > 0) {
                    System.out.println("Active threads: " + activeThreadCount);
                }
                activeThreadCount = 0;
            }
            buff.append(line).append('\n');
            if (line.trim().length() == 0) {
                String filtered = filter(buff.toString());
                if (filtered.trim().length() > 10) {
                    activeThreadCount++;
                }
                writer.print(filtered);
                buff = new StringBuilder();
            }
        }
        writer.println(filter(buff.toString()));
    }

    private static String filter(String s) {
        for (Pattern p : PATTERNS) {
            s = p.matcher(s).replaceAll("");
        }
        return s;
    }

}
