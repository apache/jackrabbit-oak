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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof.
 */
public class Profiler implements Runnable {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");
    private static final int MAX_ELEMENTS = 1000;

    public int interval = 50;
    public int depth = 16;
    public boolean paused;

    String[] ignoreLines = arraySplit("", ',', true);
    String[] ignorePackages = arraySplit("java.", ',', true);
    String[] ignoreThreads = arraySplit(
            "java.lang.Thread.dumpThreads," +
                    "java.lang.Thread.getThreads," +
                    "java.net.PlainSocketImpl.socketAccept," +
                    "java.net.SocketInputStream.socketRead0," +
                    "java.net.SocketOutputStream.socketWrite0," +
                    "java.lang.UNIXProcess.waitForProcessExit," +
                    "java.lang.Object.wait," +
                    "java.lang.Thread.sleep," +
                    "sun.awt.windows.WToolkit.eventLoop," +
                    "sun.misc.Unsafe.park," +
                    "dalvik.system.VMStack.getThreadStackTrace," +
                    "dalvik.system.NativeStart.run"
            , ',', true);
    volatile boolean stop;
    int total;
    private HashMap<String, Integer> counts = new HashMap<String, Integer>();
    private HashMap<String, Integer> packages = new HashMap<String, Integer>();
    private int minCount = 1;
    private Thread thread;
    private long time;

    /**
     * Start collecting profiling data.
     */
    public void startCollecting() {
        thread = new Thread(this, "Profiler");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stop collecting.
     */
    public void stopCollecting() {
        stop = true;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            thread = null;
        }
    }

    @Override
    public void run() {
        time = System.currentTimeMillis();
        while (!stop) {
            try {
                tick();
            } catch (Throwable t) {
                break;
            }
        }
        time = System.currentTimeMillis() - time;
    }

    private void tick() {
        if (interval > 0) {
            if (paused) {
                return;
            }
            try {
                Thread.sleep(interval);
            } catch (Exception e) {
                // ignore
            }
        }
        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
            Thread t = entry.getKey();
            if (t.getState() != Thread.State.RUNNABLE) {
                continue;
            }
            StackTraceElement[] dump = entry.getValue();
            if (dump == null || dump.length == 0) {
                continue;
            }
            if (startsWithAny(dump[0].toString(), ignoreThreads)) {
                continue;
            }
            StringBuilder buff = new StringBuilder();
            // simple recursive calls are ignored
            String last = null;
            boolean packageCounts = false;
            for (int j = 0, i = 0; i < dump.length && j < depth; i++) {
                String el = dump[i].toString();
                if (!el.equals(last) && !startsWithAny(el, ignoreLines)) {
                    last = el;
                    buff.append("at ").append(el);
                    if (!packageCounts && !startsWithAny(el, ignorePackages)) {
                        packageCounts = true;
                        int index = 0;
                        for (; index < el.length(); index++) {
                            char c = el.charAt(index);
                            if (Character.isUpperCase(c) || c == '(') {
                                break;
                            }
                        }
                        if (index > 0 && el.charAt(index - 1) == '.') {
                            index--;
                        }
                        String packageName = el.substring(0, index);
                        increment(packages, packageName, 0);
                        while (true) {
                            index = packageName.lastIndexOf('.');
                            if (index < 0) {
                                break;
                            }
                            packageName = packageName.substring(0, index);
                            increment(packages, packageName + ".*", 0);
                        }
                        buff.append('+');
                    }
                    buff.append(LINE_SEPARATOR);
                    j++;
                }
            }
            if (buff.length() > 0) {
                minCount = increment(counts, buff.toString().trim(), minCount);
                total++;
            }
        }
    }

    private static boolean startsWithAny(String s, String[] prefixes) {
        for (String p : prefixes) {
            if (p.length() > 0 && s.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    private static int increment(Map<String, Integer> map, String trace, int minCount) {
        Integer oldCount = map.get(trace);
        if (oldCount == null) {
            map.put(trace, 1);
        } else {
            map.put(trace, oldCount + 1);
        }
        while (map.size() > MAX_ELEMENTS) {
            for (Iterator<Map.Entry<String, Integer>> ei = map.entrySet().iterator(); ei.hasNext();) {
                Map.Entry<String, Integer> e = ei.next();
                if (e.getValue() <= minCount) {
                    ei.remove();
                }
            }
            if (map.size() > MAX_ELEMENTS) {
                minCount++;
            }
        }
        return minCount;
    }

    /**
     * Get the top stack traces.
     *
     * @param count the maximum number of stack traces
     * @return the stack traces.
     */
    public String getTop(int count) {
        stopCollecting();
        StringBuilder buff = new StringBuilder();
        buff.append("Profiler Results").
                append(LINE_SEPARATOR).
                append(LINE_SEPARATOR).
                append("Package summary:").
                append(LINE_SEPARATOR);
        for (String k : new TreeSet<String>(packages.keySet())) {
            int percent = 100 * packages.get(k) / Math.max(total, 1);
            if (percent >= 5) {
                if (percent < 10) {
                    buff.append('0');
                }
                buff.append(percent).append("%: ").append(k).
                        append(LINE_SEPARATOR);
            }
        }
        buff.append(LINE_SEPARATOR).
                append("Top ").append(count).append(" stack trace(s) of ").append(time).
                append(" ms ").append(LINE_SEPARATOR);
        if (counts.size() == 0) {
            buff.append("(none)");
        }
        appendTop(buff, counts, count, total);
        buff.append('.');
        return buff.toString();
    }

    private static void appendTop(StringBuilder buff, HashMap<String, Integer> map, int count, int total) {
        for (int x = 0, min = 0;;) {
            int highest = 0;
            Map.Entry<String, Integer> best = null;
            for (Map.Entry<String, Integer> el : map.entrySet()) {
                if (el.getValue() > highest || (el.getValue() == highest && el.getKey().length() < best.getKey().length())) {
                    best = el;
                    highest = el.getValue();
                }
            }
            if (best == null) {
                break;
            }
            map.remove(best.getKey());
            if (++x >= count) {
                if (best.getValue() < min) {
                    break;
                }
                min = best.getValue();
            }
            int c = best.getValue();
            int percent = 100 * c / Math.max(total, 1);
            buff.append(c).append('/').append(total).append(" (").
                    append(percent).
                    append("%):").append(LINE_SEPARATOR).
                    append(best.getKey()).
                    append(LINE_SEPARATOR);
        }
    }

    /**
     * Split a string into an array of strings using the given separator. A null
     * string will result in a null array, and an empty string in a zero element
     * array.
     *
     * @param s             the string to split
     * @param separatorChar the separator character
     * @param trim          whether each element should be trimmed
     * @return the array list
     */
    public static String[] arraySplit(String s, char separatorChar, boolean trim) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return new String[0];
        }
        ArrayList<String> list = new ArrayList<String>();
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == separatorChar) {
                String e = buff.toString();
                list.add(trim ? e.trim() : e);
                buff.setLength(0);
            } else if (c == '\\' && i < s.length() - 1) {
                buff.append(s.charAt(++i));
            } else {
                buff.append(c);
            }
        }
        String e = buff.toString();
        list.add(trim ? e.trim() : e);
        String[] array = new String[list.size()];
        list.toArray(array);
        return array;
    }

}

