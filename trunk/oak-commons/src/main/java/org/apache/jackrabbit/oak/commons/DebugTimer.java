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
package org.apache.jackrabbit.oak.commons;

import java.util.LinkedList;
import java.util.List;

/**
 * {@code DebugTimer}...
 */
public class DebugTimer {

    private static String[] units = {"ns", "us", "ms", "s"};

    private List<TimeStamp> timestamps = new LinkedList<TimeStamp>();

    private long now;

    public DebugTimer() {
        now = System.nanoTime();
    }

    public void mark(String msg) {
        long then = now;
        now = System.nanoTime();
        timestamps.add(new TimeStamp(now-then, msg));
    }

    public String getString() {
        if (timestamps.isEmpty()) {
            return "";
        }
        StringBuilder b = new StringBuilder();
        for (TimeStamp t: timestamps) {
            if (b.length() > 0) {
                b.append(", ");
            } else {
                b.append('(');
            }
            int u = 0;
            double time = t.time;
            while (time > 1000 && u<units.length-1) {
                time = time / 1000;
                u++;
            }
            b.append(String.format("%s=%.2f%s", t.msg, time, units[u]));
        }
        return b.append(')').toString();
    }

    private static final class TimeStamp {

        private final long time;

        private final String msg;

        private TimeStamp(long time, String msg) {
            this.time = time;
            this.msg = msg;
        }
    }
}