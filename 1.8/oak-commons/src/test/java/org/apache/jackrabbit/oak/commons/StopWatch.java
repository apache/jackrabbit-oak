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

/**
 * A utility class to time an operation.
 */
public class StopWatch {

    private static final long NANOS_PER_SECOND = 1000 * 1000 * 1000;

    private long start = System.nanoTime();
    private long lastLog = start;

    public long time() {
        return System.nanoTime() - start;
    }

    public String seconds() {
        double s = (double) time() / NANOS_PER_SECOND;
        return String.format("%.2f seconds", s);
    }

    public String operationsPerSecond(int operations) {
        long t = time();
        double s = (double) t / NANOS_PER_SECOND;
        if (t == 0) {
            t = 1;
        }
        int ops = (int) (operations * NANOS_PER_SECOND / t);
        return String.format("%.2f seconds (%d ops; %d op/s)", s, operations, ops);
    }

    /**
     * Returns true once 5 seconds.
     *
     * @return true once every 5 seconds
     */
    public boolean log() {
        long t = System.nanoTime();
        if (t - lastLog > 5 * NANOS_PER_SECOND) {
            lastLog = t;
            return true;
        }
        return false;
    }

    public void reset() {
        start = System.nanoTime();
    }

}
