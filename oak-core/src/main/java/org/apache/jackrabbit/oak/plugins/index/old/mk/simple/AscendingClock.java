/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

/**
 * A clock that normally returns the current system time since 1970, and is
 * guaranteed to return ascending values. It returns ascending values even if
 * the system time is changed, and never the same value twice. Worst case, it
 * returns incrementing values. Unique nanosecond values are returned for system
 * times between the years 1970 and 2554.
 */
class AscendingClock {

    /**
     * The offset between System.nanoTime() (which returns elapsed time) and the
     * absolute current system time in nanoseconds since 1970 UTC.
     */
    private long nanoOffset;

    /**
     * The last millisecond value returned.
     */
    private long lastMillis;

    /**
     * The last nanosecond value returned.
     */
    private long lastNanos;


    /**
     * Create a new clock.
     *
     * @param lastMillis the time (the next returned value will be at least one
     *            bigger)
     */
    public AscendingClock(long lastMillis) {
        this.lastMillis = lastMillis;
        this.lastNanos = lastMillis * 1000000;
        resetNanoOffset();
    }

    /**
     * Get the current time, ensuring that the value is always larger than the
     * last returned time, even if the system time was changed. Worst case, this
     * method will return the last returned value plus one.
     *
     * @return the time in nanoseconds since 1970-01-01 (UTC)
     */
    public synchronized long nanoTime() {
        long nanos = System.nanoTime();
        long now = nanoOffset + nanos;
        if (nanos > lastNanos + 1000000000) {
            // re-sync every second, in case
            // the system time was changed
            resetNanoOffset();
            now = nanoOffset + nanos;
        }
        if (now > lastNanos) {
            lastNanos = now;
        } else {
            now = ++lastNanos;
        }
        return now;
    }

    private void resetNanoOffset() {
        nanoOffset = System.currentTimeMillis() * 1000000 - System.nanoTime();
    }

    /**
     * Get the current time, ensuring that the value is always larger than the
     * last returned time, even if the system time was changed. Worst case, this
     * method will return the last returned value plus one.
     *
     * @return the time in milliseconds since 1970-01-01 (UTC)
     */
    public synchronized long time() {
        long now = System.currentTimeMillis();
        if (now > lastMillis) {
            lastMillis = now;
        } else {
            now = ++lastMillis;
        }
        return now;
    }

}
