/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A UUID implementation.
 *
 * It supports creating version 7 UUIDs, which are time-ordered.
 * See also draft-ietf-uuidrev-rfc4122bis-00
 *
 * Unlike java.util.UUID, the comparison is unsigned,
 * so that the string comparison yields the same result.
 */
public class TimeUuid implements Comparable<TimeUuid> {

    private static final AtomicLong UUID_LAST_MILLIS_AND_COUNT = new AtomicLong(0);

    private static final SecureRandom RANDOM = new SecureRandom();

    // most significant bits
    private final long msb;

    // least significant bits
    private final long lsb;

    public TimeUuid(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
    }

    @Override
    public String toString() {
        return String.format("%08x-%04x-%04x-%04x-%012x",
                msb >>> 32, (msb >>> 16) & 0xffff, msb & 0xffff,
                (lsb >>> 48) & 0xffff, lsb & 0xffffffffffffL);
    }

    public String toShortString() {
        return String.format("%012x%03x%016x",
                getTimestampPart(), getCounterPart(), getRandomPart());
    }

    public String toHumanReadableString() {
        Instant instant = Instant.ofEpochMilli(getTimestampPart());
        return String.format("%s %03x %016x",
                instant.toString(), getCounterPart(), getRandomPart());
    }

    /**
     * Get the timestamp part (48 bits).
     *
     * @return the timestamp part
     */
    public long getTimestampPart() {
        return msb >>> 16;
    }

    /**
     * Get the counter part (12 bits).
     *
     * @return counter part
     */
    public long getCounterPart() {
        return msb & ((1L << 12) - 1);
    }

    /**
     * Get the random part (62 bits).
     * The first 2 bits are fixed.
     *
     * @return the random part
     */
    public long getRandomPart() {
        return lsb;
    }

    /**
     * Unlike java.util.UUID, the comparison is unsigned,
     * so that the string comparison yields the same result.
     */
    @Override
    public int compareTo(TimeUuid o) {
        if (o.msb != msb) {
            return Long.compareUnsigned(msb, o.msb);
        }
        return Long.compareUnsigned(lsb, o.lsb);
    }

    @Override
    public int hashCode() {
        long x = lsb ^ msb;
        return (int) ((x >>> 32) ^ x);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TimeUuid other = (TimeUuid) obj;
        return lsb == other.lsb && msb == other.msb;
    }

    /**
     * Get the next timestamp (in milliseconds) and counter.
     * The lowest 12 bits of the returned value is the counter.
     * The milliseconds are shifted by 12 bits.
     *
     * @param now the milliseconds
     * @param lastMillisAndCount the last returned value
     * @return the new value
     */
    public static long getMillisAndCountIncreasing(long now, AtomicLong lastMillisAndCount) {
        long result = now << 12;
        while (true) {
            long last = lastMillisAndCount.get();
            if (result <= last) {
                // ensure it is non-decrementing
                result = last + 1;
            }
            long got = lastMillisAndCount.compareAndExchange(last, result);
            if (got == last) {
                return result;
            }
        }
    }

    static TimeUuid newUuid(long millisAndCount,
            long random) {
        long millis = millisAndCount >>> 12;
        long counter = millisAndCount & ((1L << 12) - 1);
        long version = 7;
        long variant = 2;
        long msb = (millis << 16) | (version << 12) | counter;
        long lsb = (variant << 62) | (random & ((1L << 62) - 1));
        return new TimeUuid(msb, lsb);
    }

    public static TimeUuid newUuid() {
        long millisAndCount = getMillisAndCountIncreasing(
                System.currentTimeMillis(),
                UUID_LAST_MILLIS_AND_COUNT);
        long random = RANDOM.nextLong();
        return newUuid(millisAndCount, random);
    }

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

}
