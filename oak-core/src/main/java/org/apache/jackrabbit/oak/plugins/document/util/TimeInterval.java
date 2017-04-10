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
package org.apache.jackrabbit.oak.plugins.document.util;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A class representing a time interval, with utility methods to derive related
 * intervals, check time stamps for containment, etc.
 */
public class TimeInterval {

    public final long fromMs;
    public final long toMs;

    public TimeInterval(long fromMs, long toMs) {
        checkArgument(fromMs <= toMs, "start must be <= end");
        this.fromMs = fromMs;
        this.toMs = toMs;
    }

    /**
     * Shortens the interval to the specified end value, if is contained in the
     * interval. Returns a single point in time interval when the specified time
     * is outside the time interval. Return unchanged internal if specified
     * value beyond end.
     */
    public TimeInterval notLaterThan(long timestampMs) {
        if (timestampMs < toMs) {
            return new TimeInterval((timestampMs < fromMs) ? timestampMs : fromMs, timestampMs);
        } else {
            return this;
        }
    }

    /**
     * Shortens the interval to the specified start value, if is contained in
     * the interval. Returns a single point in time interval when the specified
     * time is outside the time interval. Return unchanged internal if specified
     * value before start.
     */
    public TimeInterval notEarlierThan(long timestampMs) {
        if (fromMs < timestampMs) {
            return new TimeInterval(timestampMs, (timestampMs > toMs) ? timestampMs : toMs);
        } else {
            return this;
        }
    }

    /**
     * Returns a new interval starting at the previous time, but ending after
     * the specified duration.
     */
    public TimeInterval startAndDuration(long durationMs) {
        return new TimeInterval(fromMs, fromMs + durationMs);
    }

    /**
     * Returns the duration in ms.
     */
    public long getDurationMs() {
        return toMs - fromMs;
    }

    /**
     * Checks whether the interval contains the given time stamp.
     */
    public boolean contains(long timestampMs) {
        return fromMs <= timestampMs && timestampMs <= toMs;
    }

    /**
     * Checks whether the interval end after the given time stamp.
     */
    public boolean endsAfter(long timestampMs) {
        return toMs > timestampMs;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimeInterval && o != null) {
            return fromMs == ((TimeInterval) o).fromMs && toMs == ((TimeInterval) o).toMs;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (int)(fromMs^(fromMs>>>32)^toMs^(toMs>>>32));
    }

    @Override
    public String toString() {
        return "[" + Utils.timestampToString(fromMs) + ", " + Utils.timestampToString(toMs) + "]";
    }
}
