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
 *
 */

package org.apache.jackrabbit.oak.segment.file.tar;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

/**
 * Instances of this class represent the garbage collection generation related
 * information of a segment. Each generation consists of a full and a tail part
 * and a tail flag. The full and tail part are each increased by the respective
 * garbage collection process. In the tail compaction case the segments written
 * by the compactor will also have their tail flag set so cleanup can recognise
 * them as not reclaimable (unless the full part is older then the number of
 * retained generations). Segments written by normal repository writes will
 * inherit the full and tail generations parts of the segments written by the
 * previous compaction process. However the tail flag is never set for such
 * segments ensuring cleanup after subsequent tail compactions can reclaim them
 * once old enough (e.g. the tail part of the generation is older then the
 * number of retained generations).
 */
public final class GCGeneration {

    public static final GCGeneration NULL = new GCGeneration(0, 0, false);

    public static GCGeneration newGCGeneration(int full, int tail, boolean isTail) {
        return new GCGeneration(full, tail, isTail);
    }

    private final int full;

    private final int tail;

    private final boolean isTail;

    private GCGeneration(int full, int tail, boolean isTail) {
        this.full = full;
        this.tail = tail;
        this.isTail = isTail;
    }

    public int getFull() {
        return full;
    }

    public int getTail() {
        return tail;
    }

    public boolean isTail() {
        return isTail;
    }

    /**
     * Create a new instance with the full part incremented by one and the tail
     * part and the tail flag left unchanged.
     */
    @Nonnull
    public GCGeneration nextFull() {
        return new GCGeneration(full + 1, 0, false);
    }

    /**
     * Create a new instance with the tail part incremented by one and the full
     * part and the tail flag left unchanged.
     */
    @Nonnull
    public GCGeneration nextTail() {
        return new GCGeneration(full, tail + 1, true);
    }

    /**
     * Create a new instance with the tail flag unset and the full part and tail
     * part left unchanged.
     */
    @Nonnull
    public GCGeneration nonTail() {
        return new GCGeneration(full, tail, false);
    }

    /**
     * The the difference of the full part between {@code this} and {@code
     * that}.
     */
    public int compareFull(@Nonnull GCGeneration that) {
        return full - that.full;
    }

    /**
     * The the difference of the tail part between {@code this} and {@code
     * that}.
     */
    public int compareTail(@Nonnull GCGeneration that) {
        return tail - that.tail;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        GCGeneration that = (GCGeneration) other;
        return full == that.full && tail == that.tail && isTail == that.isTail;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(full, tail, isTail);
    }

    @Override
    public String toString() {
        return "GCGeneration{" +
                "full=" + full + ',' +
                "tail=" + tail +  ',' +
                "isTail=" + isTail + '}';
    }

}
