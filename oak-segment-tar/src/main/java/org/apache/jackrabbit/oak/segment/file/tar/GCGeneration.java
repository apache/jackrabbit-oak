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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
/**
 * Instances of this class represent the garbage collection generation related
 * information of a segment. It consists of the segment's generation, its full
 * generation and its compaction flag. The segment's generation records the number
 * of garbage collection cycles a segment went through and is incremented with
 * every garbage collection regardless its type. The segment's full generation
 * records the number of full garbage collection cycles a segment went through.
 * It is only incremented on full garbage collection cycles. The segment's compaction
 * flag is set for those segments that have been created by a compaction operation.
 * It is never set for segments created by normal write operations or defer
 * compactions triggered by such. Segments written by normal repository writes will
 * inherit the generation and full generation of the segment written by the previous
 * compaction process with the compacted flag cleared.
 * <p>
 * The information recorded in this way allows to determine the reclamation status
 * of a segment by just looking at the {@code GCGeneration} instances of that segment
 * and of the segment containing the repository head: Let {@code s} be a segment,
 * {@code h} be the segment containing the current repository head and {@code n} be
 * the number of retained generations.
 * <ul>
 *     <li>{@code s} is old iff {@code h.generation - s.generation >= n}</li>
 *     <li>{@code s} is in the same compaction tail than h iff
 *         {@code s.isCompacted && s.fullGeneration == h.fullGeneration}</li>
 *     <li>{@code s} is reclaimable iff {@code s} is old and {@code s} is not in the same compaction tail than {@code h}</li>
 * </ul>
 */
public final class GCGeneration {

    public static final GCGeneration NULL = new GCGeneration(0, 0, false);

    public static GCGeneration newGCGeneration(int generation, int fullGeneration, boolean isCompacted) {
        return new GCGeneration(generation, fullGeneration, isCompacted);
    }

    private final int generation;

    private final int fullGeneration;

    private final boolean isCompacted;

    private GCGeneration(int generation, int fullGeneration, boolean isCompacted) {
        this.generation = generation;
        this.fullGeneration = fullGeneration;
        this.isCompacted = isCompacted;
    }

    public int getGeneration() {
        return generation;
    }

    public int getFullGeneration() {
        return fullGeneration;
    }

    public boolean isCompacted() {
        return isCompacted;
    }

    /**
     * Create a new instance with the generation and the full generation incremented by one
     * and the compaction flag left unchanged.
     */
    @Nonnull
    public GCGeneration nextFull() {
        return new GCGeneration(generation + 1, fullGeneration + 1, true);
    }

    /**
     * Create a new instance with the generation incremented by one and the full
     * generation and the compaction flag left unchanged.
     */
    @Nonnull
    public GCGeneration nextTail() {
        return new GCGeneration(generation + 1, fullGeneration, true);
    }

    /**
     * Create a new instance with the compaction flag unset and the generation and the
     * full generation left unchanged.
     */
    @Nonnull
    public GCGeneration nonGC() {
        return new GCGeneration(generation, fullGeneration, false);
    }

    public int compareWith(@Nonnull GCGeneration gcGeneration) {
        return generation - checkNotNull(gcGeneration).generation;
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
        return generation == that.generation
                && fullGeneration == that.fullGeneration
                && isCompacted == that.isCompacted;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(generation, fullGeneration, isCompacted);
    }

    @Override
    public String toString() {
        return "GCGeneration{" +
                "generation=" + generation + ',' +
                "fullGeneration=" + fullGeneration +  ',' +
                "isCompacted=" + isCompacted + '}';
    }

}
