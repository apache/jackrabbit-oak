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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.jetbrains.annotations.NotNull;

/**
 * Instances of this class represent the result from a compaction. Either
 * succeeded, aborted or skipped.
 */
abstract class CompactionResult {

    @NotNull
    private final GCGeneration currentGeneration;

    private final int gcCount;

    private CompactionResult(@NotNull GCGeneration currentGeneration, int gcCount) {
        this.currentGeneration = currentGeneration;
        this.gcCount = gcCount;
    }

    /**
     * Result of a succeeded compaction.
     *
     * @param gcType          the type of the succeeded compaction operation
     * @param newGeneration   the generation successfully created by compaction
     * @param gcOptions       the current GC options used by compaction
     * @param compactedRootId the record id of the root created by compaction
     */
    static CompactionResult succeeded(
        @NotNull SegmentGCOptions.GCType gcType,
        @NotNull GCGeneration newGeneration,
        @NotNull final SegmentGCOptions gcOptions,
        @NotNull final RecordId compactedRootId,
        int gcCount
    ) {
        return new CompactionResult(newGeneration, gcCount) {

            @Override
            Predicate<GCGeneration> reclaimer() {
                return newOldReclaimer(gcType, newGeneration, gcOptions.getRetainedGenerations());
            }

            @Override
            boolean isSuccess() {
                return true;
            }

            @Override
            RecordId getCompactedRootId() {
                return compactedRootId;
            }
        };
    }

    /**
     * Result of an aborted compaction.
     *
     * @param currentGeneration the current generation of the store
     * @param failedGeneration  the generation that compaction attempted to
     *                          create
     */
    static CompactionResult aborted(
        @NotNull GCGeneration currentGeneration,
        @NotNull final GCGeneration failedGeneration,
        int gcCount
    ) {
        return new CompactionResult(currentGeneration, gcCount) {

            @Override
            Predicate<GCGeneration> reclaimer() {
                return Reclaimers.newExactReclaimer(failedGeneration);
            }

            @Override
            boolean isSuccess() {
                return false;
            }
        };
    }

    /**
     * Result serving as a placeholder for a compaction that was skipped.
     *
     * @param lastGCType        type of the most recent gc operation. {@link
     *                          SegmentGCOptions.GCType#FULL} if none.
     * @param currentGeneration the current generation of the store
     * @param gcOptions         the current GC options used by compaction
     */
    static CompactionResult skipped(
        @NotNull SegmentGCOptions.GCType lastGCType,
        @NotNull GCGeneration currentGeneration,
        @NotNull final SegmentGCOptions gcOptions,
        @NotNull final RecordId compactedRootId,
        int gcCount
    ) {
        return new CompactionResult(currentGeneration, gcCount) {

            @Override
            Predicate<GCGeneration> reclaimer() {
                return Reclaimers.newOldReclaimer(lastGCType, currentGeneration, gcOptions.getRetainedGenerations());
            }

            @Override
            boolean isSuccess() {
                return true;
            }

            @Override
            RecordId getCompactedRootId() {
                return compactedRootId;
            }
        };
    }

    static CompactionResult notApplicable(int count) {
        return new CompactionResult(GCGeneration.NULL, count) {

            @Override
            Predicate<GCGeneration> reclaimer() {
                return generation -> false;
            }

            @Override
            boolean isSuccess() {
                return false;
            }

            @Override
            boolean isNotApplicable() {
                return true;
            }

        };
    }

    /**
     * @return a predicate determining which segments to {clean up} for the
     * given compaction result.
     */
    abstract Predicate<GCGeneration> reclaimer();

    /**
     * @return {@code true} for succeeded and skipped, {@code false} otherwise.
     */
    abstract boolean isSuccess();

    /**
     * @return the record id of the compacted root on {@link #isSuccess()
     * success}, {@link RecordId#NULL} otherwise.
     */
    RecordId getCompactedRootId() {
        return RecordId.NULL;
    }

    boolean isNotApplicable() {
        return false;
    }

    /**
     * @return a diagnostic message describing the outcome of this compaction.
     */
    String gcInfo() {
        return String.format(
            "gc-count=%d,gc-status=%s,store-generation=%s,reclaim-predicate=%s",
            gcCount,
            isSuccess() ? "success" : "failed",
            currentGeneration,
            reclaimer()
        );
    }

}
