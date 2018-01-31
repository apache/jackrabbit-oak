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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

/**
 * Helper class exposing static factories for reclaimers. A reclaimer
 * is a predicate used during the cleanup phase of garbage collection
 * to decide whether a segment of a given generation is reclaimable.
 */
class Reclaimers {

    private Reclaimers() {
        // Prevent instantiation.
    }

    /**
     * Create a reclaimer for segments of old generations. Whether a segment is considered old and
     * thus reclaimable depends on the type of the most recent GC operation and the number of
     * retained generations.
     * <p>
     * In the case of {@link GCType#FULL FULL} a segment is reclaimable if its
     * {@link GCGeneration#getFullGeneration() full generation} is at least {@code retainedGenerations}
     * in the past wrt. {@code referenceGeneration} <em>or</em> if its
     * {@link GCGeneration#getGeneration() generation} is at least {@code retainedGenerations} in the
     * past wrt. {@code referenceGeneration} and it not a {@link GCGeneration#isCompacted() compacted segment}.
     * <p>
     * In the case of {@link GCType#TAIL TAIL} a segment is reclaimable if its
     * {@link GCGeneration#getGeneration() generation} is at least {@code retainedGenerations} in the
     * past wrt. {@code referenceGeneration} <em>and</em> the segment is not in the same tail as
     * segments of the {@code referenceGeneration}. A segment is in the same tail as another segment
     * if it is a {@link GCGeneration#isCompacted() compacted segment} <em>and</em> both segments have
     * the same {@link GCGeneration#fullGeneration full generation}.
     *
     * @param lastGCType  type of the most recent GC operation. {@link GCType#FULL} if unknown.
     * @param referenceGeneration  generation used as reference for determining the age of other segments.
     * @param retainedGenerations  number of generations to retain.
     */
    static Predicate<GCGeneration> newOldReclaimer(
            @Nonnull GCType lastGCType,
            @Nonnull final GCGeneration referenceGeneration,
            int retainedGenerations) {

        switch (checkNotNull(lastGCType)) {
            case FULL:
                return newOldFullReclaimer(referenceGeneration, retainedGenerations);
            case TAIL:
                return newOldTailReclaimer(referenceGeneration, retainedGenerations);
            default:
                throw new IllegalArgumentException("Invalid gc type: " + lastGCType);
        }
    }

    private static Predicate<GCGeneration> newOldFullReclaimer(
            @Nonnull final GCGeneration referenceGeneration,
            int retainedGenerations) {
        return new Predicate<GCGeneration>() {

            @Override
            public boolean apply(GCGeneration generation) {
                return isOldFull(generation) || (isOld(generation) && !generation.isCompacted());
            }

            private boolean isOld(GCGeneration generation) {
                return referenceGeneration.compareWith(generation) >= retainedGenerations;
            }

            private boolean isOldFull(GCGeneration generation) {
                return referenceGeneration.compareFullGenerationWith(generation) >= retainedGenerations;
            }

            @Override
            public String toString() {
                return String.format(
                        "(full generation older than %d.%d, with %d retained generations)",
                        referenceGeneration.getGeneration(),
                        referenceGeneration.getFullGeneration(),
                        retainedGenerations
                );
            }
        };
    }

    private static Predicate<GCGeneration> newOldTailReclaimer(
            @Nonnull final GCGeneration referenceGeneration,
            int retainedGenerations) {
        return new Predicate<GCGeneration>() {

            @Override
            public boolean apply(GCGeneration generation) {
                return isOld(generation) && !sameCompactedTail(generation);
            }

            private boolean isOld(GCGeneration generation) {
                return referenceGeneration.compareWith(generation) >= retainedGenerations;
            }

            private boolean sameCompactedTail(GCGeneration generation) {
                return generation.isCompacted()
                        && generation.getFullGeneration() == referenceGeneration.getFullGeneration();
            }

            @Override
            public String toString() {
                return String.format(
                        "(generation older than %d.%d, with %d retained generations and not in the same compacted tail)",
                        referenceGeneration.getGeneration(),
                        referenceGeneration.getFullGeneration(),
                        retainedGenerations
                );
            }

        };
    }

    /**
     * Create an exact reclaimer. An exact reclaimer reclaims only segment of on single generation.
     * @param referenceGeneration  the generation to collect.
     * @return  an new instance of an exact reclaimer for segments with their generation
     *          matching {@code referenceGeneration}.
     */
    static Predicate<GCGeneration> newExactReclaimer(@Nonnull final GCGeneration referenceGeneration) {
        return new Predicate<GCGeneration>() {
            @Override
            public boolean apply(GCGeneration generation) {
                return generation.equals(referenceGeneration);
            }
            @Override
            public String toString() {
                return "(generation==" + referenceGeneration + ")";
            }
        };
    }

}
