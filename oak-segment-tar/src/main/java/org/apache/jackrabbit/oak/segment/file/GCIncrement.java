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

import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.SegmentWriterFactory;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class to keep track of generations for incremental compaction.
 */
public class GCIncrement {
    private final @NotNull GCGeneration baseGeneration;
    private final @NotNull GCGeneration partialGeneration;
    private final @NotNull GCGeneration targetGeneration;

    public GCIncrement(@NotNull GCGeneration base, @NotNull GCGeneration partial, @NotNull GCGeneration target) {
        baseGeneration = base;
        partialGeneration = partial;
        targetGeneration = target;
    }

    @NotNull SegmentWriter createPartialWriter(@NotNull SegmentWriterFactory factory) {
        return factory.newSegmentWriter(partialGeneration);
    }

    @NotNull SegmentWriter createTargetWriter(@NotNull SegmentWriterFactory factory) {
        return factory.newSegmentWriter(targetGeneration);
    }

    /**
     * Compaction may be used to copy a repository to the same generation as before.
     * Therefore, we only consider a segment as fully compacted if it is distinct from the base generation,
     * even if it already matches the target generation.
     */
    boolean isFullyCompacted(GCGeneration generation) {
        return (generation.compareWith(baseGeneration) > 0) && generation.equals(targetGeneration);
    }

    @Override
    public String toString() {
        return "GCIncrement{\n" +
                "  base:    " + baseGeneration    + "\n" +
                "  partial: " + partialGeneration + "\n" +
                "  target:  " + targetGeneration  + "\n" +
                "}";
    }
}
