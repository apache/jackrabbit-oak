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
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.jetbrains.annotations.NotNull;

/**
 * Loads segments that were not previously in the cache from the tar files.
 */
@FunctionalInterface
public interface SegmentLoader {
    /**
     * Loads a segment
     *
     * @param tarFiles   The tar files
     * @param id         The ID of the segment
     * @param idProvider Provides {@link SegmentId} instances
     * @param writer     The object used to write segments to the tar files (can be used to retrieve a segment that is
     *                   still being written to, possibly by flushing it).
     * @return The segment
     * @throws SegmentNotFoundException if the segment could not be found
     */
    @NotNull
    Segment loadSegment(
            @NotNull TarFiles tarFiles,
            @NotNull SegmentId id,
            @NotNull SegmentIdProvider idProvider,
            @NotNull SegmentWriter writer
    ) throws SegmentNotFoundException;
}
