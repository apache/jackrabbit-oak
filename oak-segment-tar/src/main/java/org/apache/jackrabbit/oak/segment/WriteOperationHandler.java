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

package org.apache.jackrabbit.oak.segment;

import java.io.IOException;

import org.apache.jackrabbit.oak.segment.data.PartialSegmentState;
import org.jetbrains.annotations.NotNull;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.jetbrains.annotations.Nullable;

/**
 * A {@code WriteOperationHandler} executes {@link WriteOperation
 * WriteOperation}s and as such serves as a bridge between a {@link
 * SegmentWriter} and {@link SegmentBufferWriter}.
 */
interface WriteOperationHandler {

    /**
     * @return the current {@code GCGeneration} of the store.
     */
    @NotNull
    GCGeneration getGCGeneration();

    /**
     * A {@code WriteOperation} encapsulates an operation on a {@link
     * SegmentWriter}. Executing it performs the actual act of persisting
     * changes to a {@link SegmentBufferWriter}.
     */
    interface WriteOperation {

        /**
         * Persist any changes represented by the {@code WriteOperation} to the
         * passed {@code writer}.
         * @param writer  writer which must be used to persist any changes
         * @return        {@code RecordId} that resulted from persisting the changes.
         * @throws IOException
         */
        @NotNull
        RecordId execute(@NotNull SegmentBufferWriter writer) throws IOException;
    }

    /**
     * Execute the passed {@code writeOperation} by passing it a {@link SegmentBufferWriter}.
     * @param gcGeneration    the {@code GCGeneration} the changes should persisted with.
     * @param writeOperation  {@link WriteOperation} to execute
     * @return                {@code RecordId} that resulted from persisting the changes.
     * @throws IOException
     */
    @NotNull
    RecordId execute(@NotNull GCGeneration gcGeneration, @NotNull WriteOperation writeOperation)
    throws IOException;

    /**
     * Flush any pending changes on any {@link SegmentBufferWriter} managed by this instance.
     * @param store  the {@code SegmentStore} instance to write the {@code Segment} to
     * @throws IOException
     */
    void flush(@NotNull SegmentStore store) throws IOException;

    /** @see SegmentWriter#readPartialSegmentState(SegmentId) */
    @Nullable
    default PartialSegmentState readPartialSegmentState(@NotNull SegmentId sid) {
        throw new UnsupportedOperationException("Trying to read partial segment state from a WriteOperationHandler that doesn’t support it.");
    }
}
