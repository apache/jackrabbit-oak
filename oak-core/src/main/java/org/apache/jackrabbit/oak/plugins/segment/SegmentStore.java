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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;

public interface SegmentStore {

    SegmentWriter getWriter();

    /**
     * Returns the head state.
     *
     * @return head state
     */
    @Nonnull
    SegmentNodeState getHead();

    boolean setHead(SegmentNodeState base, SegmentNodeState head);

    /**
     * Reads the identified segment from this store.
     *
     * @param segmentId segment identifier
     * @return identified segment, or {@code null} if not found
     */
    @CheckForNull
    Segment readSegment(UUID segmentId);

    /**
     * Writes the given segment to the segment store.
     *
     * @param segmentId segment identifier
     * @param bytes byte buffer that contains the raw contents of the segment
     * @param offset start offset within the byte buffer
     * @param length length of the segment
     */
    void writeSegment(UUID segmentId, byte[] bytes, int offset, int length);

    void close();

    /**
     * Checks whether the given object is a record of the given type and
     * is stored in this segment store.
     *
     * @param object possible record object
     * @param type record type
     * @return {@code true} if the object is a record of the given type
     *         from this store, {@code false} otherwise
     */
    boolean isInstance(Object object, Class<? extends Record> type);

    /**
     * Read a blob from external storage.
     *
     * @param reference blob reference
     * @return external blob
     */
    Blob readBlob(String reference);

}
