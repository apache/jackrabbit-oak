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

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * The backend storage interface used by the segment node store.
 */
@Deprecated
public interface SegmentStore extends Closeable {

    @Deprecated
    SegmentTracker getTracker();

    /**
     * Returns the head state.
     *
     * @return head state
     */
    @Nonnull
    @Deprecated
    SegmentNodeState getHead();

    @Deprecated
    boolean setHead(SegmentNodeState base, SegmentNodeState head);

    /**
     * Checks whether the identified segment exists in this store.
     *
     * @param id segment identifier
     * @return {@code true} if the segment exists, {@code false} otherwise
     */
    @Deprecated
    boolean containsSegment(SegmentId id);

    /**
     * Reads the identified segment from this store.
     *
     * @param segmentId segment identifier
     * @return identified segment, or a {@link SegmentNotFoundException} thrown if not found
     */
    @CheckForNull
    @Deprecated
    Segment readSegment(SegmentId segmentId);

    /**
     * Writes the given segment to the segment store.
     *
     * @param id segment identifier
     * @param bytes byte buffer that contains the raw contents of the segment
     * @param offset start offset within the byte buffer
     * @param length length of the segment
     */
    @Deprecated
    void writeSegment(SegmentId id, byte[] bytes, int offset, int length) throws IOException;

    @Deprecated
    void close();

    /**
     * Read a blob from external storage.
     *
     * @param reference blob reference
     * @return external blob
     */
    @Deprecated
    Blob readBlob(String reference);

    /**
     * Returns the external BlobStore (if configured) with this store
     */
    @CheckForNull
    @Deprecated
    BlobStore getBlobStore();

    /**
     * Triggers removal of segments that are no longer referenceable.
     */
    @Deprecated
    void gc();

}
