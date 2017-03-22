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

import javax.annotation.Nonnull;

/**
 * The backend storage interface used by the segment node store.
 */
public interface SegmentStore {

    /**
     * A store that is always empty and that cannot be written to.
     */
    SegmentStore EMPTY_STORE = new SegmentStore() {

        /**
         * @return {@code false}
         */
        @Override
        public boolean containsSegment(SegmentId id) {
            return false;
        }

        /**
         * @throws SegmentNotFoundException always
         */
        @Nonnull
        @Override
        public Segment readSegment(SegmentId segmentId) {
            throw new SegmentNotFoundException(segmentId);
        }

        /**
         * @throws IOException always
         */
        @Override
        public void writeSegment(SegmentId id, byte[] bytes, int offset, int length)
        throws IOException {
            throw new IOException("This store is read only");
        }
    };

    /**
     * Checks whether the identified segment exists in this store.
     *
     * @param id segment identifier
     * @return {@code true} if the segment exists, {@code false} otherwise
     */
    boolean containsSegment(SegmentId id);

    /**
     * Reads the identified segment from this store.
     *
     * @param segmentId segment identifier
     * @return identified segment, or a {@link SegmentNotFoundException} thrown if not found
     */
    @Nonnull
    Segment readSegment(SegmentId segmentId);

    /**
     * Writes the given segment to the segment store.
     *
     * @param id segment identifier
     * @param bytes byte buffer that contains the raw contents of the segment
     * @param offset start offset within the byte buffer
     * @param length length of the segment
     */
    void writeSegment(SegmentId id, byte[] bytes, int offset, int length) throws IOException;
}
