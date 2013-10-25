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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;

public interface SegmentStore {

    SegmentWriter getWriter();

    Journal getJournal(String name);

    Segment readSegment(UUID segmentId);

    /**
     * Writes the given segment to the segment store.
     *
     * @param segmentId segment identifier
     * @param referencedSegmentIds identifiers of all the referenced segments
     * @param bytes byte buffer that contains the raw contents of the segment
     * @param offset start offset within the byte buffer
     * @param length length of the segment
     */
    void writeSegment(
            UUID segmentId, Collection<UUID> referencedSegmentIds,
            byte[] bytes, int offset, int length);

    void deleteSegment(UUID segmentId);

    void close();

    <T> T getRecord(RecordId id, Callable<T> loader);

}
