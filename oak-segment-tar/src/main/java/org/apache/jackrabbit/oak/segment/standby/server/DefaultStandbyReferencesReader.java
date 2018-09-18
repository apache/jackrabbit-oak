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

package org.apache.jackrabbit.oak.segment.standby.server;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.file.FileStore;

class DefaultStandbyReferencesReader implements StandbyReferencesReader {

    private final FileStore store;

    DefaultStandbyReferencesReader(FileStore store) {
        this.store = store;
    }

    @Override
    public Iterable<String> readReferences(String id) {
        UUID uuid = UUID.fromString(id);

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        SegmentId segmentId = store.getSegmentIdProvider().newSegmentId(msb, lsb);

        if (store.containsSegment(segmentId)) {
            Segment segment = store.readSegment(segmentId);

            List<String> references = newArrayList();

            for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
                references.add(segment.getReferencedSegmentId(i).toString());
            }

            return references;
        }

        return null;
    }

}
