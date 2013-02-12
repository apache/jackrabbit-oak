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

import com.google.common.base.Preconditions;

public final class RecordId implements Comparable<RecordId> {

    public static RecordId[] EMPTY_ARRAY = new RecordId[0];

    public static RecordId fromString(String id) {
        int colon = id.indexOf(':');
        if (colon != -1) {
            return new RecordId(
                    UUID.fromString(id.substring(0, colon)),
                    Integer.parseInt(id.substring(colon + 1)));
        } else {
            throw new IllegalArgumentException("Bad RecordId: " + id);
        }
    }

    private final UUID segmentId;

    private final int offset;

    public RecordId(UUID segmentId, int offset) {
        this.segmentId = Preconditions.checkNotNull(segmentId);
        this.offset = offset;
    }

    public UUID getSegmentId() {
        return segmentId;
    }

    public int getOffset() {
        return offset;
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(RecordId that) {
        int diff = segmentId.compareTo(that.segmentId);
        if (diff == 0) {
            diff = offset - that.offset;
        }
        return diff;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return segmentId + ":" + offset;
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode() ^ offset;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof RecordId) {
            RecordId that = (RecordId) object;
            return offset == that.offset && segmentId.equals(that.segmentId);
        } else {
            return false;
        }
    }

}
