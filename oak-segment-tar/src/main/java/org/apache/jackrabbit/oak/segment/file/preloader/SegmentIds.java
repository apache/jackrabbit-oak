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
package org.apache.jackrabbit.oak.segment.file.preloader;

/**
 * A simple, memory-efficient data structure to hold segment IDs (UUIDs) as pairs of long values
 * (most significant bits and least significant bits).
 */
final class SegmentIds {

    public static final SegmentIds EMPTY = new SegmentIds(0);

    private final long[] segmentIds;

    SegmentIds(int size) {
        this.segmentIds = new long[size * 2];
    }

    public void add(int index, long msb, long lsb) {
        checkIndexWithinBounds(index);
        segmentIds[index * 2] = msb;
        segmentIds[index * 2 + 1] = lsb;
    }

    public long getMsb(int index) {
        checkIndexWithinBounds(index);
        return segmentIds[index * 2];
    }

    public long getLsb(int index) {
        checkIndexWithinBounds(index);
        return segmentIds[index * 2 + 1];
    }

    public int size() {
        return segmentIds.length / 2;
    }

    public boolean isEmpty() {
        return segmentIds.length == 0;
    }

    private void checkIndexWithinBounds(int index) {
        if (index < 0 || segmentIds.length / 2 <= index) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + (segmentIds.length / 2));
        }
    }
}
