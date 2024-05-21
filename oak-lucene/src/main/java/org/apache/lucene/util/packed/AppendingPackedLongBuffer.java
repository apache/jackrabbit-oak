/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.util.packed;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Utility class to buffer a list of signed longs in memory. This class only supports appending and
 * is optimized for non-negative numbers with a uniform distribution over a fixed (limited) range
 *
 * @lucene.internal
 */
public final class AppendingPackedLongBuffer extends AbstractAppendingLongBuffer {

    /**
     * {@link AppendingPackedLongBuffer}
     *
     * @param initialPageCount        the initial number of pages
     * @param pageSize                the size of a single page
     * @param acceptableOverheadRatio an acceptable overhead ratio per value
     */
    public AppendingPackedLongBuffer(int initialPageCount, int pageSize,
        float acceptableOverheadRatio) {
        super(initialPageCount, pageSize, acceptableOverheadRatio);
    }

    /**
     * Create an {@link AppendingPackedLongBuffer} with initialPageCount=16, pageSize=1024 and
     * acceptableOverheadRatio={@link PackedInts#DEFAULT}
     */
    public AppendingPackedLongBuffer() {
        this(16, 1024, PackedInts.DEFAULT);
    }

    /**
     * Create an {@link AppendingPackedLongBuffer} with initialPageCount=16, pageSize=1024
     */
    public AppendingPackedLongBuffer(float acceptableOverheadRatio) {
        this(16, 1024, acceptableOverheadRatio);
    }

    @Override
    long get(int block, int element) {
        if (block == valuesOff) {
            return pending[element];
        } else {
            return values[block].get(element);
        }
    }

    @Override
    int get(int block, int element, long[] arr, int off, int len) {
        if (block == valuesOff) {
            int sysCopyToRead = Math.min(len, pendingOff - element);
            System.arraycopy(pending, element, arr, off, sysCopyToRead);
            return sysCopyToRead;
        } else {
            /* packed block */
            return values[block].get(element, arr, off, len);
        }
    }

    @Override
    void packPendingValues() {
        // compute max delta
        long minValue = pending[0];
        long maxValue = pending[0];
        for (int i = 1; i < pendingOff; ++i) {
            minValue = Math.min(minValue, pending[i]);
            maxValue = Math.max(maxValue, pending[i]);
        }

        // build a new packed reader
        final int bitsRequired = minValue < 0 ? 64 : PackedInts.bitsRequired(maxValue);
        final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired,
            acceptableOverheadRatio);
        for (int i = 0; i < pendingOff; ) {
            i += mutable.set(i, pending, i, pendingOff - i);
        }
        values[valuesOff] = mutable;

    }

}
