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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

class IndexV2 implements Index {

    static final int FOOTER_SIZE = 16;

    private final ByteBuffer entries;

    IndexV2(ByteBuffer entries) {
        this.entries = entries;
    }

    @Override
    public Set<UUID> getUUIDs() {
        Set<UUID> uuids = newHashSetWithExpectedSize(entries.remaining() / IndexEntryV2.SIZE);
        int position = entries.position();
        while (position < entries.limit()) {
            long msb = entries.getLong(position);
            long lsb = entries.getLong(position + 8);
            uuids.add(new UUID(msb, lsb));
            position += IndexEntryV2.SIZE;
        }
        return uuids;
    }

    @Override
    public int findEntry(long msb, long lsb) {
        // The segment identifiers are randomly generated with uniform
        // distribution, so we can use interpolation search to find the
        // matching entry in the index. The average runtime is O(log log n).

        int lowIndex = 0;
        int highIndex = entries.remaining() / IndexEntryV2.SIZE - 1;
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex + Math.round(
                    (highIndex - lowIndex)
                            * (targetValue - lowValue)
                            / (highValue - lowValue));
            int position = entries.position() + guessIndex * IndexEntryV2.SIZE;
            long m = entries.getLong(position);
            if (msb < m) {
                highIndex = guessIndex - 1;
                highValue = m;
            } else if (msb > m) {
                lowIndex = guessIndex + 1;
                lowValue = m;
            } else {
                // getting close...
                long l = entries.getLong(position + 8);
                if (lsb < l) {
                    highIndex = guessIndex - 1;
                    highValue = m;
                } else if (lsb > l) {
                    lowIndex = guessIndex + 1;
                    lowValue = m;
                } else {
                    return position / IndexEntryV2.SIZE;
                }
            }
        }

        return -1;
    }

    @Override
    public int size() {
        return entries.remaining() + FOOTER_SIZE;
    }

    @Override
    public int count() {
        return entries.remaining() / IndexEntryV2.SIZE;
    }

    @Override
    public IndexEntryV2 entry(int i) {
        return new IndexEntryV2(entries, checkElementIndex(i, count()) * IndexEntryV2.SIZE);
    }

}
