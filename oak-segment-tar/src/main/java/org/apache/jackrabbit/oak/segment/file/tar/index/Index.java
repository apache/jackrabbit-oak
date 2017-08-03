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

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

public class Index {

    private final ByteBuffer index;

    Index(ByteBuffer index) {
        this.index = index;
    }

    public Set<UUID> getUUIDs() {
        Set<UUID> uuids = newHashSetWithExpectedSize(index.remaining() / IndexLoader.ENTRY_SIZE);
        int position = index.position();
        while (position < index.limit()) {
            long msb = index.getLong(position);
            long lsb = index.getLong(position + 8);
            uuids.add(new UUID(msb, lsb));
            position += IndexLoader.ENTRY_SIZE;
        }
        return uuids;
    }

    public IndexEntry findEntry(long msb, long lsb) {
        // The segment identifiers are randomly generated with uniform
        // distribution, so we can use interpolation search to find the
        // matching entry in the index. The average runtime is O(log log n).

        int lowIndex = 0;
        int highIndex = index.remaining() / IndexLoader.ENTRY_SIZE - 1;
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex + Math.round(
                    (highIndex - lowIndex)
                            * (targetValue - lowValue)
                            / (highValue - lowValue));
            int position = index.position() + guessIndex * IndexLoader.ENTRY_SIZE;
            long m = index.getLong(position);
            if (msb < m) {
                highIndex = guessIndex - 1;
                highValue = m;
            } else if (msb > m) {
                lowIndex = guessIndex + 1;
                lowValue = m;
            } else {
                // getting close...
                long l = index.getLong(position + 8);
                if (lsb < l) {
                    highIndex = guessIndex - 1;
                    highValue = m;
                } else if (lsb > l) {
                    lowIndex = guessIndex + 1;
                    lowValue = m;
                } else {
                    return new IndexEntry(index, position);
                }
            }
        }

        return null;
    }

    public int size() {
        return index.remaining() + 16;
    }

    public int entryCount() {
        return index.remaining() / IndexLoader.ENTRY_SIZE;
    }

    public IndexEntry entry(int i) {
        return new IndexEntry(index, index.position() + i * IndexLoader.ENTRY_SIZE);
    }

}
