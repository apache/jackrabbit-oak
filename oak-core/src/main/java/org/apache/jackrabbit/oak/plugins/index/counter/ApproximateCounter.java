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
package org.apache.jackrabbit.oak.plugins.index.counter;

import java.util.Random;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An approximate counter algorithm.
 */
public class ApproximateCounter {
    
    public static final String COUNT_PROPERTY_PREFIX = ":count_";
    public static final int COUNT_RESOLUTION = 100;
    public static final int COUNT_MAX = 10000000;

    private static final Random RANDOM = new Random();

    private ApproximateCounter() {
    }

    /**
     * Calculate the approximate offset from a given offset. The offset is the
     * number of added or removed entries. The result is 0 in most of the cases,
     * but sometimes it is a (positive or negative) multiple of the resolution,
     * such that on average, the sum of the returned value matches the sum of
     * the passed offsets.
     * 
     * @param offset the high-resolution input offset
     * @param resolution the resolution
     * @return the low-resolution offset (most of the time 0)
     */
    public static long calculateOffset(long offset, int resolution) {
        if (offset == 0 || resolution <= 1) {
            return offset;
        }
        int add = resolution;
        if (offset < 0) {
            offset = -offset;
            add = -add;
        }
        long result = 0;
        for (long i = 0; i < offset; i++) {
            if (RANDOM.nextInt(resolution) == 0) {
                result += add;
            }
        }
        return result;
    }

    /**
     * This method ensures that the new approximate count (the old count plus
     * the calculated offset) does not go below 0.
     * 
     * Also, for large counts and resolutions larger than 10, it reduces the
     * resolution by a factor of 10 (further reducing the number of updates
     * needed by a factor of 10).
     * 
     * @param oldCount the old count
     * @param calculatedOffset the calculated offset (may not be 0)
     * @param resolution the new (lower) resolution
     * @return the new offset
     */
    public static long adjustOffset(long oldCount, long calculatedOffset,
            int resolution) {
        if (oldCount + calculatedOffset < 0) {
            return -oldCount;
        }
        if (resolution <= 10 || oldCount < resolution * 10) {
            return calculatedOffset;
        }
        return RANDOM.nextInt(10) == 0 ? calculatedOffset * 10 : 0;
    }
    
    /**
     * Set the seed of the random number generator (used for testing).
     * 
     * @param seed the new seed
     */
    static void setSeed(int seed) {
        RANDOM.setSeed(seed);
    }
    
    /**
     * Adjust a counter in the given node. This method supports concurrent
     * changes. It uses multiple properties, and is less accurate, but can be
     * used in a multi-threaded environment, as it uses unique property names.
     * 
     * @param builder the node builder
     * @param offset the offset
     */
    public static void adjustCountSync(NodeBuilder builder, long offset) {
        if (offset == 0) {
            return;
        }
        boolean added = offset > 0;
        for (long i = 0; i < Math.abs(offset); i++) {
            adjustCountSync(builder, added);
        }
    }
    
    private static void adjustCountSync(NodeBuilder builder, boolean added) {
        if (RANDOM.nextInt(COUNT_RESOLUTION) != 0) {
            return;
        }
        int max = getMaxCount(builder, added);
        if (max >= COUNT_MAX) {
            return;
        }
        // TODO is this the right approach? divide by count_resolution
        int x = Math.max(COUNT_RESOLUTION, max * 2) / COUNT_RESOLUTION;
        if (RANDOM.nextInt(x) > 0) {
            return;
        }
        long value = x * COUNT_RESOLUTION;
        String propertyName = COUNT_PROPERTY_PREFIX + UUID.randomUUID();
        builder.setProperty(propertyName, added ? value : -value);
    }
    
    private static int getMaxCount(NodeBuilder node, boolean added) {
        long max = 0;
        for (PropertyState p : node.getProperties()) {
            if (!p.getName().startsWith(COUNT_PROPERTY_PREFIX)) {
                continue;
            }
            long x = p.getValue(Type.LONG);
            if (added == x > 0) {
                max = Math.max(max, Math.abs(x));
            }
        }
        max = Math.min(Integer.MAX_VALUE, max);
        return (int) max;
    }
    
    /**
     * Get the count estimation.
     *
     * @param node the node
     * @return the estimation (-1 if no estimation is available)
     */
    public static long getCountSync(NodeState node) {
        boolean hasCountProperty = false;
        long added = 0;
        long removed = 0;
        for (PropertyState p : node.getProperties()) {
            if (!p.getName().startsWith(COUNT_PROPERTY_PREFIX)) {
                continue;
            }
            hasCountProperty = true;
            long x = p.getValue(Type.LONG);
            if (x > 0) {
                added += x;
            } else {
                removed -= x;
            }
        }
        if (!hasCountProperty) {
            return -1;
        }
        return Math.max(added / 2, added - removed);
    }

}
