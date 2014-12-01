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
package org.apache.jackrabbit.oak.util;

import java.util.Random;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * An approximate counter algorithm.
 */
public class ApproximateCounter {
    
    public static final String COUNT_PROPERTY_NAME = ":count";
    public static final int COUNT_RESOLUTION = 100;

    private static final Random RANDOM = new Random();
    
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
     * Adjust a counter in the given node.
     * 
     * @param builder the node builder
     * @param offset the offset
     */
    public static void adjustCount(NodeBuilder builder, long offset) {
        offset = ApproximateCounter.calculateOffset(
                offset, COUNT_RESOLUTION);
        if (offset == 0) {
            return;
        }
        PropertyState p = builder.getProperty(COUNT_PROPERTY_NAME);
        long count = p == null ? 0 : p.getValue(Type.LONG);
        offset = ApproximateCounter.adjustOffset(count,
                offset, COUNT_RESOLUTION);
        if (offset == 0) {
            return;
        }
        count += offset;
        builder.setProperty(COUNT_PROPERTY_NAME, count);
    }

}
