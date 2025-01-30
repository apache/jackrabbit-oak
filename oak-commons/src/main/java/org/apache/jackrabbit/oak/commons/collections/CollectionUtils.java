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
package org.apache.jackrabbit.oak.commons.collections;

/**
 * Utility methods for collections conversions.
 */
class CollectionUtils {

    // Maximum capacity for a hash based collection. (used internally by JDK).
    // Also, it helps to avoid overflow errors when calculating the capacity
    private static final int MAX_CAPACITY = 1 << 30;

    private CollectionUtils() {
        // no instances for you
    }

    /**
     * Ensure the capacity of a map or set given the expected number of elements.
     *
     * @param capacity the expected number of elements
     * @return the capacity to use to avoid rehashing & collisions
     */
    static int ensureCapacity(final int capacity) {

        if (capacity < 0) {
            throw new IllegalArgumentException("Capacity must be non-negative");
        }

        if (capacity > MAX_CAPACITY) {
            return MAX_CAPACITY;
        }

        return 1 + (int) (capacity / 0.75f);
    }
}