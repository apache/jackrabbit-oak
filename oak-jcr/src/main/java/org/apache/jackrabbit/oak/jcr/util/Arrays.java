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

package org.apache.jackrabbit.oak.jcr.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class providing helper functions for arrays.
 */
public final class Arrays {
    private Arrays() {}

    /**
     * Check whether an array contains a given element
     * @param array
     * @param element
     * @param <T>
     * @return {@code true} iff {@code array} contains {@code element}.
     */
    public static <T> boolean contains(T[] array, T element) {
        for (T t : array) {
            if (element == null && t == null || element != null && element.equals(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convert an array to a set.
     * @param elements
     * @param <T>
     * @return
     */
    public static <T> Set<T> toSet(T... elements) {
        return new HashSet<T>(java.util.Arrays.asList(elements));
    }

    /**
     * Create a new array of the same type with an additional element added.
     * @param array
     * @param value
     * @param <T>
     * @return array of {@code array.length + 1} with {@code value} as its last element.
     */
    public static <T> T[] add(T[] array, T value) {
        T[] copy = java.util.Arrays.copyOf(array, array.length + 1);
        copy[array.length] = value;
        return copy;
    }

    /**
     * Create a new array with all occurrences of {@code value} removed.
     * @param array
     * @param value
     * @param <T>
     * @return an array containing all elements of {@code array} except for {@code value}.
     */
    @SuppressWarnings("unchecked")
    public static <T> T[] remove(T[] array, T value) {
        List<T> copy = new ArrayList<T>(array.length);
        for (T v : array) {
            if (value == null) {
                if (v != null) {
                    copy.add(v);
                }
            }
            else if (!value.equals(v)) {
                copy.add(v);
            }
        }

        return (T[]) copy.toArray();
    }
}
