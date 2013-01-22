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
package org.apache.jackrabbit.oak.plugins.index.old;

import java.lang.reflect.Array;

import javax.annotation.Nonnull;

/**
 * Array utility methods.
 */
public class ArrayUtils {

    public static final String[] EMPTY_STRING_ARRAY = new String[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final int[] EMPTY_INTEGER_ARRAY = new int[0];

    private ArrayUtils() {
        // utility class
    }

    /**
     * Replace an element in a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @param x the value to add
     * @return the new array
     */
    @Nonnull
    public static <T> T[] arrayReplace(T[] values, int index, T x) {
        int size = values.length;
        @SuppressWarnings("unchecked")
        T[] v2 = (T[]) Array.newInstance(values.getClass().getComponentType(), size);
        System.arraycopy(values, 0, v2, 0, size);
        v2[index] = x;
        return v2;
    }

    /**
     * Insert an element into a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @param x the value to add
     * @return the new array
     */
    public static int[] arrayInsert(int[] values, int index, int x) {
        int size = values.length;
        int[] v2 = new int[size + 1];
        v2[index] = x;
        copyArrayAdd(values, v2, size, index);
        return v2;
    }

    /**
     * Insert an element into a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @param x the value to add
     * @return the new array
     */
    public static long[] arrayInsert(long[] values, int index, long x) {
        int size = values.length;
        long[] v2 = new long[size + 1];
        v2[index] = x;
        copyArrayAdd(values, v2, size, index);
        return v2;
    }

    /**
     * Insert an element into a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @param x the value to add
     * @return the new array
     */
    @Nonnull
    public static <T> T[] arrayInsert(T[] values, int index, T x) {
        int size = values.length;
        @SuppressWarnings("unchecked")
        T[] v2 = (T[]) Array.newInstance(values.getClass().getComponentType(), size + 1);
        v2[index] = x;
        copyArrayAdd(values, v2, size, index);
        return v2;
    }

    /**
     * Insert an element into a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @param x the value to add
     * @return the new array
     */
    @Nonnull
    public static String[] arrayInsert(String[] values, int index, String x) {
        int size = values.length;
        String[] v2 = new String[size + 1];
        v2[index] = x;
        copyArrayAdd(values, v2, size, index);
        return v2;
    }

    /**
     * Remove an element from a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @return the new array
     */
    public static int[] arrayRemove(int[] values, int index) {
        int size = values.length;
        if (size == 1) {
            return EMPTY_INTEGER_ARRAY;
        }
        int[] v2 = new int[size - 1];
        copyArrayRemove(values, v2, size, index);
        return v2;
    }

    /**
     * Remove an element from a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @return the new array
     */
    @Nonnull
    public static <T> T[] arrayRemove(T[] values, int index) {
        int size = values.length;
        @SuppressWarnings("unchecked")
        T[] v2 = (T[]) Array.newInstance(values.getClass().getComponentType(), size - 1);
        copyArrayRemove(values, v2, size, index);
        return v2;
    }

    /**
     * Remove an element from a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @return the new array
     */
    public static long[] arrayRemove(long[] values, int index) {
        int size = values.length;
        if (size == 1) {
            return EMPTY_LONG_ARRAY;
        }
        long[] v2 = new long[size - 1];
        copyArrayRemove(values, v2, size, index);
        return v2;
    }


    /**
     * Remove an element from a clone of the array at the given position.
     *
     * @param values the values
     * @param index the index
     * @return the new array
     */
    @Nonnull
    public static String[] arrayRemove(String[] values, int index) {
        int size = values.length;
        if (size == 1) {
            return EMPTY_STRING_ARRAY;
        }
        String[] v2 = new String[size - 1];
        copyArrayRemove(values, v2, size, index);
        return v2;
    }

    private static void copyArrayAdd(Object src, Object dst, int size, int index) {
        if (index > 0) {
            System.arraycopy(src, 0, dst, 0, index);
        }
        if (index < size) {
            System.arraycopy(src, index, dst, index + 1, size - index);
        }
    }

    private static void copyArrayRemove(Object src, Object dst, int size, int index) {
        if (index > 0 && size > 0) {
            System.arraycopy(src, 0, dst, 0, index);
        }
        if (index < size) {
            System.arraycopy(src, index + 1, dst, index, size - index - 1);
        }
    }

}
