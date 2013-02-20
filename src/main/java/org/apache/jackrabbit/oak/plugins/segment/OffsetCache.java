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

import java.util.Arrays;

abstract class OffsetCache<T> {

    private static final int SIZE_INCREMENT = 1024;

    private static final int[] NO_OFFSETS = new int[0];

    private static final Object[] NO_VALUES = new Object[0];

    private int length = 0;

    private int[] offsets = NO_OFFSETS;

    private Object[] values = NO_VALUES;

    @SuppressWarnings("unchecked")
    public synchronized T get(int offset) {
        int i = Arrays.binarySearch(offsets, 0, length, offset);
        if (i < 0) {
            i = ~i;
            if (length < offsets.length) {
                System.arraycopy(offsets, i, offsets, i + 1, length - i);
                System.arraycopy(values, i, values, i + 1, length - i);
            } else {
                int[] newOffsets = new int[length + SIZE_INCREMENT];
                System.arraycopy(offsets, 0, newOffsets, 0, i);
                System.arraycopy(offsets, i, newOffsets, i + 1, length - i);
                offsets = newOffsets;
                Object[] newValues = new Object[length + SIZE_INCREMENT];
                System.arraycopy(values, 0, newValues, 0, i);
                System.arraycopy(values, i, newValues, i + 1, length - i);
                values = newValues;
            }
            offsets[i] = offset;
            values[i] = load(offset);
        }
        return (T) values[i];
    }

    protected abstract T load(int offset);

}
