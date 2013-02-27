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

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;

abstract class OffsetCache<T> {

    private static final int SIZE_INCREMENT = 1024;

    private static final int[] NO_OFFSETS = new int[0];

    private static final SoftReference<?>[] NO_VALUES = new SoftReference<?>[0];

    private int length = 0;

    private int[] offsets;

    private SoftReference<?>[] values;

    OffsetCache() {
        offsets = NO_OFFSETS;
        values = NO_VALUES;
    }

    OffsetCache(Map<T, RecordId> entries) {
        List<Map.Entry<T, RecordId>> list =
                Lists.newArrayList(entries.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<T, RecordId>>() {
            @Override
            public int compare(Entry<T, RecordId> a, Entry<T, RecordId> b) {
                return Integer.valueOf(a.getValue().getOffset()).compareTo(
                        Integer.valueOf(b.getValue().getOffset()));
            }
        });

        int n = list.size();
        offsets = new int[n];
        values = new SoftReference<?>[n];
        for (int i = 0; i < n; i++) {
            Entry<T, RecordId> entry = list.get(i);
            offsets[i] = entry.getValue().getOffset();
            values[i] = new SoftReference<T>(entry.getKey());
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized T get(int offset) {
        int i = Arrays.binarySearch(offsets, 0, length, offset);
        if (i >= 0) {
            T value = (T) values[i].get();
            if (value == null) {
                value = load(offset);
                values[i] = new SoftReference<T>(value);
            }
            return value;
        } else {
            i = ~i;
            if (length < offsets.length) {
                System.arraycopy(offsets, i, offsets, i + 1, length - i);
                System.arraycopy(values, i, values, i + 1, length - i);
            } else {
                int[] newOffsets = new int[length + SIZE_INCREMENT];
                System.arraycopy(offsets, 0, newOffsets, 0, i);
                System.arraycopy(offsets, i, newOffsets, i + 1, length - i);
                offsets = newOffsets;
                SoftReference<?>[] newValues =
                        new SoftReference<?>[length + SIZE_INCREMENT];
                System.arraycopy(values, 0, newValues, 0, i);
                System.arraycopy(values, i, newValues, i + 1, length - i);
                values = newValues;
            }

            T value = load(offset);
            offsets[i] = offset;
            values[i] = new SoftReference<T>(value);
            return value;
        }
    }

    protected abstract T load(int offset);

}
