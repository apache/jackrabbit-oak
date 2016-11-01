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

package org.apache.jackrabbit.oak.plugins.segment;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A memory optimised map of {@code short} key to {@link RecordId} values.
 */
@Deprecated
public class RecordIdMap {
    private static final short[] NO_KEYS = new short[0];
    private static final RecordId[] NO_VALUES = new RecordId[0];

    private short[] keys = NO_KEYS;
    private RecordId[] values = NO_VALUES;

    /**
     * Associates {@code key} with {@code value} if not already present
     * @param key
     * @param value
     * @return  {@code true} if added, {@code false} if already present
     */
    @Deprecated
    public boolean put(short key, @Nonnull RecordId value) {
        if (keys.length == 0) {
            keys = new short[1];
            values = new RecordId[1];
            keys[0] = key;
            values[0] = value;
            return true;
        } else {
            int k = binarySearch(keys, key);
            if (k < 0) {
                int l = -k - 1;
                short[] newKeys = new short[keys.length + 1];
                RecordId[] newValues = new RecordId[(values.length + 1)];
                arraycopy(keys, 0, newKeys, 0, l);
                arraycopy(values, 0, newValues, 0, l);
                newKeys[l] = key;
                newValues[l] = value;
                int c = keys.length - l;
                if (c > 0) {
                    arraycopy(keys, l, newKeys, l + 1, c);
                    arraycopy(values, l, newValues, l + 1, c);
                }
                keys = newKeys;
                values = newValues;
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Returns the value associated with a given {@code key} or {@code null} if none.
     * @param key  the key to retrieve
     * @return  the value associated with a given {@code key} or {@code null} if none.
     */
    @CheckForNull
    @Deprecated
    public RecordId get(short key) {
        int k = binarySearch(keys, key);
        if (k >= 0) {
            return values[k];
        } else {
            return null;
        }
    }

    /**
     * Check whether {@code key} is present is this map.
     * @param key  the key to check for
     * @return  {@code true} iff {@code key} is present.
     */
    @Deprecated
    public boolean containsKey(short key) {
        return binarySearch(keys, key) >= 0;
    }

    /**
     * @return the number of keys in this map
     */
    @Deprecated
    public int size() {
        return keys.length;
    }

    /**
     * Retrieve the key at a given index. Keys are ordered according
     * the natural ordering of shorts.
     * @param index
     * @return the key at {@code index}
     * @throws ArrayIndexOutOfBoundsException if not {@code 0 <= index < size()}
     */
    @Deprecated
    public short getKey(int index) {
        return keys[index];
    }

    /**
     * Retrieve the value at a given index. Keys are ordered according
     * the natural ordering of shorts.
     * @param index
     * @return the value at {@code index}
     * @throws ArrayIndexOutOfBoundsException if not {@code 0 <= index < size()}
     */
    @Nonnull
    @Deprecated
    public RecordId getRecordId(int index) {
        return values[index];
    }
}
