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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.nio.ByteBuffer;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

public class KeyDataType implements DataType<Object> {
    
    private final CacheType type;
    
    public KeyDataType(CacheType type) {
        this.type = type;
    }

    @Override
    public int compare(Object a, Object b) {
        return type.compareKeys(a, b);
    }

    @Override
    public int getMemory(Object obj) {
        return ((CacheValue) obj).getMemory();
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        type.writeKey(buff, obj);
    }

    @Override
    public Object read(ByteBuffer buff) {
        return type.readKey(buff);
    }

    @Override
    public void write(WriteBuffer buff, Object storage, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, cast(storage)[i]);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object storage, int len) {
        for (int i = 0; i < len; i++) {
            cast(storage)[i] = read(buff);
        }
    }

    /**
     * Cast the storage object to an array of type T.
     *
     * @param storage the storage object
     * @return the array
     */
    protected final Object[] cast(Object storage) {
        return (Object[])storage;
    }

    @Override
    public int binarySearch(Object key, Object storageObj, int size, int initialGuess) {
        Object[] storage = cast(storageObj);
        int low = 0;
        int high = size - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        while (low <= high) {
            int compare = compare(key, storage[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
            x = (low + high) >>> 1;
        }
        return ~low;
    }

    @Override
    public boolean isMemoryEstimationAllowed() {
        return true;
    }

    @Override
    public Object[] createStorage(int size) {
        return new Object[size];
    }

}