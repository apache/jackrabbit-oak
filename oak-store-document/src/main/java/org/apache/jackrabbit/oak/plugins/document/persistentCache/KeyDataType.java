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

import static org.apache.jackrabbit.oak.plugins.document.persistentCache.DataTypeUtil.cast;

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

    @Override
    public int binarySearch(Object key, Object storage, int size, int initialGuess) {
        return DataTypeUtil.binarySearch(this, key, storage, size, initialGuess);
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