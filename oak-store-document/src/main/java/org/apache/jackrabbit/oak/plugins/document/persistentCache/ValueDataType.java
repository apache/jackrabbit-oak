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
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.StringDataType;

public class ValueDataType implements DataType {
    
    private final DocumentNodeStore docNodeStore;
    private final DocumentStore docStore;
    private final CacheType type;
    
    public ValueDataType(
            DocumentNodeStore docNodeStore,
            DocumentStore docStore, CacheType type) {
        this.docNodeStore = docNodeStore;
        this.docStore = docStore;
        this.type = type;
    }

    @Override
    public int compare(Object a, Object b) {
        return 0;
    }

    @Override
    public int getMemory(Object obj) {
        return ((CacheValue) obj).getMemory();
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        String s = type.valueToString(obj);
        StringDataType.INSTANCE.write(buff, s);
    }

    @Override
    public Object read(ByteBuffer buff) {
        String s = StringDataType.INSTANCE.read(buff);
        return type.valueFromString(docNodeStore, docStore, s);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }
    
}