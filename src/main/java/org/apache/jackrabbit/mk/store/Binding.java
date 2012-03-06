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
package org.apache.jackrabbit.mk.store;

import java.util.Iterator;

/**
 *
 */
public interface Binding {
    
    void write(String key, String value) throws Exception;
    void write(String key, byte[] value) throws Exception;
    void write(String key, long value) throws Exception;
    void write(String key, int value) throws Exception;
    void writeMap(String key, int count, StringEntryIterator iterator) throws Exception;
    void writeMap(String key, int count, BytesEntryIterator iterator) throws Exception;

    String readStringValue(String key) throws Exception;
    byte[] readBytesValue(String key) throws Exception;
    long readLongValue(String key) throws Exception;
    int readIntValue(String key) throws Exception;
    StringEntryIterator readStringMap(String key) throws Exception;
    BytesEntryIterator readBytesMap(String key) throws Exception;

    static abstract class Entry<V> {
        String key;
        V value;

        public Entry(String key, V value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    static class StringEntry extends Entry<String> {

        public StringEntry(String key, String value) {
            super(key, value);
        }

        public String getValue() {
            return value;
        }
    }

    static class BytesEntry extends Entry<byte[]> {

        public BytesEntry(String key, byte[] value) {
            super(key, value);
        }

        public byte[] getValue() {
            return value;
        }
    }

    static interface StringEntryIterator extends Iterator<StringEntry> {
    }

    static interface BytesEntryIterator extends Iterator<BytesEntry> {
    }
}
