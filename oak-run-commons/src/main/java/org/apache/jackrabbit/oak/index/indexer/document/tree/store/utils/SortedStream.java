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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import java.util.Iterator;

/**
 * A helper class to iterate over key-value pairs in a tree store, in ascending
 * key order. The class helps merging multiple streams of key-value pairs.
 *
 * Internally, it is backed by an iterator over positions in the key-value pair.
 */
public class SortedStream implements Comparable<SortedStream> {

    private final int priority;
    private final String rootFileName;
    private Iterator<Position> it;
    private String currentKey;
    private String currentValue;

    public SortedStream(int priority, String rootFileName, Iterator<Position> it) {
        this.priority = priority;
        this.rootFileName = rootFileName;
        this.it = it;
        next();
    }

    public String toString() {
        return "priority " + priority + " file " + rootFileName + " key " + currentKey + " value " + currentValue;
    }

    public String currentKeyOrNull() {
        return currentKey;
    }

    public String currentValue() {
        return currentValue;
    }

    public void next() {
        if (it.hasNext()) {
            Position pos = it.next();
            currentKey = pos.file.getKey(pos.valuePos);
            currentValue = pos.file.getValue(pos.valuePos);
        } else {
            currentKey = null;
            currentValue = null;
        }
    }

    @Override
    public int compareTo(SortedStream o) {
        if (currentKey == null) {
            if (o.currentKey == null) {
                return Integer.compare(priority, o.priority);
            }
            return 1;
        } else if (o.currentKey == null) {
            return -1;
        }
        int comp = currentKey.compareTo(o.currentKey);
        if (comp == 0) {
            return Integer.compare(priority, o.priority);
        }
        return comp;
    }
}