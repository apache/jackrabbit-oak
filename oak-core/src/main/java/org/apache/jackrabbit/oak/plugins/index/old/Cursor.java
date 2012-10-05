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
package org.apache.jackrabbit.oak.plugins.index.old;

import java.util.Iterator;

/**
 * A cursor to navigate in a result list.
 */
public class Cursor implements Iterator<String> {

    // TODO a cursor should be based on a specific revision
    private BTreeLeaf current;
    private int pos;
    private String currentValue;

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public String next() {
        if (current == null) {
            currentValue = null;
            return null;
        }
        String key = current.keys[pos];
        currentValue = current.values[pos];
        step();
        return key;
    }

    public String getValue() {
        return currentValue;
    }

    void step() {
        while (true) {
            pos++;
            if (pos < current.size()) {
                return;
            }
            pos = 0;
            current = current.nextLeaf();
            if (current == null || pos < current.size()) {
                return;
            }
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    void setCurrent(BTreeLeaf current, int pos) {
        this.current = current;
        this.pos = pos;
    }

    /**
     * An iterator over a cursor.
     */
    public static class RangeIterator implements Iterator<String> {
        private final Cursor cursor;
        private final String maxKey;
        private String value;

        RangeIterator(Cursor cursor, String maxKey) {
            this.cursor = cursor;
            this.maxKey = maxKey;
            step();
        }

        private void step() {
            value = null;
            if (cursor.hasNext()) {
                String k = cursor.next();
                if (maxKey == null || k.compareTo(maxKey) <= 0) {
                    value = cursor.getValue();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return value != null;
        }

        @Override
        public String next() {
            String v = value;
            step();
            return v;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
