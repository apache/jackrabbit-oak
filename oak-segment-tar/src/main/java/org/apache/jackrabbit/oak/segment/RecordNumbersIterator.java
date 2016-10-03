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

package org.apache.jackrabbit.oak.segment;

import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;

/**
 * Utility class implementing an iterator over a record table. It wraps an
 * underlying iterator looping over map entries, where each entry is a
 * representation of a record entry.
 */
class RecordNumbersIterator implements Iterator<Entry> {

    private static class Entry implements RecordNumbers.Entry {

        private final Map.Entry<Integer, RecordEntry> entry;

        public Entry(Map.Entry<Integer, RecordEntry> entry) {
            this.entry = entry;
        }

        @Override
        public int getRecordNumber() {
            return entry.getKey();
        }

        @Override
        public int getOffset() {
            return entry.getValue().getOffset();
        }

        @Override
        public RecordType getType() {
            return entry.getValue().getType();
        }

    }

    private final Iterator<Map.Entry<Integer, RecordEntry>> iterator;

    RecordNumbersIterator(Iterator<Map.Entry<Integer, RecordEntry>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public RecordNumbers.Entry next() {
        return new Entry(iterator.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
