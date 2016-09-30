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

import com.google.common.collect.Maps;

/**
 * A thread-safe, mutable record table.
 */
class MutableRecordNumbers implements RecordNumbers {

    private final Object lock = new Object();

    private final Map<Integer, RecordEntry> records = Maps.newHashMap();

    @Override
    public int getOffset(int recordNumber) {
        RecordEntry entry = records.get(recordNumber);

        if (entry != null) {
            return entry.getOffset();
        }

        synchronized (lock) {
            entry = records.get(recordNumber);

            if (entry != null) {
                return entry.getOffset();
            }

            return -1;
        }
    }

    @Override
    public Iterator<Entry> iterator() {
        Map<Integer, RecordEntry> recordNumbers;

        synchronized (lock) {
            recordNumbers = Maps.newHashMap(this.records);
        }

        return new RecordNumbersIterator(recordNumbers.entrySet().iterator());
    }

    /**
     * Return the size of this table.
     *
     * @return the size of this table.
     */
    public int size() {
        synchronized (lock) {
            return records.size();
        }
    }

    /**
     * Add a new offset to this table and generate a record number for it.
     *
     * @param type   the type of the record.
     * @param offset an offset to be added to this table.
     * @return the record number associated to the offset.
     */
    int addRecord(RecordType type, int offset) {
        int recordNumber;

        synchronized (lock) {
            recordNumber = records.size();
            records.put(recordNumber, new RecordEntry(type, offset));
        }

        return recordNumber;
    }

}
