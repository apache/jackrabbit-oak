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

import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;

import java.util.Iterator;

import javax.annotation.Nonnull;

import com.google.common.collect.AbstractIterator;

/**
 * A thread-safe, mutable record table.
 */
class MutableRecordNumbers implements RecordNumbers {
    private int[] recordEntries;
    private int size;

    public MutableRecordNumbers() {
        recordEntries = new int[16384];
        fill(recordEntries, -1);
    }

    @Override
    public int getOffset(int recordNumber) {
        int recordEntry = getRecordEntry(recordEntries, recordNumber);

        if (recordEntry == -1) {
            synchronized (this) {
                recordEntry = getRecordEntry(recordEntries, recordNumber);
            }
        }
        return recordEntry;
    }

    private static int getRecordEntry(int[] entries, int index) {
        return index * 2 >= entries.length
            ? -1
            : entries[index * 2];
    }

    @Nonnull
    @Override
    public synchronized Iterator<Entry> iterator() {
        return new AbstractIterator<Entry>() {
            final int[] entries = copyOf(recordEntries, size * 2);
            int index = 0;

            @Override
            protected Entry computeNext() {
                if (index < entries.length) {
                    return new Entry() {
                        final int recordNumber = index/2;
                        final int offset = entries[index++];
                        final RecordType type = RecordType.values()[entries[index++]];

                        @Override
                        public int getRecordNumber() {
                            return recordNumber;
                        }

                        @Override
                        public int getOffset() {
                            return offset;
                        }

                        @Override
                        public RecordType getType() {
                            return type;
                        }
                    };
                } else {
                    return endOfData();
                }
            }
        };
    }

    /**
     * Return the size of this table.
     *
     * @return the size of this table.
     */
    public synchronized int size() {
        return size;
    }

    /**
     * Add a new offset to this table and generate a record number for it.
     *
     * @param type   the type of the record.
     * @param offset an offset to be added to this table.
     * @return the record number associated to the offset.
     */
    synchronized int addRecord(RecordType type, int offset) {
        if (recordEntries.length <= size * 2) {
            recordEntries = copyOf(recordEntries, recordEntries.length * 2);
            fill(recordEntries, recordEntries.length/2, recordEntries.length, -1);
        }
        recordEntries[2 * size] = offset;
        recordEntries[2 * size + 1] = type.ordinal();
        return size++;
    }

}
