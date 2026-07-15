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

import static java.util.Arrays.fill;

import java.util.Collections;
import java.util.Iterator;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.jetbrains.annotations.NotNull;

/**
 * A table to translate record numbers to offsets.
 */
public interface RecordNumbers extends Iterable<Entry> {

    /**
     * An always empty {@code RecordNumber} table.
     */
    RecordNumbers EMPTY_RECORD_NUMBERS = new RecordNumbers() {
        @Override
        public int getOffset(int recordNumber) {
            return -1;
        }

        @NotNull
        @Override
        public Iterator<Entry> iterator() {
            return Collections.emptyIterator();
        }
    };

    /**
     * Read the serialized table mapping record numbers to offsets.
     *
     * @return An instance of {@link RecordNumbers}
     */
    static @NotNull RecordNumbers fromSegmentData(@NotNull SegmentData data) {
        int recordNumberCount = data.getRecordReferencesCount();

        if (recordNumberCount == 0) {
            return EMPTY_RECORD_NUMBERS;
        }

        int maxIndex = data.getRecordReferenceNumber(recordNumberCount - 1);

        byte[] types = new byte[maxIndex + 1];
        int[] offsets = new int[maxIndex + 1];
        fill(offsets, -1);

        for (int i = 0; i < recordNumberCount; i++) {
            int recordNumber = data.getRecordReferenceNumber(i);
            types[recordNumber] = data.getRecordReferenceType(i);
            offsets[recordNumber] = data.getRecordReferenceOffset(i);
        }

        return new ImmutableRecordNumbers(offsets, types);
    }

    /**
     * Translate a record number to an offset.
     *
     * @param recordNumber A record number.
     * @return the offset corresponding to the record number, or {@code -1} if
     * no offset is associated to the record number.
     */
    int getOffset(int recordNumber);

    /**
     * Represents an entry in the record table.
     */
    interface Entry {

        /**
         * The record number.
         *
         * @return a record number.
         */
        int getRecordNumber();

        /**
         * The offset of this record..
         *
         * @return an offset.
         */
        int getOffset();

        /**
         * The type of this record.
         *
         * @return a record type.
         */
        RecordType getType();

    }

}
