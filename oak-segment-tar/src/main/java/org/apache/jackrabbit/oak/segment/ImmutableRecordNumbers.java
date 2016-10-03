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

import static com.google.common.collect.Maps.newLinkedHashMap;

import java.util.Iterator;
import java.util.Map;

/**
 * An immutable record table. It is initialized at construction time and can
 * never be changed afterwards.
 * <p>
 * This implementation is trivially thread-safe.
 */
class ImmutableRecordNumbers implements RecordNumbers {

    private final Map<Integer, RecordEntry> records;

    /**
     * Create a new immutable record table.
     *
     * @param records a map of record numbers to record entries. It can't be
     *                {@code null}.
     */
    ImmutableRecordNumbers(Map<Integer, RecordEntry> records) {
        this.records = newLinkedHashMap(records);
    }

    @Override
    public int getOffset(int recordNumber) {
        RecordEntry entry = records.get(recordNumber);

        if (entry == null) {
            return -1;
        }

        return entry.getOffset();
    }

    @Override
    public Iterator<Entry> iterator() {
        return new RecordNumbersIterator(records.entrySet().iterator());
    }

}
