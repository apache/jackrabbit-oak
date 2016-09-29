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
 * An immutable record number to offset table. It is initialized at construction
 * time and can never be changed afterwards.
 * <p>
 * This implementation is trivially thread-safe.
 */
class ImmutableRecordNumbers implements RecordNumbers {

    private final Map<Integer, Integer> recordNumbers;

    /**
     * Create a new immutable record number to offset table.
     *
     * @param recordNumbers a map of record numbers to offsets. It can't be
     *                      {@code null}.
     */
    ImmutableRecordNumbers(Map<Integer, Integer> recordNumbers) {
        this.recordNumbers = Maps.newHashMap(recordNumbers);
    }

    @Override
    public int getOffset(int recordNumber) {
        Integer offset = recordNumbers.get(recordNumber);

        if (offset == null) {
            return -1;
        }

        return offset;
    }

    @Override
    public Iterator<Entry> iterator() {
        return new RecordNumbersIterator(recordNumbers.entrySet().iterator());
    }

}
