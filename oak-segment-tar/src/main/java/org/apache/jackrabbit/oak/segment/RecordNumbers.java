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

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;

/**
 * A table to translate record numbers to offsets.
 */
interface RecordNumbers extends Iterable<Entry> {

    /**
     * Translate a record number to an offset.
     *
     * @param recordNumber A record number.
     * @return the offset corresponding to the record number, or {@code -1} if
     * no offset is associated to the record number.
     */
    int getOffset(int recordNumber);

    /**
     * Represents a pair of a record number and its corresponding offset.
     */
    interface Entry {

        /**
         * The record number part of this pair.
         *
         * @return a record number.
         */
        int getRecordNumber();

        /**
         * The offset part of this pair.
         *
         * @return an offset.
         */
        int getOffset();

    }

}
