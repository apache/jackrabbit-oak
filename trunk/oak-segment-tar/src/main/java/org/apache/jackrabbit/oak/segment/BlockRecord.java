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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;

/**
 * A record of type "BLOCK".
 */
class BlockRecord extends Record {

    private final int size;

    BlockRecord(RecordId id, int size) {
        super(id);
        this.size = size;
    }

    /**
     * Reads bytes from this block. Up to the given number of bytes are
     * read starting from the given position within this block. The number
     * of bytes read is returned.
     *
     * @param position position within this block
     * @param buffer target buffer
     * @param offset offset within the target buffer
     * @param length maximum number of bytes to read
     * @return number of bytes that could be read
     */
    public int read(int position, byte[] buffer, int offset, int length) {
        checkElementIndex(position, size);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        if (position + length > size) {
            length = size - position;
        }
        if (length > 0) {
            getSegment().readBytes(getRecordNumber(), position, buffer, offset, length);
        }
        return length;
    }

}
