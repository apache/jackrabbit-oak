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

import javax.annotation.Nonnull;

import com.google.common.collect.AbstractIterator;

/**
 * An immutable record table. It is initialized at construction time and can
 * never be changed afterwards.
 * <p>
 * This implementation is trivially thread-safe.
 */
class ImmutableRecordNumbers implements RecordNumbers {

    @Nonnull
    private final int[] offsets;

    @Nonnull
    private final byte[] type;

    /**
     * Create a new instance based on arrays for the offsets and types.
     * <p>
     * <em>Note:</em> for performance reasons these arrays are directly referenced
     * by this class and must not anymore be modified from other places.
     *
     * @param offsets  Offsets per position. -1 if not mapped.
     * @param type     Types per position. Not defined if not mapped.
     */
    public ImmutableRecordNumbers(@Nonnull int[] offsets, @Nonnull byte[] type) {
        this.offsets = offsets;
        this.type = type;
    }


    @Override
    public int getOffset(int recordNumber) {
        if (recordNumber < offsets.length) {
            return offsets[recordNumber];
        } else {
            return -1;
        }
    }

    @Nonnull
    @Override
    public Iterator<Entry> iterator() {
        return new AbstractIterator<Entry>() {
            private int pos = -1;
            @Override
            protected Entry computeNext() {
                while (++pos < offsets.length && offsets[pos] < 0) { }
                if (pos < offsets.length) {
                    return new Entry() {
                        @Override
                        public int getRecordNumber() {
                            return pos;
                        }

                        @Override
                        public int getOffset() {
                            return offsets[pos];
                        }

                        @Override
                        public RecordType getType() {
                            return RecordType.values()[type[pos]];
                        }
                    };
                } else {
                    return endOfData();
                }
            }
        };
    }

}
