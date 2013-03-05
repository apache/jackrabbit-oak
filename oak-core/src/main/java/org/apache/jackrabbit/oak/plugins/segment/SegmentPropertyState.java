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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.AbstractPropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

class SegmentPropertyState extends AbstractPropertyState {

    private final PropertyTemplate template;

    private final SegmentStore store;

    private final RecordId recordId;

    public SegmentPropertyState(
            PropertyTemplate template, SegmentStore store, RecordId recordId) {
        this.template = checkNotNull(template);
        this.store = checkNotNull(store);
        this.recordId = checkNotNull(recordId);
    }

    @Override @Nonnull
    public String getName() {
        return template.getName();
    }

    @Override
    public Type<?> getType() {
        return template.getType();
    }

    @Override
    public boolean isArray() {
        return getType().isArray();
    }

    @Override
    public int count() {
        if (isArray()) {
            Segment segment = store.readSegment(recordId.getSegmentId());
            return segment.readInt(recordId.getOffset());
        } else {
            return 1;
        }
    }

    @Override @Nonnull @SuppressWarnings("unchecked")
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            final int count = count();
            final Type<?> base = type.getBaseType();
            return (T) new Iterable<Object>() {
                @Override
                public Iterator<Object> iterator() {
                    return new Iterator<Object>() {
                        private int index = 0;
                        @Override
                        public boolean hasNext() {
                            return index < count;
                        }
                        @Override
                        public Object next() {
                            if (hasNext()) {
                                return getValue(base, index++);
                            } else {
                                throw new NoSuchElementException();
                            }
                        }
                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        } else {
            return getValue(type, 0);
        }
    }

    @Override
    public long size() {
        return size(0);
    }

    @Override @Nonnull @SuppressWarnings("unchecked")
    public <T> T getValue(Type<T> type, int index) {
        checkNotNull(type);
        checkArgument(!type.isArray(), "Type must not be an array type");

        Segment segment = store.readSegment(recordId.getSegmentId());

        Type<?> base = getType();
        ListRecord values;
        if (base.isArray()) {
            base = base.getBaseType();
            int size = segment.readInt(recordId.getOffset());
            RecordId listId = segment.readRecordId(recordId.getOffset() + 4);
            values = new ListRecord(listId, size);
        } else {
            values = new ListRecord(recordId, 1);
        }
        checkElementIndex(index, values.size());

        SegmentReader reader = new SegmentReader(store);
        RecordId valueId = values.getEntry(reader, index);
        if (type == Type.BINARY) {
            return (T) new SegmentBlob(reader, valueId);
        } else {
            String value = segment.readString(valueId);
            if (type == Type.STRING || type == Type.URI
                    || type == Type.NAME || type == Type.PATH
                    || type == Type.REFERENCE || type == Type.WEAKREFERENCE) {
                return (T) value;
            } else {
                Converter converter = Conversions.convert(value);
                if (base == Type.DATE) {
                    converter = Conversions.convert(converter.toCalendar());
                } else if (base == Type.DECIMAL) {
                    converter = Conversions.convert(converter.toDecimal());
                } else if (base == Type.DOUBLE) {
                    converter = Conversions.convert(converter.toDouble());
                } else if (base == Type.LONG) {
                    converter = Conversions.convert(converter.toLong());
                }
                if (type == Type.BOOLEAN) {
                    return (T) Boolean.valueOf(converter.toBoolean());
                } else if (type == Type.DATE) {
                    return (T) converter.toDate();
                } else if (type == Type.DECIMAL) {
                    return (T) converter.toDecimal();
                } else if (type == Type.DOUBLE) {
                    return (T) Double.valueOf(converter.toDouble());
                } else if (type == Type.LONG) {
                    return (T) Long.valueOf(converter.toLong());
                } else {
                    throw new UnsupportedOperationException(
                            "Unknown type: " + type);
                }
            }
        }
    }

    @Override
    public long size(int index) {
        ListRecord values;
        if (isArray()) {
            Segment segment = store.readSegment(recordId.getSegmentId());
            int size = segment.readInt(recordId.getOffset());
            RecordId listId = segment.readRecordId(recordId.getOffset() + 4);
            values = new ListRecord(listId, size);
        } else {
            values = new ListRecord(recordId, 1);
        }
        checkElementIndex(index, values.size());
        SegmentReader reader = new SegmentReader(store);
        return reader.readLength(values.getEntry(reader, 0));
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        // optimize for common cases
        if (this == object) {
            return true;
        } else if (object instanceof SegmentPropertyState) {
            SegmentPropertyState that = (SegmentPropertyState) object;
            if (recordId.equals(that.recordId)
                    && template.equals(that.template)) {
                return true;
            }
        }
        // fall back to default equality check in AbstractPropertyState
        return super.equals(object);
    }

}
