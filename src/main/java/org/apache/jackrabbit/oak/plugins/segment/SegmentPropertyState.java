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
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.AbstractPropertyState;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

class SegmentPropertyState extends Record implements PropertyState {

    private final PropertyTemplate template;

    public SegmentPropertyState(
            Segment segment, RecordId id, PropertyTemplate template) {
        super(segment, id);
        this.template = checkNotNull(template);
    }

    private ListRecord getValueList(Segment segment) {
        RecordId listId = getRecordId();
        int size = 1;
        if (isArray()) {
            size = segment.readInt(getOffset());
            if (size > 0) {
                listId = segment.readRecordId(getOffset(4));
            }
        }
        return new ListRecord(segment, listId, size);
    }

    Map<String, RecordId> getValueRecords() {
        if (getType().tag() == PropertyType.BINARY) {
            return emptyMap();
        }

        Map<String, RecordId> map = newHashMap();

        Segment segment = getSegment();
        ListRecord values = getValueList(segment);
        for (int i = 0; i < values.size(); i++) {
            RecordId valueId = values.getEntry(i);
            String value = segment.readString(valueId);
            map.put(value, valueId);
        }

        return map;
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
            return getSegment().readInt(getOffset());
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

        Segment segment = getSegment();
        ListRecord values = getValueList(segment);
        checkElementIndex(index, values.size());

        Type<?> base = getType();
        if (base.isArray()) {
            base = base.getBaseType();
        }

        RecordId valueId = values.getEntry(index);
        if (type == Type.BINARY) {
            return (T) new SegmentBlob(segment, valueId);
        } else {
            String value = segment.readString(valueId);
            if (type == Type.STRING || type == Type.URI || type == Type.DATE
                    || type == Type.NAME || type == Type.PATH
                    || type == Type.REFERENCE || type == Type.WEAKREFERENCE) {
                return (T) value;
            } else {
                Converter converter = Conversions.convert(value, base);
                if (type == Type.BOOLEAN) {
                    return (T) Boolean.valueOf(converter.toBoolean());
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
        Segment segment = getSegment();
        ListRecord values = getValueList(segment);
        checkElementIndex(index, values.size());
        return segment.readLength(values.getEntry(0));
    }


    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        // optimize for common cases
        if (this == object) { // don't use fastEquals here due to value sharing
            return true;
        } else if (object instanceof SegmentPropertyState) {
            SegmentPropertyState that = (SegmentPropertyState) object;
            if (!template.equals(that.template)) {
                return false;
            } else if (getRecordId().equals(that.getRecordId())) {
                return true;
            }
        }
        // fall back to default equality check in AbstractPropertyState
        return object instanceof PropertyState
                && AbstractPropertyState.equal(this, (PropertyState) object);
    }

    @Override
    public int hashCode() {
        return AbstractPropertyState.hashCode(this);
    }

    @Override
    public String toString() {
        return AbstractPropertyState.toString(this);
    }

}
