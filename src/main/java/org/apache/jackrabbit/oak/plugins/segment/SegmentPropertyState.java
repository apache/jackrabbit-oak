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
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

class SegmentPropertyState extends EmptyPropertyState {

    private final SegmentReader reader;

    private final int tag;

    private final int count;

    private final ListRecord values;

    SegmentPropertyState(SegmentReader reader, String name, RecordId id) {
        super(name);

        this.reader = checkNotNull(reader);

        checkNotNull(id);
        this.tag = reader.readInt(id, 0);
        this.count = reader.readInt(id, 4);
        this.values = new ListRecord(reader.readRecordId(id, 8), count());
    }

    @Override
    public boolean isArray() {
        return count != -1;
    }

    @Override
    public int count() {
        if (isArray()) {
            return count;
        } else {
            return 1;
        }
    }

    @Override
    public Type<?> getType() {
        return Type.fromTag(tag, isArray());
    }

    @Override @Nonnull @SuppressWarnings("unchecked")
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            final Type<?> base = type.getBaseType();
            return (T) new Iterable<Object>() {
                @Override
                public Iterator<Object> iterator() {
                    return new Iterator<Object>() {
                        private int index = 0;
                        @Override
                        public boolean hasNext() {
                            return index < count();
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

        RecordId valueId = values.getEntry(reader, index);
        if (type == Type.BINARY) {
            return (T) new SegmentBlob(reader, valueId);
        } else {
            String value = reader.readString(valueId);
            if (type == Type.STRING || type == Type.URI
                    || type == Type.NAME || type == Type.PATH
                    || type == Type.REFERENCE || type == Type.WEAKREFERENCE) {
                return (T) value;
            } else {
                Converter converter = Conversions.convert(value);
                if (tag == PropertyType.DATE) {
                    converter = Conversions.convert(converter.toCalendar());
                } else if (tag == PropertyType.DECIMAL) {
                    converter = Conversions.convert(converter.toDecimal());
                } else if (tag == PropertyType.DOUBLE) {
                    converter = Conversions.convert(converter.toDouble());
                } else if (tag == PropertyType.LONG) {
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
        RecordId valueId = values.getEntry(reader, index);
        return reader.readLong(valueId, 0);
    }

}
