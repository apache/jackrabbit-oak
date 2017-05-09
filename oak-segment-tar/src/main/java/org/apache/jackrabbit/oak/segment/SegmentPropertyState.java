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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.DECIMAL;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.PATH;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.URI;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

/**
 * A property, which can read a value or list record from a segment. It
 * currently doesn't cache data.
 * <p>
 * Depending on the property type, this is a record of type "VALUE" or a record
 * of type "LIST" (for arrays).
 */
public class SegmentPropertyState extends Record implements PropertyState {
    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final String name;

    @Nonnull
    private final Type<?> type;

    SegmentPropertyState(@Nonnull SegmentReader reader, @Nonnull RecordId id,
                         @Nonnull String name, @Nonnull Type<?> type) {
        super(id);
        this.reader = checkNotNull(reader);
        this.name = checkNotNull(name);
        this.type = checkNotNull(type);
    }

    SegmentPropertyState(@Nonnull SegmentReader reader, @Nonnull RecordId id,
                         @Nonnull PropertyTemplate template) {
        this(reader, id, template.getName(), template.getType());
    }

    private ListRecord getValueList(Segment segment) {
        RecordId listId = getRecordId();
        int size = 1;
        if (isArray()) {
            size = segment.readInt(getRecordNumber());
            if (size > 0) {
                listId = segment.readRecordId(getRecordNumber(), 4);
            }
        }
        return new ListRecord(listId, size);
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
            String value = reader.readString(valueId);
            map.put(value, valueId);
        }

        return map;
    }

    @Override @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public Type<?> getType() {
        return type;
    }

    @Override
    public boolean isArray() {
        return getType().isArray();
    }

    @Override
    public int count() {
        if (isArray()) {
            return getSegment().readInt(getRecordNumber());
        } else {
            return 1;
        }
    }

    @Override @Nonnull @SuppressWarnings("unchecked")
    public <T> T getValue(Type<T> type) {
        Segment segment = getSegment();
        if (isArray()) {
            checkState(type.isArray());
            ListRecord values = getValueList(segment);
            if (values.size() == 0) {
                return (T) emptyList();
            } else if (values.size() == 1) {
                return (T) singletonList(getValue(values.getEntry(0), type.getBaseType()));
            } else {
                Type<?> base = type.getBaseType();
                List<Object> list = newArrayListWithCapacity(values.size());
                for (RecordId id : values.getEntries()) {
                    list.add(getValue(id, base));
                }
                return (T) list;
            }
        } else {
            RecordId id = getRecordId();
            if (type.isArray()) {
                return (T) singletonList(
                        getValue(id, type.getBaseType()));
            } else {
                return getValue(id, type);
            }
        }
    }

    @Override
    public long size() {
        return size(0);
    }

    @Override @Nonnull
    public <T> T getValue(Type<T> type, int index) {
        checkNotNull(type);
        checkArgument(!type.isArray(), "Type must not be an array type");

        Segment segment = getSegment();
        ListRecord values = getValueList(segment);
        checkElementIndex(index, values.size());
        return getValue(values.getEntry(index), type);
    }

    @SuppressWarnings("unchecked")
    private <T> T getValue(RecordId id, Type<T> type) {
        if (type == BINARY) {
            return (T) reader.readBlob(id); // load binaries lazily
        }

        String value = reader.readString(id);
        if (type == STRING || type == URI || type == DATE
                || type == NAME || type == PATH
                || type == REFERENCE || type == WEAKREFERENCE) {
            return (T) value; // no conversion needed for string types
        }

        Type<?> base = getType();
        if (base.isArray()) {
            base = base.getBaseType();
        }
        Converter converter = Conversions.convert(value, base);
        if (type == BOOLEAN) {
            return (T) Boolean.valueOf(converter.toBoolean());
        } else if (type == DECIMAL) {
            return (T) converter.toDecimal();
        } else if (type == DOUBLE) {
            return (T) Double.valueOf(converter.toDouble());
        } else if (type == LONG) {
            return (T) Long.valueOf(converter.toLong());
        } else {
            throw new UnsupportedOperationException(
                    "Unknown type: " + type);
        }
    }

    @Override
    public long size(int index) {
        ListRecord values = getValueList(getSegment());
        checkElementIndex(index, values.size());
        RecordId entry = values.getEntry(index);

        if (getType().equals(BINARY) || getType().equals(BINARIES)) {
            return reader.readBlob(entry).length();
        }

        return Segment.readLength(entry);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        // optimize for common cases
        if (this == object) { // don't use fastEquals here due to value sharing
            return true;
        }
        if (object instanceof SegmentPropertyState) {
            SegmentPropertyState that = (SegmentPropertyState) object;
            if (!type.equals(that.type) || !name.equals(that.name)) {
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
