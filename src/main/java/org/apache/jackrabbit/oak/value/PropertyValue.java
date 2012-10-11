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
package org.apache.jackrabbit.oak.value;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.util.ISO8601;

public class PropertyValue implements Comparable<PropertyValue> {

    private final PropertyState ps;

    protected PropertyValue(PropertyState ps) {
        this.ps = ps;
    }

    public boolean isArray() {
        return ps.isArray();
    }

    @Nonnull
    public Type<?> getType() {
        return ps.getType();
    }

    @Nonnull
    public <T> T getValue(Type<T> type) {
        return ps.getValue(type);
    }

    @Nonnull
    public <T> T getValue(Type<T> type, int index) {
        return ps.getValue(type, index);
    }

    public List<PropertyValue> values() {
        List<PropertyValue> pvs = Lists.newArrayList();

        int type = getType().tag();
        switch (type) {
            case PropertyType.STRING:
                for (String value : getValue(Type.STRINGS)) {
                    pvs.add(PropertyValues.newString(value));
                }
                break;
            case PropertyType.BINARY:
                for (Blob value : getValue(Type.BINARIES)) {
                    pvs.add(PropertyValues.newBinary(value));
                }
                break;
            case PropertyType.LONG:
                for (Long value : getValue(Type.LONGS)) {
                    pvs.add(PropertyValues.newLong(value));
                }
                break;
            case PropertyType.DOUBLE:
                for (Double value : getValue(Type.DOUBLES)) {
                    pvs.add(PropertyValues.newDouble(value));
                }
                break;
            case PropertyType.DATE:
                for (String value : getValue(Type.DATES)) {
                    pvs.add(PropertyValues.newDate(value));
                }
                break;
            case PropertyType.BOOLEAN:
                for (Boolean value : getValue(Type.BOOLEANS)) {
                    pvs.add(PropertyValues.newBoolean(value));
                }
                break;
            case PropertyType.NAME:
                for (String value : getValue(Type.NAMES)) {
                    pvs.add(PropertyValues.newName(value));
                }
                break;
            case PropertyType.PATH:
                for (String value : getValue(Type.PATHS)) {
                    pvs.add(PropertyValues.newPath(value));
                }
                break;
            case PropertyType.REFERENCE:
                for (String value : getValue(Type.REFERENCES)) {
                    pvs.add(PropertyValues.newReference(value));
                }
                break;
            case PropertyType.WEAKREFERENCE:
                for (String value : getValue(Type.WEAKREFERENCES)) {
                    pvs.add(PropertyValues.newWeakReference(value));
                }
                break;
            case PropertyType.URI:
                for (String value : getValue(Type.URIS)) {
                    pvs.add(PropertyValues.newUri(value));
                }
                break;
            case PropertyType.DECIMAL:
                for (BigDecimal value : getValue(Type.DECIMALS)) {
                    pvs.add(PropertyValues.newDecimal(value));
                }
                break;
            default:
                throw new IllegalStateException("Invalid type: " + getType());
        }
        return pvs;
    }

    public long size() {
        return ps.size();
    }

    public long size(int index) {
        return ps.size(index);
    }

    public int count() {
        return ps.count();
    }

    @CheckForNull
    public PropertyState unwrap() {
        return ps;
    }

    @Override
    public int compareTo(PropertyValue p2) {
        if (getType().tag() != p2.getType().tag()) {
            return Integer.signum(getType().tag() - p2.getType().tag());
        }
        switch (getType().tag()) {
        case PropertyType.BINARY:
            return compare(getValue(Type.BINARIES), p2.getValue(Type.BINARIES));
        case PropertyType.DOUBLE:
            return compare(getValue(Type.DOUBLES), p2.getValue(Type.DOUBLES));
        case PropertyType.DATE:
            return compareAsDate(getValue(Type.STRINGS),
                    p2.getValue(Type.STRINGS));
        default:
            return compare(getValue(Type.STRINGS), p2.getValue(Type.STRINGS));
        }
    }

    private static <T extends Comparable<T>> int compare(Iterable<T> p1,
            Iterable<T> p2) {
        Iterator<T> i1 = p1.iterator();
        Iterator<T> i2 = p2.iterator();
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext()) {
                return 1;
            }
            if (!i2.hasNext()) {
                return -1;
            }
            int compare = i1.next().compareTo(i2.next());
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static int compareAsDate(Iterable<String> p1, Iterable<String> p2) {
        Iterator<String> i1 = p1.iterator();
        Iterator<String> i2 = p2.iterator();
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext()) {
                return 1;
            }
            if (!i2.hasNext()) {
                return -1;
            }
            String v1 = i1.next();
            String v2 = i2.next();

            Calendar c1 = ISO8601.parse(v1);
            Calendar c2 = ISO8601.parse(v2);
            int compare = -1;
            if (c1 != null && c2 != null) {
                compare = c1.compareTo(c2);
            } else {
                compare = v1.compareTo(v2);
            }
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    // --------------------------------------------------------------< Object >

    private String getInternalString() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iterator = getValue(Type.STRINGS).iterator();
        while (iterator.hasNext()) {
            sb.append(iterator.next());
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return getType().tag() ^ getInternalString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof PropertyValue) {
            return compareTo((PropertyValue) o) == 0;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getInternalString();
    }

}
