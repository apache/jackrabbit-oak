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
package org.apache.jackrabbit.oak.spi.query;

import java.util.Calendar;
import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.util.ISO8601;

/**
 * A {@link PropertyValue} implementation that wraps a {@link PropertyState}
 * 
 */
public class PropertyStateValue implements PropertyValue {

    private final PropertyState ps;

    protected PropertyStateValue(PropertyState ps) {
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
        } else if (o instanceof PropertyStateValue) {
            return compareTo((PropertyStateValue) o) == 0;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getInternalString();
    }

}
