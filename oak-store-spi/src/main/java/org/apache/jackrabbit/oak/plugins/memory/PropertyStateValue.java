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
package org.apache.jackrabbit.oak.plugins.memory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.util.ISO8601;

/**
 * A {@link PropertyValue} implementation that wraps a {@link PropertyState}
 */
public class PropertyStateValue implements PropertyValue {

    private final PropertyState ps;

    PropertyStateValue(PropertyState ps) {
        this.ps = ps;
    }

    @Override
    public boolean isArray() {
        return ps.isArray();
    }

    @Override
    @Nonnull
    public Type<?> getType() {
        return ps.getType();
    }

    @Override
    @Nonnull
    public <T> T getValue(Type<T> type) {
        return ps.getValue(type);
    }

    @Override
    @Nonnull
    public <T> T getValue(Type<T> type, int index) {
        return ps.getValue(type, index);
    }

    @Override
    public long size() {
        return ps.size();
    }

    @Override
    public long size(int index) {
        return ps.size(index);
    }

    @Override
    public int count() {
        return ps.count();
    }

    @CheckForNull
    public PropertyState unwrap() {
        return ps;
    }

    @Override
    public int compareTo(@Nonnull PropertyValue p2) {
        if (getType().tag() != p2.getType().tag()) {
            return Integer.signum(p2.getType().tag() - getType().tag());
        }
        switch (getType().tag()) {
        case PropertyType.BOOLEAN:
            return compare(getValue(Type.BOOLEANS), p2.getValue(Type.BOOLEANS));
        case PropertyType.DECIMAL:
            return compare(getValue(Type.DECIMALS), p2.getValue(Type.DECIMALS));
        case PropertyType.DOUBLE:
            return compare(getValue(Type.DOUBLES), p2.getValue(Type.DOUBLES));
        case PropertyType.LONG:
            return compare(getValue(Type.LONGS), p2.getValue(Type.LONGS));
        case PropertyType.BINARY:
            return compareBinaries(
                    getValue(Type.BINARIES), p2.getValue(Type.BINARIES));
        case PropertyType.DATE:
            return compareAsDate(
                    getValue(Type.STRINGS), p2.getValue(Type.STRINGS));
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

    private static int compareBinaries(Iterable<Blob> p1, Iterable<Blob> p2) {
        Iterator<Blob> i1 = p1.iterator();
        Iterator<Blob> i2 = p2.iterator();
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext()) {
                return 1;
            }
            if (!i2.hasNext()) {
                return -1;
            }
            try {
                InputStream v1 = i1.next().getNewStream();
                try {
                    InputStream v2 = i2.next().getNewStream();
                    try {
                        while (true) {
                            int b1 = v1.read();
                            int b2 = v2.read();
                            int compare = b1 - b2;
                            if (compare != 0) {
                                return compare;
                            } else if (b1 == -1) {
                                break;
                            }
                        }
                    } finally {
                        v2.close();
                    }
                } finally {
                    v1.close();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to compare stream values", e);
            }
        }
        return 0;
    }

    private static int compareAsDate(Iterable<String> p1, Iterable<String> p2) {
        final char plus = '+';
        final char minus = '-';
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

            char v1sign = v1.charAt(0);
            char v2sign = v2.charAt(0);
            boolean processStringLiterals = false;
            
            if (v1sign != plus && v1sign != minus && v2sign != plus && v2sign != minus) {
                // if both the dates don't start with a sign
                String tz1 = v1.substring(23);
                String tz2 = v2.substring(23);
                if (tz1.equals(tz2)) {
                    // if we're on the same time zone we should be able to process safely a String
                    // literal of ISO8601 dates (2014-12-12T16:29:10.012Z)
                    processStringLiterals = true;
                }
            }
            
            if (processStringLiterals) {
                return v1.compareTo(v2);
            } else {
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
