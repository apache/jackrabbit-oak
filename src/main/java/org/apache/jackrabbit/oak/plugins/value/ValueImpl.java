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
package org.apache.jackrabbit.oak.plugins.value;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of {@link Value} based on {@code PropertyState}.
 */
public class ValueImpl implements Value {

    private final PropertyState propertyState;
    private final int index;
    private final NamePathMapper namePathMapper;

    private InputStream stream = null;

    /**
     * Create a new {@code Value} instance
     * @param property  The property state this instance is based on
     * @param index  The index
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @throws IllegalArgumentException if {@code index < propertyState.count()}
     */
    ValueImpl(PropertyState property, int index, NamePathMapper namePathMapper) {
        checkArgument(index < property.count());
        this.propertyState = property;
        this.index = index;
        this.namePathMapper = namePathMapper;
    }

    /**
     * Create a new {@code Value} instance
     * @param property  The property state this instance is based on
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    ValueImpl(PropertyState property, NamePathMapper namePathMapper) {
        this(checkSingleValued(property), 0, namePathMapper);
    }

    /**
     * Same as {@link #getString()} unless that names and paths are returned in their
     * Oak representation instead of being mapped to their JCR representation.
     * @return  A String representation of the value of this property.
     */
    public String getOakString() {
        return propertyState.getValue(Type.STRING, index);
    }

    private static PropertyState checkSingleValued(PropertyState property) {
        checkArgument(!property.isArray());
        return property;
    }

    //--------------------------------------------------------------< Value >---

    /**
     * @see javax.jcr.Value#getType()
     */
    @Override
    public int getType() {
        return propertyState.getType().tag();
    }

    /**
     * @see javax.jcr.Value#getBoolean()
     */
    @Override
    public boolean getBoolean() throws RepositoryException {
        switch (getType()) {
            case PropertyType.STRING:
            case PropertyType.BINARY:
            case PropertyType.BOOLEAN:
                return propertyState.getValue(Type.BOOLEAN, index);
            default:
                throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
        }
    }

    /**
     * @see javax.jcr.Value#getDate()
     */
    @Override
    public Calendar getDate() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.STRING:
                case PropertyType.BINARY:
                    String value = propertyState.getValue(Type.DATE, index);
                    return Conversions.convert(value).toCalendar();
                case PropertyType.LONG:
                case PropertyType.DOUBLE:
                case PropertyType.DATE:
                case PropertyType.DECIMAL:
                    return Conversions.convert(propertyState.getValue(Type.LONG, index)).toCalendar();
                default:
                    throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new ValueFormatException("Error converting value to date", e);
        }
    }

    /**
     * @see javax.jcr.Value#getDecimal()
     */
    @Override
    public BigDecimal getDecimal() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.STRING:
                case PropertyType.BINARY:
                case PropertyType.LONG:
                case PropertyType.DOUBLE:
                case PropertyType.DATE:
                case PropertyType.DECIMAL:
                    return propertyState.getValue(Type.DECIMAL, index);
                default:
                    throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new ValueFormatException("Error converting value to decimal", e);
        }
    }

    /**
     * @see javax.jcr.Value#getDouble()
     */
    @Override
    public double getDouble() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.STRING:
                case PropertyType.BINARY:
                case PropertyType.LONG:
                case PropertyType.DOUBLE:
                case PropertyType.DATE:
                case PropertyType.DECIMAL:
                    return propertyState.getValue(Type.DOUBLE, index);
                default:
                    throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new ValueFormatException("Error converting value to double", e);
        }
    }

    /**
     * @see javax.jcr.Value#getLong()
     */
    @Override
    public long getLong() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.STRING:
                case PropertyType.BINARY:
                case PropertyType.LONG:
                case PropertyType.DOUBLE:
                case PropertyType.DATE:
                case PropertyType.DECIMAL:
                    return propertyState.getValue(Type.LONG, index);
                default:
                    throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new ValueFormatException("Error converting value to double", e);
        }
    }

    /**
     * @see javax.jcr.Value#getString()
     */
    @Override
    public String getString() throws RepositoryException {
        checkState(getType() != PropertyType.BINARY || stream == null,
                "getStream has previously been called on this Value instance. " +
                "In this case a new Value instance must be acquired in order to successfully call this method.");

        switch (getType()) {
            case PropertyType.NAME:
                return namePathMapper.getJcrName(getOakString());
            case PropertyType.PATH:
                String s = getOakString();
                if (s.startsWith("[") && s.endsWith("]")) {
                    // identifier paths are returned as-is (JCR 2.0, 3.4.3.1)
                    return s;
                } else {
                    return namePathMapper.getJcrPath(s);
                }
            default:
                return getOakString();
        }
    }

    /**
     * @see javax.jcr.Value#getStream()
     */
    @Override
    public InputStream getStream() throws IllegalStateException {
        if (stream == null) {
            stream = getNewStream();
        }
        return stream;
    }

    InputStream getNewStream() {
        return propertyState.getValue(Type.BINARY, index).getNewStream();
    }

    long getStreamLength() {
        return propertyState.getValue(Type.BINARY, index).length();
    }

    /**
     * @see javax.jcr.Value#getBinary()
     */
    @Override
    public Binary getBinary() throws RepositoryException {
        return new BinaryImpl(this);
    }

    //-------------------------------------------------------------< Object >---

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof ValueImpl) {
            ValueImpl that = (ValueImpl) other;
            return compare(propertyState, index, that.propertyState, that.index) == 0;
        } else {
            return false;
        }
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (getType() == PropertyType.BINARY) {
            return propertyState.getValue(Type.BINARY, index).hashCode();
        } else {
            return getOakString().hashCode();
        }
    }

    @Override
    public String toString() {
        return getOakString();
    }

    private static int compare(PropertyState p1, int i1, PropertyState p2, int i2) {
        if (p1.getType().tag() != p2.getType().tag()) {
            return Integer.signum(p1.getType().tag() - p2.getType().tag());
        }
        switch (p1.getType().tag()) {
            case PropertyType.BINARY:
                return compare(p1.getValue(Type.BINARY, i1), p2.getValue(Type.BINARY, i2));
            case PropertyType.DOUBLE:
                return compare(p1.getValue(Type.DOUBLE, i1), p2.getValue(Type.DOUBLE, i2));
            case PropertyType.LONG:
                return compare(p1.getValue(Type.LONG, i1), p2.getValue(Type.LONG, i2));
            case PropertyType.DECIMAL:
                return compare(p1.getValue(Type.DECIMAL, i1), p2.getValue(Type.DECIMAL, i2));
            case PropertyType.DATE:
                return compareAsDate(p1.getValue(Type.STRING, i1), p2.getValue(Type.STRING, i2));
            default:
                return compare(p1.getValue(Type.STRING, i1), p2.getValue(Type.STRING, i2));
        }
    }

    private static <T extends Comparable<T>> int compare(T p1, T p2) {
        return p1.compareTo(p2);
    }

    private static int compareAsDate(String p1, String p2) {
        Calendar c1 = Conversions.convert(p1).toCalendar();
        Calendar c2 = Conversions.convert(p1).toCalendar();
        return c1 != null && c2 != null
                ? c1.compareTo(c2)
                : p1.compareTo(p2);
    }

}