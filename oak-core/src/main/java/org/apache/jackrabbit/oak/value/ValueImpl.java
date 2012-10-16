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
package org.apache.jackrabbit.oak.value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.TimeZone;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of {@link Value} based on {@code PropertyState}.
 */
public class ValueImpl implements Value {
    private static final Logger log = LoggerFactory.getLogger(ValueImpl.class);

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
        if (getType() == PropertyType.STRING || getType() == PropertyType.BINARY || getType() == PropertyType.BOOLEAN) {
            return propertyState.getValue(Type.BOOLEAN, index);
        } else {
            throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
        }
    }

    /**
     * @see javax.jcr.Value#getDate()
     */
    @Override
    public Calendar getDate() throws RepositoryException {
        Calendar cal;
        switch (getType()) {
            case PropertyType.DOUBLE:
            case PropertyType.LONG:
            case PropertyType.DECIMAL:
                cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
                cal.setTimeInMillis(getLong());
                break;
            default:
                cal = ISO8601.parse(getString());
                if (cal == null) {
                    throw new ValueFormatException("Not a date string: " + getString());
                }
        }
        return cal;
    }

    /**
     * @see javax.jcr.Value#getDecimal()
     */
    @Override
    public BigDecimal getDecimal() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.DATE:
                    Calendar cal = getDate();
                    return BigDecimal.valueOf(cal.getTimeInMillis());
                default:
                    return propertyState.getValue(Type.DECIMAL, index);
            }
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
        }
    }

    /**
     * @see javax.jcr.Value#getDouble()
     */
    @Override
    public double getDouble() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.DATE:
                    Calendar cal = getDate();
                    return cal.getTimeInMillis();
                default:
                    return propertyState.getValue(Type.DOUBLE, index);
            }
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
        }
    }

    /**
     * @see javax.jcr.Value#getLong()
     */
    @Override
    public long getLong() throws RepositoryException {
        try {
            switch (getType()) {
                case PropertyType.DATE:
                    Calendar cal = getDate();
                    return cal.getTimeInMillis();
                default:
                    return propertyState.getValue(Type.LONG, index);
            }
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
        }
    }

    /**
     * @see javax.jcr.Value#getString()
     */
    @Override
    public String getString() throws RepositoryException {
        switch (getType()) {
            case PropertyType.NAME:
                return namePathMapper.getJcrName(propertyState.getValue(Type.STRING, index));
            case PropertyType.PATH:
                String s = propertyState.getValue(Type.STRING, index);
                if (s.startsWith("[") && s.endsWith("]")) {
                    // identifier paths are returned as-is (JCR 2.0, 3.4.3.1)
                    return s;
                } else {
                    return namePathMapper.getJcrPath(s);
                }
            case PropertyType.BINARY:
                if (stream != null) {
                    throw new IllegalStateException("getStream has previously been called on this Value instance. " +
                            "In this case a new Value instance must be acquired in order to successfully call this method.");
                }
                try {
                    final InputStream is = propertyState.getValue(Type.BINARY, index).getNewStream();
                    try {
                        return CharStreams.toString(CharStreams.newReaderSupplier(
                                new InputSupplier<InputStream>() {
                                    @Override
                                    public InputStream getInput() {
                                        return is;
                                    }
                                }, Charsets.UTF_8));
                    } finally {
                        is.close();
                    }
                } catch (IOException e) {
                    throw new RepositoryException("conversion from stream to string failed", e);
                }
            default:
                return propertyState.getValue(Type.STRING, index);
        }
    }

    /**
     * @see javax.jcr.Value#getStream()
     */
    @Override
    public InputStream getStream() throws IllegalStateException, RepositoryException {
        if (stream == null) {
            stream = getNewStream();
        }
        return stream;
    }

    InputStream getNewStream() throws RepositoryException {
        switch (getType()) {
            case PropertyType.NAME:
            case PropertyType.PATH:
                return new ByteArrayInputStream(
                        getString().getBytes(Charsets.UTF_8));
        }
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
        }
        else {
            return propertyState.getValue(Type.STRING, index).hashCode();
        }
    }

    @Override
    public String toString() {
        return propertyState.getValue(Type.STRING, index);
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
        Calendar c1 = ISO8601.parse(p1);
        Calendar c2 = ISO8601.parse(p2);
        return c1 != null && c2 != null
                ? c1.compareTo(c2)
                : p1.compareTo(p2);
    }

}