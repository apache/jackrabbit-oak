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
package org.apache.jackrabbit.oak.plugins.value.jcr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;

import com.google.common.base.Objects;
import org.apache.jackrabbit.api.JackrabbitValue;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.ErrorValue;
import org.apache.jackrabbit.oak.plugins.value.OakValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Value} based on {@code PropertyState}.
 */
class ValueImpl implements JackrabbitValue, OakValue {
    private static final Logger LOG = LoggerFactory.getLogger(ValueImpl.class);

    private final PropertyState propertyState;
    private final Type<?> type;
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
     * @throws RepositoryException if the underlying node state cannot be accessed
     */
    ValueImpl(@Nonnull PropertyState property, int index, @Nonnull NamePathMapper namePathMapper)
            throws RepositoryException {
        checkArgument(index < property.count());
        this.propertyState = checkNotNull(property);
        this.type = getType(property);
        this.index = index;
        this.namePathMapper = checkNotNull(namePathMapper);
    }

    /**
     * Create a new {@code Value} instance
     * @param property  The property state this instance is based on
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     * @throws RepositoryException if the underlying node state cannot be accessed
     */
    ValueImpl(@Nonnull PropertyState property, @Nonnull NamePathMapper namePathMapper)
            throws RepositoryException {
        this(checkSingleValued(property), 0, namePathMapper);
    }

    private static PropertyState checkSingleValued(PropertyState property) {
        checkArgument(!property.isArray());
        return property;
    }

    /**
     * Create a new {@code Value} instance
     * @param property  The property state this instance is based on
     * @param index  The index
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @throws IllegalArgumentException if {@code index < propertyState.count()}
     */
    @Nonnull
    static Value newValue(@Nonnull PropertyState property, int index, @Nonnull NamePathMapper namePathMapper) {
        try {
            return new ValueImpl(property, index, namePathMapper);
        } catch (RepositoryException e) {
            return new ErrorValue(e);
        }
    }

    /**
     * Create a new {@code Value} instance
     * @param property  The property state this instance is based on
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    @Nonnull
    static Value newValue(@Nonnull PropertyState property, @Nonnull NamePathMapper namePathMapper) {
        try {
            return new ValueImpl(property, 0, namePathMapper);
        } catch (RepositoryException e) {
            return new ErrorValue(e);
        }
    }

    //-----------------------------------------------------------< OakValue >---

    public Blob getBlob() throws RepositoryException {
        return getValue(Type.BINARY, index);
    }

    /**
     * Same as {@link #getString()} unless that names and paths are returned in their
     * Oak representation instead of being mapped to their JCR representation.
     * @return  A String representation of the value of this property.
     */
    public String getOakString() throws RepositoryException {
        return getValue(Type.STRING, index);
    }

    //--------------------------------------------------------------< Value >---

    /**
     * @see javax.jcr.Value#getType()
     */
    @Override
    public int getType() {
        return type.tag();
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
                return getValue(Type.BOOLEAN, index);
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
                case PropertyType.DATE:
                    String value = getValue(Type.DATE, index);
                    return Conversions.convert(value).toCalendar();
                case PropertyType.LONG:
                case PropertyType.DOUBLE:
                case PropertyType.DECIMAL:
                    return Conversions.convert(getValue(Type.LONG, index)).toCalendar();
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
                    return getValue(Type.DECIMAL, index);
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
                    return getValue(Type.DOUBLE, index);
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
                    return getValue(Type.LONG, index);
                default:
                    throw new ValueFormatException("Incompatible type " + PropertyType.nameFromValue(getType()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new ValueFormatException("Error converting value to long", e);
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
    public InputStream getStream() throws IllegalStateException, RepositoryException {
        if (stream == null) {
            stream = getBlob().getNewStream();
        }
        return stream;
    }

    /**
     * @see javax.jcr.Value#getBinary()
     */
    @Override
    public Binary getBinary() throws RepositoryException {
        return new BinaryImpl(this);
    }

    @Override
    public String getContentIdentity() {
        try {
            return getBlob().getContentIdentity();
        } catch (RepositoryException e) {
            LOG.warn("Error getting content identity", e);
            return null;
        }
    }

    //-------------------------------------------------------------< Object >---

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof ValueImpl) {
            ValueImpl that = (ValueImpl) other;
            Type<?> thisType = this.type;
            if (thisType.isArray()) {
                thisType = thisType.getBaseType();
            }
            Type<?> thatType = that.type;
            if (thatType.isArray()) {
                thatType = thatType.getBaseType();
            }
            try {
                return thisType == thatType
                        && Objects.equal(
                        getValue(thatType, index),
                        that.getValue(thatType, that.index));
            } catch (RepositoryException e) {
                LOG.warn("Error while comparing values", e);
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        try {
            if (getType() == PropertyType.BINARY) {
                    return getValue(Type.BINARY, index).hashCode();
            } else {
                return getValue(Type.STRING, index).hashCode();
            }
        } catch (RepositoryException e) {
            LOG.warn("Error while calculating hash code", e);
            return 0;
        }
    }

    @Override
    public String toString() {
        try {
            return getValue(Type.STRING, index);
        } catch (RepositoryException e) {
            return e.toString();
        }
    }

    //------------------------------------------------------------< private >---

    private <T> T getValue(Type<T> type, int index) throws RepositoryException {
        try {
            return propertyState.getValue(type, index);
        } catch (IllegalRepositoryStateException e) {
            throw new RepositoryException(e);
        }
    }

    private Type<?> getType(PropertyState property) throws RepositoryException {
        try {
            return propertyState.getType();
        } catch (IllegalRepositoryStateException e) {
            throw new RepositoryException(e);
        }
    }

}