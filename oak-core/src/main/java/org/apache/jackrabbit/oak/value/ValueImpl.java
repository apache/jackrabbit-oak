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

/**
 * ValueImpl...
 */
public class ValueImpl implements Value {
    private static final Logger log = LoggerFactory.getLogger(ValueImpl.class);

    private final PropertyValue value;
    private final NamePathMapper namePathMapper;

    private InputStream stream = null;

    ValueImpl(PropertyValue value, NamePathMapper namePathMapper) {
        this.value = value;
        this.namePathMapper = namePathMapper;
    }

    public ValueImpl(PropertyState property, NamePathMapper namePathMapper) {
        this(PropertyValues.create(property), namePathMapper);
    }

    //--------------------------------------------------------------< Value >---
    /**
     * @see javax.jcr.Value#getType()
     */
    @Override
    public int getType() {
        return value.getType().tag();
    }

    /**
     * @see javax.jcr.Value#getBoolean()
     */
    @Override
    public boolean getBoolean() throws RepositoryException {
        if (getType() == PropertyType.STRING || getType() == PropertyType.BINARY || getType() == PropertyType.BOOLEAN) {
            return value.getValue(Type.BOOLEAN);
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
                    return value.getValue(Type.DECIMAL);
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
                    return value.getValue(Type.DOUBLE);
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
                    return value.getValue(Type.LONG);
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
                return namePathMapper.getJcrName(value.getValue(Type.STRING));
            case PropertyType.PATH:
                String s = value.getValue(Type.STRING);
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
                    final InputStream is = value.getValue(Type.BINARY).getNewStream();
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
                return value.getValue(Type.STRING);
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
        return value.getValue(Type.BINARY).getNewStream();
    }

    long getStreamLength() {
        return value.getValue(Type.BINARY).length();
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
    public boolean equals(Object obj) {
        if (obj instanceof ValueImpl) {
            return value.equals(((ValueImpl) obj).value);
        } else {
            return false;
        }
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value.toString();
    }

}