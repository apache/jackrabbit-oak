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
package org.apache.jackrabbit.oak.jcr.value;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * ValueImpl...
 */
class ValueImpl implements Value {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ValueImpl.class);

    private final CoreValue value;
    private final NamePathMapper namePathMapper;

    /**
     * Constructs a {@code ValueImpl} object based on a {@code CoreValue}
     *
     * @param value the value object this {@code ValueImpl} should represent
     * @param namePathMapper
     */
    public ValueImpl(CoreValue value, NamePathMapper namePathMapper) {
        this.value = value;
        this.namePathMapper = namePathMapper;
    }

    CoreValue unwrap() {
        return value;
    }

    //--------------------------------------------------------------< Value >---
    /**
     * @see javax.jcr.Value#getType()
     */
    @Override
    public int getType() {
        return value.getType();
    }

    /**
     * @see javax.jcr.Value#getBoolean()
     */
    @Override
    public boolean getBoolean() throws RepositoryException {
        if (getType() == PropertyType.STRING || getType() == PropertyType.BINARY || getType() == PropertyType.BOOLEAN) {
            return value.getBoolean();
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
            return value.getDecimal();
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
            return value.getDouble();
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
            return value.getLong();
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
            case PropertyType.NAME :
                return namePathMapper.getJcrName(value.toString());
            case PropertyType.PATH:
                return namePathMapper.getJcrPath(value.toString());
            case PropertyType.BINARY:
                InputStream stream = getStream();
                try {
                    return IOUtils.toString(stream, "UTF-8");
                } catch (IOException e) {
                    throw new RepositoryException("conversion from stream to string failed", e);
                } finally {
                    IOUtils.closeQuietly(stream);
                }
            default:
                return value.toString();
        }
    }


    /**
     * @see javax.jcr.Value#getStream()
     */
    @Override
    public InputStream getStream() throws IllegalStateException, RepositoryException {
        InputStream stream;
        switch (getType()) {
            case PropertyType.NAME:
            case PropertyType.PATH:
                try {
                    stream = new ByteArrayInputStream(getString().getBytes("UTF-8"));
                } catch (UnsupportedEncodingException ex) {
                    throw new RepositoryException("UTF-8 is not supported", ex);
                }
                break;
            default:
                stream = value.getNewStream();
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
}