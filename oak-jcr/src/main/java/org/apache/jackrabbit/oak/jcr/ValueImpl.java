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
package org.apache.jackrabbit.oak.jcr;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CoreValue;
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

    // TODO need utility to convert the internal NAME/PATH format to JCR format
    private final ValueFactoryImpl.DummyNamePathResolver resolver;

    /**
     * Constructs a {@code ValueImpl} object representing an SPI
     * <codeQValue</code>.
     *
     * @param value the value object this {@code ValueImpl} should represent
     * @param resolver
     */
    public ValueImpl(CoreValue value, ValueFactoryImpl.DummyNamePathResolver resolver) {
        this.value = value;
        this.resolver = resolver;
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
                return resolver.getJCRName(value.toString());
            case PropertyType.PATH:
                return resolver.getJCRPath(value.toString());
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
        return new BinaryImpl();
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

    //------------------------------------------------------------< Binary >----
    private class BinaryImpl implements Binary {

        @Override
        public InputStream getStream() throws RepositoryException {
            switch (value.getType()) {
                case PropertyType.NAME:
                case PropertyType.PATH:
                    // need to respect namespace remapping
                    try {
                        final String strValue = getString();
                        return new ByteArrayInputStream(strValue.getBytes("utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        throw new RepositoryException(e.getMessage());
                    }
                default:
                    return value.getNewStream();
            }
        }

        @Override
        public int read(byte[] b, long position) throws IOException, RepositoryException {
            // TODO
            throw new UnsupportedOperationException("implementation missing");
        }

        @Override
        public long getSize() throws RepositoryException {
            switch (value.getType()) {
                case PropertyType.NAME:
                case PropertyType.PATH:
                    // need to respect namespace remapping
                    return getString().length();
                default:
                    return value.length();
            }
        }

        @Override
        public void dispose() {
            // nothing to do
        }
    }
}