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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

/**
 * CoreValueImpl... TODO: javadoc
 */
class CoreValueImpl implements CoreValue {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CoreValueImpl.class);

    private final Object value;
    private final int type;

    /**
     * Create a new instance.
     *
     * @param value The value.
     * @param type The property type.
     * @throws IllegalArgumentException if the passed {@code value} is {@code null}.
     */
    private CoreValueImpl(Object value, int type) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        this.value = value;
        this.type = type;
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @param type
     * @throws IllegalArgumentException if the specified type cannot be created
     * from a string value.
     */
    CoreValueImpl(String value, int type) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        switch (type) {
            case PropertyType.STRING:
            case PropertyType.NAME:
            case PropertyType.PATH:
            case PropertyType.DATE:
            case PropertyType.REFERENCE:
            case PropertyType.WEAKREFERENCE:
            case PropertyType.URI:
                this.value = value;
                break;
            case PropertyType.BOOLEAN:
                this.value = Boolean.parseBoolean(value);
                break;
            case PropertyType.DOUBLE:
                this.value = Double.parseDouble(value);
                break;
            case PropertyType.DECIMAL:
                this.value = new BigDecimal(value);
                break;
            case PropertyType.LONG:
                this.value = Long.parseLong(value);
                break;
            default:
                // undefined property type or binary.
                // NOTE: binary must be constructed using BinaryValue -> see CoreValueFactory
                throw new IllegalArgumentException("Illegal type " + PropertyType.nameFromValue(type));
        }
        this.type = type;
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @throws IllegalArgumentException if the passed {@code value}
     * is {@code null}.
     */
    CoreValueImpl(Long value) {
        this(value, PropertyType.LONG);
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @throws IllegalArgumentException if the passed {@code value}
     * is {@code null}.
     */
    CoreValueImpl(Double value) {
        this(value, PropertyType.DOUBLE);
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @throws IllegalArgumentException if the passed {@code value}
     * is {@code null}.
     */
    CoreValueImpl(Boolean value) {
        this(value, PropertyType.BOOLEAN);
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @throws IllegalArgumentException if the passed {@code value}
     * is {@code null}.
     */
    CoreValueImpl(BigDecimal value) {
        this(value, PropertyType.DECIMAL);
    }

    /**
     * Create a new instance.
     *
     * @param value
     * @throws IllegalArgumentException if the passed {@code value}
     * is {@code null}.
     */
    CoreValueImpl(BinaryValue value) {
        this(value, PropertyType.BINARY);
    }

    //----------------------------------------------------------< CoreValue >---
    @Override
    public int getType() {
        return type;
    }

    @Override
    public String getString() {
        return value.toString();
    }

    @Override
    public long getLong() {
        long l;
        switch (getType()) {
            case PropertyType.LONG:
                l = (Long) value;
                break;
            case PropertyType.DOUBLE:
                l = ((Double) value).longValue();
                break;
            case PropertyType.DECIMAL:
                l = ((BigDecimal) value).longValue();
                break;
            default:
                l = Long.parseLong(getString());
        }
        return l;
    }

    @Override
    public double getDouble() {
        double d;
        switch (getType()) {
            case PropertyType.DOUBLE:
                d = (Double) value;
                break;
            case PropertyType.LONG:
                d = ((Long) value).doubleValue();
                break;
            case PropertyType.DECIMAL:
                d = ((BigDecimal) value).doubleValue();
                break;
            default:
                d = Double.parseDouble(getString());
        }
        return d;
    }

    @Override
    public boolean getBoolean() {
        boolean b;
        switch (getType()) {
            case PropertyType.BOOLEAN:
                b = (Boolean) value;
                break;
            case PropertyType.STRING:
            case PropertyType.BINARY:
                b = Boolean.parseBoolean(getString());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported conversion.");

        }
        return b;
    }

    @Override
    public BigDecimal getDecimal() {
        BigDecimal decimal;
        switch (getType()) {
            case PropertyType.DECIMAL:
                decimal = (BigDecimal) value;
                break;
            case PropertyType.DOUBLE:
                decimal = new BigDecimal(getDouble());
                break;
            case PropertyType.LONG:
                decimal = new BigDecimal(getLong());
                break;
            default:
                decimal = new BigDecimal(getString());
        }
        return decimal;
    }

    @Override
    public InputStream getNewStream() {
        InputStream in;
        switch (type) {
            case PropertyType.BINARY:
                in = ((BinaryValue) value).getStream();
                break;
            default:
                try {
                    in = new ByteArrayInputStream(getString().getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    // TODO: proper log output and exception handling
                    throw new RuntimeException(e);
                }
        }
        return in;
    }

    @Override
    public long length() {
        long length;
        switch (type) {
            case PropertyType.BINARY:
                length = ((BinaryValue) value).length();
                break;
            default:
                length = getString().length();
        }
        return length;
    }

    //---------------------------------------------------------< Comparable >---
    @Override
    public int compareTo(CoreValue o) {
        if (this == o) {
            return 0;
        }
        if (type != o.getType()) {
            // TODO convert?
            return type - o.getType();
        }
        switch (type) {
            case PropertyType.LONG:
                return ((Long) value).compareTo(o.getLong());
            case PropertyType.DOUBLE:
                return ((Double) value).compareTo(o.getDouble());
            case PropertyType.DECIMAL:
                return ((BigDecimal) value).compareTo(o.getDecimal());
            case PropertyType.BOOLEAN:
                return ((Boolean) value).compareTo(o.getBoolean());
            default:
                return value.toString().compareTo(o.toString());
        }

    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return type ^ value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof CoreValueImpl) {
            CoreValueImpl other = (CoreValueImpl) o;
            return type == other.type && value.equals(other.value);
        }

        return false;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}