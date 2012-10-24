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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.List;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.BlobFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.util.ISO8601;

/**
 * Implementation of {@link ValueFactory} interface.
 */
public class ValueFactoryImpl implements ValueFactory {

    private final BlobFactory blobFactory;
    private final NamePathMapper namePathMapper;

    /**
     * Creates a new instance of {@code ValueFactory}.
     *
     * @param blobFactory The factory for creation of binary values
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     */
    public ValueFactoryImpl(BlobFactory blobFactory, NamePathMapper namePathMapper) {
        this.blobFactory = blobFactory;
        this.namePathMapper = namePathMapper;
    }

    /**
     * Utility method for creating a {@code Value} based on a {@code PropertyState}.
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    public static Value createValue(PropertyState property, NamePathMapper namePathMapper) {
        return new ValueImpl(property, namePathMapper);
    }

    /**
     * Utility method for creating a {@code Value} based on a {@code PropertyValue}.
     * @param property  The property value
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    public static Value createValue(PropertyValue property, NamePathMapper namePathMapper) {
        return new ValueImpl(PropertyValues.create(property), namePathMapper);
    }

    /**
     * Utility method for creating {@code Value}s based on a {@code PropertyState}.
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  A list of new {@code Value} instances
     */
    public static List<Value> createValues(PropertyState property, NamePathMapper namePathMapper) {
        List<Value> values = Lists.newArrayList();
        for (int i = 0; i < property.count(); i++) {
            values.add(new ValueImpl(property, i, namePathMapper));
        }
        return values;
    }

    //-------------------------------------------------------< ValueFactory >---

    @Override
    public Value createValue(String value) {
        return new ValueImpl(StringPropertyState.stringProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(InputStream value) {
        try {
            return createBinaryValue(value);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    public Value createValue(Binary value) {
        try {
            if (value instanceof BinaryImpl) {
                // No need to create the value again if we have it already underlying the binary
                return ((BinaryImpl) value).getBinaryValue();
            } else {
                return createBinaryValue(value.getStream());
            }
        } catch (RepositoryException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    public Value createValue(long value) {
        return new ValueImpl(LongPropertyState.createLongProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(double value) {
        return new ValueImpl(DoublePropertyState.doubleProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Calendar value) {
        return new ValueImpl(LongPropertyState.createDateProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(boolean value) {
        return new ValueImpl(BooleanPropertyState.booleanProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @Override
    public Value createValue(Node value, boolean weak) throws RepositoryException {
        return weak
            ? new ValueImpl(GenericPropertyState.weakreferenceProperty("", value.getUUID()), namePathMapper)
            : new ValueImpl(GenericPropertyState.referenceProperty("", value.getUUID()), namePathMapper);
    }

    @Override
    public Value createValue(BigDecimal value) {
        return new ValueImpl(DecimalPropertyState.decimalProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(String value, int type) throws ValueFormatException {
        if (value == null) {
            throw new ValueFormatException("null");
        }

        try {
            switch (type) {
                case PropertyType.STRING:
                    return createValue(value);
                case PropertyType.BINARY:
                    return new ValueImpl(BinaryPropertyState.binaryProperty("", value), namePathMapper);
                case PropertyType.LONG:
                    return createValue(Conversions.convert(value).toLong());
                case PropertyType.DOUBLE:
                    return createValue(Conversions.convert(value).toDouble());
                case PropertyType.DATE:
                    if (ISO8601.parse(value) == null) {
                        throw new ValueFormatException("Invalid date " + value);
                    }
                    return new ValueImpl(LongPropertyState.createDateProperty("", value), namePathMapper);
                case PropertyType.BOOLEAN:
                    return createValue(Conversions.convert(value).toBoolean());
                case PropertyType.NAME:
                    String oakName = namePathMapper.getOakName(value);
                    if (oakName == null) {
                        throw new ValueFormatException("Invalid name: " + value);
                    }
                    return new ValueImpl(GenericPropertyState.nameProperty("", oakName), namePathMapper);
                case PropertyType.PATH:
                    String oakValue = value;
                    if (value.startsWith("[") && value.endsWith("]")) {
                        // identifier path; do no change
                    } else {
                        oakValue = namePathMapper.getOakPath(value);
                    }
                    if (oakValue == null) {
                        throw new ValueFormatException("Invalid path: " + value);
                    }
                    return new ValueImpl(GenericPropertyState.pathProperty("", oakValue), namePathMapper);
                case PropertyType.REFERENCE:
                    if (!IdentifierManager.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid reference value " + value);
                    }
                    return new ValueImpl(GenericPropertyState.referenceProperty("", value), namePathMapper);
                case PropertyType.WEAKREFERENCE:
                    if (!IdentifierManager.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid weak reference value " + value);
                    }
                    return new ValueImpl(GenericPropertyState.weakreferenceProperty("", value), namePathMapper);
                case PropertyType.URI:
                    new URI(value);
                    return new ValueImpl(GenericPropertyState.uriProperty("", value), namePathMapper);
                case PropertyType.DECIMAL:
                    return createValue(Conversions.convert(value).toDecimal());
                default:
                    throw new ValueFormatException("Invalid type: " + type);
            }
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        } catch (URISyntaxException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        }
    }

    @Override
    public Binary createBinary(InputStream stream) throws RepositoryException {
        try {
            return new BinaryImpl(createBinaryValue(stream));
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

    private ValueImpl createBinaryValue(InputStream value) throws IOException {
        Blob blob = blobFactory.createBlob(value);
        return new ValueImpl(BinaryPropertyState.binaryProperty("", blob), namePathMapper);
    }

    //------------------------------------------------------------< ErrorValue >---

    /**
     * Instances of this class represent a {@code Value} which couldn't be retrieved.
     * All its accessors throw a {@code RepositoryException}.
     */
    private static class ErrorValue implements Value {
        private final Exception exception;
        private final int type;

        private ErrorValue(Exception exception, int type) {
            this.exception = exception;
            this.type = type;
        }

        @Override
        public String getString() throws RepositoryException {
            throw createException();
        }

        @Override
        public InputStream getStream() throws RepositoryException {
            throw createException();
        }

        @Override
        public Binary getBinary() throws RepositoryException {
            throw createException();
        }

        @Override
        public long getLong() throws RepositoryException {
            throw createException();
        }

        @Override
        public double getDouble() throws RepositoryException {
            throw createException();
        }

        @Override
        public BigDecimal getDecimal() throws RepositoryException {
            throw createException();
        }

        @Override
        public Calendar getDate() throws RepositoryException {
            throw createException();
        }

        @Override
        public boolean getBoolean() throws RepositoryException {
            throw createException();
        }

        @Override
        public int getType() {
            return type;
        }

        private RepositoryException createException() {
            return new RepositoryException("Inaccessible value", exception);
        }

        /**
         * Error values are never equal.
         * @return {@code false}
         */
        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public String toString() {
            return "Inaccessible value: " + exception.getMessage();
        }
    }
}
