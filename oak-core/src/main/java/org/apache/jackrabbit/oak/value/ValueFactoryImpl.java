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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ValueFactory} interface.
 */
public class ValueFactoryImpl implements ValueFactory {
    private static final Logger log = LoggerFactory.getLogger(ValueFactoryImpl.class);

    private final NamePathMapper namePathMapper;

    /**
     * Creates a new instance of {@code ValueFactory}.
     *
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     */
    public ValueFactoryImpl(NamePathMapper namePathMapper) {
        this.namePathMapper = namePathMapper;
    }

    public static Value createValue(PropertyState property, NamePathMapper namePathMapper) {
        return new ValueImpl(property, namePathMapper);
    }

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
        return new ValueImpl(PropertyStates.stringProperty("", value), namePathMapper);
    }

    @Override
    public ValueImpl createValue(InputStream value) {
        try {
            try {
                // TODO add streaming capability to ContentSession via KernelBasedBlob
                return new ValueImpl(PropertyStates.binaryProperty("", ByteStreams.toByteArray(value)), namePathMapper);
            } finally {
                value.close();
            }
        } catch (IOException ex) {
            // TODO return a value which throws on each access instead
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Value createValue(Binary value) {
        try {
            return createValue(value.getStream());
        } catch (RepositoryException ex) {
            // TODO return a value which throws on each access instead
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Value createValue(long value) {
        return new ValueImpl(PropertyStates.longProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(double value) {
        return new ValueImpl(PropertyStates.doubleProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Calendar value) {
        String dateStr = ISO8601.format(value);
        return new ValueImpl(PropertyStates.dateProperty("", dateStr), namePathMapper);
    }

    @Override
    public Value createValue(boolean value) {
        return new ValueImpl(PropertyStates.booleanProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @Override
    public Value createValue(Node value, boolean weak) throws RepositoryException {
        return weak
            ? new ValueImpl(PropertyStates.weakreferenceProperty("", value.getUUID()), namePathMapper)
            : new ValueImpl(PropertyStates.referenceProperty("", value.getUUID()), namePathMapper);
    }

    @Override
    public Value createValue(BigDecimal value) {
        return new ValueImpl(PropertyStates.decimalProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(String value, int type) throws ValueFormatException {
        if (value == null) {
            throw new ValueFormatException();
        }

        try {
            PropertyState pv;
            switch (type) {
                case PropertyType.STRING:
                    return createValue(value);
                case PropertyType.BINARY:
                    pv = PropertyStates.binaryProperty("", value.getBytes("UTF-8"));
                    break;
                case PropertyType.LONG:
                    return createValue(StringPropertyState.getLong(value));
                case PropertyType.DOUBLE:
                    return createValue(StringPropertyState.getDouble(value));
                case PropertyType.DATE:
                    if (ISO8601.parse(value) == null) {
                        throw new ValueFormatException("Invalid date " + value);
                    }
                    pv = PropertyStates.dateProperty("", value);
                    break;
                case PropertyType.BOOLEAN:
                    return createValue(StringPropertyState.getBoolean(value));
                case PropertyType.NAME:
                    String oakName = namePathMapper.getOakName(value);
                    if (oakName == null) {
                        throw new ValueFormatException("Invalid name: " + value);
                    }
                    pv = PropertyStates.nameProperty("", oakName);
                    break;
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
                    pv = PropertyStates.pathProperty("", oakValue);
                    break;
                case PropertyType.REFERENCE:
                    if (!IdentifierManager.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid reference value " + value);
                    }
                    pv = PropertyStates.referenceProperty("", value);
                    break;
                case PropertyType.WEAKREFERENCE:
                    if (!IdentifierManager.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid weak reference value " + value);
                    }
                    pv = PropertyStates.weakreferenceProperty("", value);
                    break;
                case PropertyType.URI:
                    new URI(value);
                    pv = PropertyStates.uriProperty("", value);
                    break;
                case PropertyType.DECIMAL:
                    return createValue(StringPropertyState.getDecimal(value));
                default:
                    throw new ValueFormatException("Invalid type: " + type);
            }

            return new ValueImpl(pv, namePathMapper);
        } catch (UnsupportedEncodingException e) {
            throw new ValueFormatException("Encoding UTF-8 not supported (this should not happen!)", e);
        } catch (IOException e) {
            throw new ValueFormatException(e);
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        } catch (URISyntaxException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        }
    }

    @Override
    public Binary createBinary(InputStream stream) throws RepositoryException {
        return new BinaryImpl(createValue(stream));
    }
}
