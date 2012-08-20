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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Calendar;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ValueFactory} interface based on the
 * {@link CoreValueFactory} exposed by the
 * {@link org.apache.jackrabbit.oak.api.ContentSession#getCoreValueFactory()}
 * being aware of namespaces remapped on the editing session.
 */
public class ValueFactoryImpl implements ValueFactory {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ValueFactoryImpl.class);

    private final CoreValueFactory factory;
    private final NamePathMapper namePathMapper;

    /**
     * Creates a new instance of {@code ValueFactory}.
     *
     * @param factory The core value factory.
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     */
    public ValueFactoryImpl(CoreValueFactory factory, NamePathMapper namePathMapper) {
        this.factory = factory;
        this.namePathMapper = namePathMapper;
    }

    public CoreValueFactory getCoreValueFactory() {
        return factory;
    }

    public Value createValue(CoreValue coreValue) {
        return new ValueImpl(coreValue, namePathMapper);
    }

    public CoreValue getCoreValue(Value jcrValue) {
        ValueImpl v;
        if (jcrValue instanceof ValueImpl) {
            v = (ValueImpl) jcrValue;
        } else {
            // TODO add proper implementation
            try {
                switch (jcrValue.getType()) {
                    case PropertyType.BINARY:
                        v = (ValueImpl) createValue(jcrValue.getStream());
                        break;
                    default:
                        v = (ValueImpl) createValue(jcrValue.getString(), jcrValue.getType());
                }
            } catch (RepositoryException e) {
                throw new UnsupportedOperationException("Not implemented yet...");
            }
        }

        return v.unwrap();
    }

    //-------------------------------------------------------< ValueFactory >---
    @Override
    public Value createValue(String value) {
        CoreValue cv = factory.createValue(value, PropertyType.STRING);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(long value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(double value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(boolean value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(Calendar value) {
        String dateStr = ISO8601.format(value);
        CoreValue cv = factory.createValue(dateStr, PropertyType.DATE);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(InputStream value) {
        try {
            CoreValue cv = factory.createValue(value);
            return new ValueImpl(cv, namePathMapper);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Value createValue(Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @Override
    public Value createValue(String value, int type) throws ValueFormatException {

        if (value == null) {
            throw new ValueFormatException();
        }

        try {
            CoreValue cv;

            switch (type) {
                case PropertyType.NAME:
                    String oakName = namePathMapper.getOakName(value);
                    if (oakName == null) {
                        throw new ValueFormatException("Invalid name: " + value);
                    }
                    cv = factory.createValue(oakName, type);
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
                    cv = factory.createValue(oakValue, type);
                    break;

                case PropertyType.DATE:
                    if (ISO8601.parse(value) == null) {
                        throw new ValueFormatException("Invalid date " + value);
                    }
                    cv = factory.createValue(value, type);
                    break;

                case PropertyType.REFERENCE:
                case PropertyType.WEAKREFERENCE:
                    if (!IdentifierManager.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid reference value " + value);
                    }
                    cv = factory.createValue(value, type);
                    break;

                case PropertyType.BINARY:
                    cv = factory.createValue(new ByteArrayInputStream(value.getBytes("UTF-8")));
                    break;

                default:
                    cv = factory.createValue(value, type);
                    break;
            }

            return new ValueImpl(cv, namePathMapper);
        } catch (UnsupportedEncodingException e) {
            throw new ValueFormatException("Encoding UTF-8 not supported (this should not happen!)", e);
        } catch (IOException e) {
            throw new ValueFormatException(e);
        } catch (NumberFormatException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type));
        }
    }

    @Override
    public Binary createBinary(InputStream stream) throws RepositoryException {
        ValueImpl value = (ValueImpl) createValue(stream);
        return new BinaryImpl(value);
    }

    @Override
    public Value createValue(Binary value) {
        try {
            return createValue(value.getStream());
        } catch (RepositoryException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Value createValue(BigDecimal value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, namePathMapper);
    }

    @Override
    public Value createValue(Node value, boolean weak) throws RepositoryException {
        CoreValue cv = factory.createValue(value.getUUID(), weak ? PropertyType.WEAKREFERENCE : PropertyType.REFERENCE);
        return new ValueImpl(cv, namePathMapper);
    }
}
