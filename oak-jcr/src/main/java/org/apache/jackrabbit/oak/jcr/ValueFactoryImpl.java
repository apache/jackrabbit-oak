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
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * CoreValueFactoryImpl...
 */
public class ValueFactoryImpl implements ValueFactory {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ValueFactoryImpl.class);

    private final CoreValueFactory factory;
    private final DummyNamePathResolver resolver; // TODO: add proper name/path conversion

    /**
     *
     * @param factory
     */
    public ValueFactoryImpl(CoreValueFactory factory/*, NamePathResolver resolver*/) {
        this.factory = factory;
        this.resolver = new DummyNamePathResolver();
    }

    public CoreValueFactory getCoreValueFactory() {
        return factory;
    }

    public Value createValue(CoreValue coreValue) {
        return new ValueImpl(coreValue, resolver);
    }

    public CoreValue getCoreValue(Value jcrValue) {
        if (jcrValue instanceof ValueImpl) {
            return ((ValueImpl) jcrValue).unwrap();
        } else {
            // TODO
            throw new UnsupportedOperationException("Unsupported Value implementation.");
        }
    }

    //-------------------------------------------------------< ValueFactory >---
    @Override
    public Value createValue(String value) {
        CoreValue cv = factory.createValue(value, PropertyType.STRING);
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(long value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(double value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(boolean value) {
        CoreValue cv = factory.createValue(value);
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(Calendar value) {
        String dateStr = ISO8601.format(value);
        CoreValue cv = factory.createValue(dateStr, PropertyType.DATE);
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(InputStream value) {
        try {
            CoreValue cv = factory.createValue(value);
            return new ValueImpl(cv, resolver);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            // JCR-2903
            IOUtils.closeQuietly(value);
        }
    }

    @Override
    public Value createValue(Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @Override
    public Value createValue(String value, int type) throws ValueFormatException {
        CoreValue cv;
        if (type == PropertyType.NAME) {
            cv = factory.createValue(resolver.getInternalName(value), type);
        } else if (type == PropertyType.PATH) {
            cv = factory.createValue(resolver.getInternalPath(value), type);
        } else {
            cv = factory.createValue(value, type);
        }

        return new ValueImpl(cv, resolver);
    }

    @Override
    public Binary createBinary(InputStream stream) throws RepositoryException {
        try {
            CoreValue value = factory.createValue(stream);
            // TODO: BINARY implementation missing
            throw new UnsupportedOperationException("BINARY handling");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            IOUtils.closeQuietly(stream);
        }
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
        return new ValueImpl(cv, resolver);
    }

    @Override
    public Value createValue(Node value, boolean weak) throws RepositoryException {
        CoreValue cv = factory.createValue(value.getUUID(), weak ? PropertyType.WEAKREFERENCE : PropertyType.REFERENCE);
        return new ValueImpl(cv, resolver);
    }


    //--------------------------------------------------------------------------
    // TODO: replace by reasonable implementation taking namespace mappings etc. into account.
    public class DummyNamePathResolver {
        String getJCRPath(String internalPath) {
            return internalPath;
        }

        String getInternalPath(String jcrPath) {
            return jcrPath;
        }

        String getJCRName(String internalName) {
            return internalName;
        }

        String getInternalName(String jcrName) {
            return jcrName;
        }
    }
}