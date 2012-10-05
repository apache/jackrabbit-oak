/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;

import static java.util.Collections.singleton;

abstract class SinglePropertyState extends EmptyPropertyState {

    protected SinglePropertyState(String name) {
        super(name);
    }

    protected abstract String getString();

    protected Blob getBlob() {
        return new StringBasedBlob(getString());
    }

    protected long getLong() {
        return Long.parseLong(getString());
    }

    protected double getDouble() {
        return Double.parseDouble(getString());
    }

    protected boolean getBoolean() {
        throw new UnsupportedOperationException("Unsupported conversion.");
    }

    protected BigDecimal getDecimal() {
        return new BigDecimal(getString());
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) singleton(getString());
                case PropertyType.BINARY: return (T) singleton(getBlob());
                case PropertyType.LONG: return (T) singleton(getLong());
                case PropertyType.DOUBLE: return (T) singleton(getDouble());
                case PropertyType.DATE: return (T) singleton(getString());
                case PropertyType.BOOLEAN: return (T) singleton(getBoolean());
                case PropertyType.NAME: return (T) singleton(getString());
                case PropertyType.PATH: return (T) singleton(getString());
                case PropertyType.REFERENCE: return (T) singleton(getString());
                case PropertyType.WEAKREFERENCE: return (T) singleton(getString());
                case PropertyType.URI: return (T) singleton(getString());
                case PropertyType.DECIMAL: return (T) singleton(getDecimal());
                default: throw new IllegalArgumentException("Invalid primitive type:" + type);
            }
        }
        else {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) getString();
                case PropertyType.BINARY: return (T) getBlob();
                case PropertyType.LONG: return (T) (Long) getLong();
                case PropertyType.DOUBLE: return (T) (Double) getDouble();
                case PropertyType.DATE: return (T) getString();
                case PropertyType.BOOLEAN: return (T) (Boolean) getBoolean();
                case PropertyType.NAME: return (T) getString();
                case PropertyType.PATH: return (T) getString();
                case PropertyType.REFERENCE: return (T) getString();
                case PropertyType.WEAKREFERENCE: return (T) getString();
                case PropertyType.URI: return (T) getString();
                case PropertyType.DECIMAL: return (T) getDecimal();
                default: throw new IllegalArgumentException("Invalid array type:" + type);
            }
        }
    }

    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (type.isArray()) {
            throw new IllegalArgumentException("Nested arrows not supported");
        }
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return getValue(type);
    }

    @Override
    public long size() {
        return getString().length();
    }

    @Override
    public long size(int index) {
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return size();
    }

    @Override
    public int count() {
        return 1;
    }

}
