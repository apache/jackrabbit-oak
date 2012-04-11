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
package org.apache.jackrabbit.oak.jcr.query;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.query.CoreValue;
import org.apache.jackrabbit.oak.query.CoreValueFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

/**
 * Convert values to the jcr-core flavor.
 */
public class ValueConverter {

    private static final CoreValueFactory coreValueFactory = new CoreValueFactory();
    private static final ValueFactory jcrValueFactory = new SimpleValueFactory();

    private ValueConverter() {
    }

    public static CoreValue convert(Value v) throws RepositoryException {
        switch (v.getType()) {
        case PropertyType.BINARY:
            // TODO convert binary to data store entry
            throw new UnsupportedOperationException();
        case PropertyType.BOOLEAN:
            return coreValueFactory.createValue(v.getBoolean());
        case PropertyType.DATE:
            // TODO convert date
            throw new UnsupportedOperationException();
        case PropertyType.DECIMAL:
            return coreValueFactory.createValue(v.getDecimal());
        case PropertyType.DOUBLE:
            return coreValueFactory.createValue(v.getDouble());
        case PropertyType.LONG:
            return coreValueFactory.createValue(v.getLong());
        case PropertyType.NAME:
            // TODO possibly do name space mapping here
            return coreValueFactory.createValue(v.getString(), CoreValue.NAME);
        case PropertyType.PATH:
            // TODO possibly do name space mapping here
            return coreValueFactory.createValue(v.getString(), CoreValue.PATH);
        case PropertyType.REFERENCE:
            // TODO possibly do name space mapping here
            return coreValueFactory.createValue(v.getString(), CoreValue.REFERENCE);
        case PropertyType.STRING:
            return coreValueFactory.createValue(v.getString());
        default:
            throw new IllegalArgumentException("Unsupported property type " + v.getType());
        }
    }

    public static Value convert(CoreValue v) throws ValueFormatException {
        switch (v.getType()) {
        case CoreValue.BINARY:
            // TODO convert binary to data store entry
            throw new UnsupportedOperationException();
        case CoreValue.BOOLEAN:
            return jcrValueFactory.createValue(v.getBoolean());
        case CoreValue.DATE:
            // TODO convert date
            throw new UnsupportedOperationException();
        case CoreValue.DECIMAL:
            return jcrValueFactory.createValue(v.getDecimal());
        case CoreValue.DOUBLE:
            return jcrValueFactory.createValue(v.getDouble());
        case CoreValue.LONG:
            return jcrValueFactory.createValue(v.getLong());
        case CoreValue.NAME:
            // TODO possibly do name space mapping here
            return jcrValueFactory.createValue(v.getString(), CoreValue.NAME);
        case CoreValue.PATH:
            // TODO possibly do name space mapping here
            return jcrValueFactory.createValue(v.getString(), CoreValue.PATH);
        case CoreValue.REFERENCE:
            // TODO possibly do name space mapping here
            return jcrValueFactory.createValue(v.getString(), CoreValue.REFERENCE);
        case CoreValue.STRING:
            return jcrValueFactory.createValue(v.getString());
        default:
            throw new IllegalArgumentException("Unsupported property type " + v.getType());
        }
    }

}
