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

package org.apache.jackrabbit.oak.jcr.util;

import org.apache.jackrabbit.mk.model.Scalar;
import org.apache.jackrabbit.oak.ScalarImpl;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting between internal value representation and JCR
 * values.
 * todo: needs refactoring. see OAK-16.
 */
public final class ValueConverter {
    private ValueConverter() {}

    public static Scalar toScalar(Value value) throws RepositoryException {
        switch (value.getType()) {
            case PropertyType.STRING: {
                return ScalarImpl.stringScalar(value.getString());
            }
            case PropertyType.DOUBLE: {
                return ScalarImpl.doubleScalar(value.getDouble());
            }
            case PropertyType.LONG: {
                return ScalarImpl.longScalar(value.getLong());
            }
            case PropertyType.BOOLEAN: {
                return ScalarImpl.booleanScalar(value.getBoolean());
            }
            case PropertyType.DECIMAL:
            case PropertyType.BINARY:
            case PropertyType.DATE:
            case PropertyType.NAME:
            case PropertyType.PATH:
            case PropertyType.REFERENCE:
            case PropertyType.WEAKREFERENCE:
            case PropertyType.URI:
            default: {
                throw new UnsupportedRepositoryOperationException("toScalar"); // todo implement toScalar
            }
        }
    }
    
    public static List<Scalar> toScalar(Value[] values) throws RepositoryException {
        List<Scalar> scalars = new ArrayList<Scalar>();
        for (Value value : values) {
            if (value != null) {
                scalars.add(toScalar(value));
            }
        }
        return scalars;
    }

    public static Value toValue(ValueFactory valueFactory, Scalar scalar)
            throws UnsupportedRepositoryOperationException {

        switch (scalar.getType()) {
            case BOOLEAN:
                return valueFactory.createValue(scalar.getBoolean());
            case LONG:
                return valueFactory.createValue(scalar.getLong());
            case DOUBLE:
                return valueFactory.createValue(scalar.getDouble());
            case STRING:
                return valueFactory.createValue(scalar.getString());
            default:
                throw new UnsupportedRepositoryOperationException("toValue"); // todo implement toValue
        }
    }

    public static Value[] toValues(ValueFactory valueFactory, List<Scalar> scalars)
            throws UnsupportedRepositoryOperationException {

        Value[] values = new Value[scalars.size()];
        int k = 0;
        for (Scalar scalar : scalars) {
            values[k++] = toValue(valueFactory, scalar);
        }
        return values;
    }

}
