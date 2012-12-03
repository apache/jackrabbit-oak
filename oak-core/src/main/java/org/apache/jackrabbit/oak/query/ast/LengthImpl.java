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
package org.apache.jackrabbit.oak.query.ast;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

/**
 * The function "length(..)".
 */
public class LengthImpl extends DynamicOperandImpl {

    private final PropertyValueImpl propertyValue;

    public LengthImpl(PropertyValueImpl propertyValue) {
        this.propertyValue = propertyValue;
    }

    public PropertyValueImpl getPropertyValue() {
        return propertyValue;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "length(" + propertyValue + ')';
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyValue p = propertyValue.currentProperty();
        if (p == null) {
            return null;
        }
        if (!p.isArray()) {
            long length = p.size();
            return PropertyValues.newLong(length);
        }
        // TODO what is the expected result for LENGTH(multiValueProperty)?
        throw new IllegalArgumentException("LENGTH(x) on multi-valued property is not supported");
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (v != null) {
            switch (v.getType().tag()) {
            case PropertyType.LONG:
            case PropertyType.DECIMAL:
            case PropertyType.DOUBLE:
                // ok - comparison with a number
                break;
            case PropertyType.BINARY:
            case PropertyType.STRING:
            case PropertyType.DATE:
                // ok - compare with a string literal
                break;
            default:
                throw new IllegalArgumentException(
                        "Can not compare the length with a constant of type "
                                + PropertyType.nameFromValue(v.getType().tag()) +
                                " and value " + v.toString());
            }
        }
        // LENGTH(x) implies x is not null
        propertyValue.restrict(f, Operator.NOT_EQUAL, null);
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return propertyValue.canRestrictSelector(s);
    }
    
    @Override
    int getPropertyType() {
        return PropertyType.LONG;
    }

}
