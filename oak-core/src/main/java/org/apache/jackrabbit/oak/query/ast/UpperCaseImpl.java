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

import static org.apache.jackrabbit.oak.api.Type.STRING;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValue;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

/**
 * The function "upper(..)".
 */
public class UpperCaseImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl operand;

    public UpperCaseImpl(DynamicOperandImpl operand) {
        this.operand = operand;
    }

    public DynamicOperandImpl getOperand() {
        return operand;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "upper(" + operand + ')';
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyState p = operand.currentProperty();
        if (p == null) {
            return null;
        }
        // TODO what is the expected result of UPPER(x) for an array property?
        // currently throws an exception
        String value = p.getValue(STRING);
        return PropertyValues.newString(value.toUpperCase());
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        // UPPER(x) implies x is not null
        operand.restrict(f, Operator.NOT_EQUAL, null);
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return operand.canRestrictSelector(s);
    }

}
