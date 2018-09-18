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

import java.util.Collections;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.ValueConverter;
import org.apache.jackrabbit.oak.spi.query.fulltext.LikePattern;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * A comparison operation (including "like").
 */
public class ComparisonImpl extends ConstraintImpl {
    
    private final DynamicOperandImpl operand1;
    private final Operator operator;
    private final StaticOperandImpl operand2;

    public ComparisonImpl(DynamicOperandImpl operand1, Operator operator, StaticOperandImpl operand2) {
        this.operand1 = operand1;
        this.operator = operator;
        this.operand2 = operand2;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public Operator getOperator() {
        return operator;
    }

    public StaticOperandImpl getOperand2() {
        return operand2;
    }
    
    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        PropertyExistenceImpl p = operand1.getPropertyExistence();
        if (p == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(p);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return operand1.getSelectors();
    }
    
    @Override
    public boolean evaluate() {
        // JCR 2.0 spec, 6.7.16 Comparison:
        // "operand1 may evaluate to an array of values"
        PropertyValue p1 = operand1.currentProperty();
        if (p1 == null) {
            return false;
        }
        PropertyValue p2 = operand2.currentValue();
        if (p2 == null) {
            // if the property doesn't exist, the result is always false
            // even for "null <> 'x'" (same as in SQL) 
            return false;
        }
        // "the value of operand2 is converted to the
        // property type of the value of operand1" if possible
        p2 = convertValueToType(p2, p1);
        // if not possible, convert to the same type
        if (p1.getType().tag() != p2.getType().tag()) {
            // conversion failed: convert both to binary or string
            int targetType = getCommonType(p1, p2);
            p1 = convertToType(p1, targetType);
            p2 = convertToType(p2, targetType);
        }
        if (p1.isArray()) {
            // JCR 2.0 spec, 6.7.16 Comparison:
            // "... constraint is satisfied as a whole if the comparison
            // against any element of the array is satisfied."
            Type<?> base = p1.getType().getBaseType();
            for (int i = 0; i < p1.count(); i++) {
                PropertyState value = PropertyStates.createProperty(
                        "value", p1.getValue(base, i), base);
                if (operator.evaluate(PropertyValues.create(value), p2)) {
                    return true;
                }
            }
            return false;
        } else {
            return operator.evaluate(p1, p2);
        }
    }
    
    private static int getCommonType(PropertyValue p1, PropertyValue p2) {
        if (p1.getType().tag() == PropertyType.BINARY || p2.getType().tag() == PropertyType.BINARY) {
            return PropertyType.BINARY;
        }
        return PropertyType.STRING;
    }
    
    private PropertyValue convertToType(PropertyValue v, int targetType) {
        try {
            return ValueConverter.convert(v, targetType, query.getNamePathMapper());
        } catch (IllegalArgumentException e) {
            // not possible to convert
            return v;
        }        
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return operand1 + " " + operator + " " + operand2;
    }

    @Override
    public void restrict(FilterImpl f) {
        PropertyValue v = operand2.currentValue();
        if (!ValueConverter.canConvert(
                operand2.getPropertyType(),
                operand1.getPropertyType())) {
            throw new IllegalArgumentException(
                    "Unsupported conversion from property type " + 
                            PropertyType.nameFromValue(operand2.getPropertyType()) + 
                            " to property type " +
                            PropertyType.nameFromValue(operand1.getPropertyType()));
        }
        if (v != null) {
            if (operator == Operator.LIKE) {
                String pattern;
                pattern = v.getValue(Type.STRING);
                LikePattern p = new LikePattern(pattern);
                String lowerBound = p.getLowerBound();
                if (lowerBound != null) {
                    String upperBound = p.getUpperBound();
                    if (lowerBound.equals(upperBound)) {
                        // no wildcards: equality comparison
                        // but v may contain escaped wildcards, so we can't use it
                        PropertyValue pv = PropertyValues.newString(lowerBound);
                        operand1.restrict(f, Operator.EQUAL, pv);
                    } else if (operand1.supportsRangeConditions()) {
                        if (lowerBound != null) {
                            PropertyValue pv = PropertyValues.newString(lowerBound);
                            operand1.restrict(f, Operator.GREATER_OR_EQUAL, pv);
                        }
                        if (upperBound != null) {
                            PropertyValue pv = PropertyValues.newString(upperBound);
                            operand1.restrict(f, Operator.LESS_OR_EQUAL, pv);
                        }
                    } else {
                        // path conditions
                        operand1.restrict(f, operator, v);
                    }
                } else {
                    // like '%' conditions
                    operand1.restrict(f, operator, v);
                }
            } else {
                operand1.restrict(f, operator, v);
            }
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (operand2.currentValue() != null) {
            if (operand1.canRestrictSelector(s)) {
                s.restrictSelector(this);
            }
        }
    }

    @Override
    public AstElement copyOf() {
        return new ComparisonImpl(operand1.createCopy(), operator, operand2);
    }
}
