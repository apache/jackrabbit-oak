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
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.fulltext.LikePattern;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

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
    public Map<DynamicOperandImpl, Set<StaticOperandImpl>> getInMap() {
        if (operator == Operator.EQUAL) {
            return Collections.singletonMap(operand1, Collections.singleton(operand2));
        }
        return Collections.emptyMap();
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
        // property type of the value of operand1"
        try {
            p2 = convertValueToType(p2, p1);
        } catch (IllegalArgumentException ex) {
            // unable to convert, just skip this node
            return false;
        }
        if (p1.isArray()) {
            // JCR 2.0 spec, 6.7.16 Comparison:
            // "... constraint is satisfied as a whole if the comparison
            // against any element of the array is satisfied."
            Type<?> base = p1.getType().getBaseType();
            for (int i = 0; i < p1.count(); i++) {
                PropertyState value = PropertyStates.createProperty(
                        "value", p1.getValue(base, i), base);
                if (evaluate(PropertyValues.create(value), p2)) {
                    return true;
                }
            }
            return false;
        } else {
            return evaluate(p1, p2);
        }
    }

    /**
     * "operand2 always evaluates to a scalar value"
     * 
     * for multi-valued properties: if any of the value matches, then return true
     * 
     * @param p1
     * @param p2
     * @return
     */
    private boolean evaluate(PropertyValue p1, PropertyValue p2) {
        switch (operator) {
        case EQUAL:
            return PropertyValues.match(p1, p2);
        case NOT_EQUAL:
            return PropertyValues.notMatch(p1, p2);
        case GREATER_OR_EQUAL:
            return p1.compareTo(p2) >= 0;
        case GREATER_THAN:
            return p1.compareTo(p2) > 0;
        case LESS_OR_EQUAL:
            return p1.compareTo(p2) <= 0;
        case LESS_THAN:
            return p1.compareTo(p2) < 0;
        case LIKE:
            return evaluateLike(p1, p2);
        // case IN is not needed here, as this is handled in the class InImpl.
        }
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }

    private static boolean evaluateLike(PropertyValue v1, PropertyValue v2) {
        LikePattern like = new LikePattern(v2.getValue(Type.STRING));
        for (String s : v1.getValue(Type.STRINGS)) {
            if (like.matches(s)) {
                return true;
            }
        }
        return false;
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
        if (!PropertyValues.canConvert(
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
                        // no wildcards
                        operand1.restrict(f, Operator.EQUAL, v);
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

}
