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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.ValueConverter;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * A "in" comparison operation.
 */
public class InImpl extends ConstraintImpl {

    private final DynamicOperandImpl operand1;
    private final List<StaticOperandImpl> operand2;

    public InImpl(DynamicOperandImpl operand1, List<StaticOperandImpl> operand2) {
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public List<StaticOperandImpl> getOperand2() {
        return operand2;
    }

    @Override
    public ConstraintImpl simplify() {
        if (operand2.size() == 1) {
            return new ComparisonImpl(
                    operand1, Operator.EQUAL, operand2.iterator().next());
        }

        Set<StaticOperandImpl> set = newHashSet(operand2);
        if (set.size() == 1) {
            return new ComparisonImpl(
                    operand1, Operator.EQUAL, set.iterator().next());
        } else if (set.size() != operand2.size()) {
            return new InImpl(operand1, newArrayList(set));
        } else {
            return this;
        }
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
        for (StaticOperandImpl s : operand2) {
            PropertyValue p2 = s.currentValue();
            if (p2 == null) {
                // if the property doesn't exist, the result is false
                continue;
            }
            // "the value of operand2 is converted to the
            // property type of the value of operand1"
            p2 = convertValueToType(p2, p1);
            if (PropertyValues.match(p1, p2)) {
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
        StringBuilder buff = new StringBuilder();
        buff.append(operand1).append(" in(");
        int i = 0;
        for (StaticOperandImpl s : operand2) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(s);
        }
        buff.append(")");
        return buff.toString();
    }

    @Override
    public void restrict(FilterImpl f) {
        ArrayList<PropertyValue> list = new ArrayList<PropertyValue>();
        for (StaticOperandImpl s : operand2) {
            if (!ValueConverter.canConvert(
                    s.getPropertyType(),
                    operand1.getPropertyType())) {
                throw new IllegalArgumentException(
                        "Unsupported conversion from property type " + 
                                PropertyType.nameFromValue(s.getPropertyType()) + 
                                " to property type " +
                                PropertyType.nameFromValue(operand1.getPropertyType()));
            }
            list.add(s.currentValue());
        }
        if (list != null) {
            operand1.restrictList(f, list);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        for (StaticOperandImpl op : operand2) {
            if (op.currentValue() == null) {
                // one unknown value means it is not pushed down
                return;
            }
        }
        if (operand1.canRestrictSelector(s)) {
            s.restrictSelector(this);
        }
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof InImpl) {
            return operand1.equals(((InImpl) that).operand1)
                    && newHashSet(operand2).equals(newHashSet(((InImpl) that).operand2));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return operand1.hashCode();
    }

    @Override
    public AstElement copyOf() {
        return new InImpl(operand1.createCopy(), operand2);
    }
}
