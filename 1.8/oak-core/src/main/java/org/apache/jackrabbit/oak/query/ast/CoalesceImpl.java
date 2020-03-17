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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

import javax.jcr.PropertyType;
import java.util.List;
import java.util.Set;

/**
 * The function "coalesce(..)".
 */
public class CoalesceImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl operand1;
    private final DynamicOperandImpl operand2;

    public CoalesceImpl(DynamicOperandImpl operand1, DynamicOperandImpl operand2) {
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public DynamicOperandImpl getOperand2() {
        return operand2;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "coalesce(" + operand1 + ", " + operand2 + ')';
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        PropertyExistenceImpl pe = operand1.getPropertyExistence();
        return pe != null ? pe : operand2.getPropertyExistence();
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return Sets.union(
                operand1.getSelectors(),
                operand2.getSelectors()
        );
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyValue p = operand1.currentProperty();
        return p != null ? p : operand2.currentProperty();
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (operator == Operator.NOT_EQUAL && v != null) {
            // not supported
            return;
        }
        String fn = getFunction(f.getSelector());
        if (fn != null) {
            f.restrictProperty(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                    operator, v, PropertyType.STRING);
        }
    }

    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        String fn = getFunction(f.getSelector());
        f.restrictPropertyAsList(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn, list);
    }

    @Override
    public String getFunction(SelectorImpl s) {
        String f1 = operand1.getFunction(s);
        if (f1 == null) {
            return null;
        }
        String f2 = operand2.getFunction(s);
        if (f2 == null) {
            return null;
        }
        return "coalesce*" + f1 + "*" + f2;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return operand1.canRestrictSelector(s) && operand2.canRestrictSelector(s);
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new CoalesceImpl(operand1.createCopy(), operand2.createCopy());
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        String fn = getFunction(s);
        if (fn != null) {
            return new OrderEntry(
                QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                Type.STRING,
                o.isDescending() ?
                OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
        }
        return null;
    }

}
