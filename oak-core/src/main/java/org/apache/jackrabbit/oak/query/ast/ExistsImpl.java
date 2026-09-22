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

import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * The function "exists(..)". Returns true if the operand has a value, and
 * false otherwise. Always determinate (never returns null).
 */
public class ExistsImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl operand;

    public ExistsImpl(DynamicOperandImpl operand) {
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
        return "exists(" + operand + ')';
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return operand.getSelectors();
    }

    @Override
    public PropertyValue currentProperty() {
        return PropertyValues.newBoolean(operand.currentProperty() != null);
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
        String f = operand.getFunction(s);
        if (f == null) {
            return null;
        }
        return "exists*" + f;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return operand.canRestrictSelector(s);
    }

    @Override
    int getPropertyType() {
        return PropertyType.BOOLEAN;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new ExistsImpl(operand.createCopy());
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        String fn = getFunction(s);
        if (fn != null) {
            return new OrderEntry(
                QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                Type.BOOLEAN,
                o.isDescending() ?
                OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
        }
        return null;
    }

}
