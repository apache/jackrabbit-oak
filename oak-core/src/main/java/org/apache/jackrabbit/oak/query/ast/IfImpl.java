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
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * The function "if(condition, trueValue, falseValue)".
 */
public class IfImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl condition;
    private final DynamicOperandImpl trueValue;
    private final DynamicOperandImpl falseValue;

    public IfImpl(DynamicOperandImpl condition, DynamicOperandImpl trueValue, DynamicOperandImpl falseValue) {
        this.condition = condition;
        this.trueValue = trueValue;
        this.falseValue = falseValue;
    }

    public DynamicOperandImpl getCondition() {
        return condition;
    }

    public DynamicOperandImpl getTrueValue() {
        return trueValue;
    }

    public DynamicOperandImpl getFalseValue() {
        return falseValue;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "if(" + condition + ", " + trueValue + ", " + falseValue + ')';
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return SetUtils.union(
                SetUtils.union(condition.getSelectors(), trueValue.getSelectors()),
                falseValue.getSelectors());
    }

    @Override
    public PropertyValue currentProperty() {
        boolean truthy = FunctionOperatorUtils.isTruthy(condition.currentProperty());
        return truthy ? trueValue.currentProperty() : falseValue.currentProperty();
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
        String fc = condition.getFunction(s);
        if (fc == null) {
            return null;
        }
        String ft = trueValue.getFunction(s);
        if (ft == null) {
            return null;
        }
        String ff = falseValue.getFunction(s);
        if (ff == null) {
            return null;
        }
        return "if*" + fc + "*" + ft + "*" + ff;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return condition.canRestrictSelector(s) && trueValue.canRestrictSelector(s)
                && falseValue.canRestrictSelector(s);
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new IfImpl(condition.createCopy(), trueValue.createCopy(), falseValue.createCopy());
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
