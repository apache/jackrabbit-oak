/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.util.AsciiNormalizer;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * String normalization using ASCII folding and lower casing.
 */
public class NormalizeImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl operand;

    public NormalizeImpl(DynamicOperandImpl operand) {
        this.operand = operand;
    }

    public DynamicOperandImpl getOperand() {
        return operand;
    }

    @Override
    boolean accept(AstVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "normalize(" + operand + ')';
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return operand.getPropertyExistence();
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return operand.getSelectors();
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyValue value = operand.currentProperty();
        if (value == null) {
            return null;
        }
        if (value.getType().isArray()) {
            return PropertyValues.newString(IterableUtils.transform(
                    value.getValue(STRINGS), AsciiNormalizer::normalize));
        }
        return PropertyValues.newString(AsciiNormalizer.normalize(value.getValue(STRING)));
    }

    @Override
    public void restrict(FilterImpl filter, Operator operator, PropertyValue value) {
        operand.restrict(filter, Operator.NOT_EQUAL, null);
        if (operator == Operator.NOT_EQUAL && value != null) {
            return;
        }
        String function = getFunction(filter.getSelector());
        if (function != null) {
            filter.restrictProperty(QueryConstants.FUNCTION_RESTRICTION_PREFIX + function,
                    operator, value, PropertyType.STRING);
        }
    }

    @Override
    public void restrictList(FilterImpl filter, List<PropertyValue> list) {
        operand.restrict(filter, Operator.NOT_EQUAL, null);
        String function = getFunction(filter.getSelector());
        if (function != null) {
            filter.restrictPropertyAsList(QueryConstants.FUNCTION_RESTRICTION_PREFIX + function, list);
        }
    }

    @Override
    public String getFunction(SelectorImpl selector) {
        String function = operand.getFunction(selector);
        return function == null ? null : "normalize*" + function;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl selector) {
        return operand.canRestrictSelector(selector);
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new NormalizeImpl(operand.createCopy());
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl selector, OrderingImpl ordering) {
        String function = getFunction(selector);
        if (function == null) {
            return null;
        }
        return new OrderEntry(QueryConstants.FUNCTION_RESTRICTION_PREFIX + function, Type.STRING,
                ordering.isDescending() ? OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
    }
}
