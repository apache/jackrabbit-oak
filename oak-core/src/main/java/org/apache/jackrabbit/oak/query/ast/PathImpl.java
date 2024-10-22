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
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * The function "path(..)".
 */
public class PathImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public PathImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "path(" + quote(selectorName) + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    @Override
    public PropertyValue currentProperty() {
        String path = selector.currentPath();
        if (path == null) {
            return null;
        }
        return PropertyValues.newString(path);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (v == null) {
            return;
        }
        if (operator == Operator.NOT_EQUAL) {
            // not supported
            return;
        }
        if (f.getSelector().equals(selector)) {
            String path = v.getValue(Type.STRING);
            String fn = getFunction(f.getSelector());
            f.restrictProperty(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                    operator, PropertyValues.newString(path));
        }
    }

    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        if (!f.getQueryLimits().getOptimizeInRestrictionsForFunctions()) {
            return;
        }
        String fn = getFunction(f.getSelector());
        if (fn != null) {
            f.restrictPropertyAsList(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn, list);
        }
    }

    @Override
    public String getFunction(SelectorImpl s) {
        if (!s.equals(selector)) {
            return null;
        }
        return "@" + QueryConstants.RESTRICTION_PATH;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s.equals(selector);
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new PathImpl(selectorName);
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        if (!s.equals(selector)) {
            // ordered by a different selector
            return null;
        }
        return new OrderEntry(
                QueryConstants.FUNCTION_RESTRICTION_PREFIX + getFunction(s),
            Type.STRING,
            o.isDescending() ?
            OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
    }

}
