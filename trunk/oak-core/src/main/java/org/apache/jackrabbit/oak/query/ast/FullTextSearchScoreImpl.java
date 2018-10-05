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
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * A fulltext search score expression.
 */
public class FullTextSearchScoreImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public FullTextSearchScoreImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "score(" + quote(selectorName) + ')';
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
        PropertyValue p = selector.currentOakProperty(QueryConstants.JCR_SCORE);
        if (p == null) {
            // TODO if score() is not supported by the index, use the value 0.0?
            return PropertyValues.newDouble(0.0);
        }
        return p;
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (f.getSelector().equals(selector)) {
            if (operator == Operator.NOT_EQUAL && v != null) {
                // not supported
                return;
            }
            f.restrictProperty(QueryConstants.JCR_SCORE, operator, v);
        }
    }
    
    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        // optimizations of the type "jcr:score() in(a, b, c)" are not supported
    }

    @Override
    public String getFunction(SelectorImpl s) {
        // optimizations of the type "upper(jcr:score()) = '1'" are not supported
        return null;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s.equals(selector);
    }
    
    @Override
    int getPropertyType() {
        return PropertyType.DOUBLE;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new FullTextSearchScoreImpl(selectorName);
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        return null;
    }

}
