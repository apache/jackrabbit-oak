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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

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
    public PropertyState currentProperty() {
        PropertyState p = selector.currentProperty(Query.JCR_SCORE);
        if (p == null) {
            // TODO if score() is not supported by the index, use the value 0.0?
            CoreValue v = query.getValueFactory().createValue(0.0);
            p = PropertyStates.createProperty(Query.JCR_SCORE, v);
        }
        return p;
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, CoreValue v) {
        if (f.getSelector() == selector) {
            if (operator == Operator.NOT_EQUAL && v != null) {
                // not supported
                return;
            }
            f.restrictProperty(Query.JCR_SCORE, operator, v);
        }
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s == selector;
    }

}
