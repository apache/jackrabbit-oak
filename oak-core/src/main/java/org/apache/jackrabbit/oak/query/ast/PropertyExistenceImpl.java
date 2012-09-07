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
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A condition to check if the property exists ("is not null").
 */
public class PropertyExistenceImpl extends ConstraintImpl {

    private final String selectorName;
    private final String propertyName;
    private SelectorImpl selector;

    public PropertyExistenceImpl(String selectorName, String propertyName) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    public boolean evaluate() {
        PropertyState p = selector.currentProperty(propertyName);
        return p != null;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        // TODO quote property names?
        return getSelectorName() + '.' + propertyName + " is not null";
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector() == selector) {
            f.restrictProperty(propertyName, Operator.NOT_EQUAL, (CoreValue) null);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s == selector) {
            s.restrictSelector(this);
        }
    }

}
