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
import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A condition to check if the property exists ("is not null").
 */
public class PropertyExistenceImpl extends ConstraintImpl {

    private final String selectorName;
    private final String propertyName;
    private SelectorImpl selector;

    public PropertyExistenceImpl(SelectorImpl selector, String selectorName, String propertyName) {
        this.selector = selector;
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }
    
    public PropertyExistenceImpl(String selectorName, String propertyName) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }

    @Override
    public boolean evaluate() {
        return selector.currentProperty(propertyName) != null;
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.singleton(this);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return quote(selectorName) + '.' + quote(propertyName) + " is not null";
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector().equals(selector)) {
            String pn = normalizePropertyName(propertyName);
            f.restrictProperty(pn, Operator.NOT_EQUAL, null);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s.equals(selector)) {
            s.restrictSelector(this);
        }
    }
    
    @Override
    public int hashCode() {
        String pn = normalizePropertyName(propertyName);
        return ((selectorName == null) ? 0 : selectorName.hashCode()) * 31 +
                ((pn == null) ? 0 : pn.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PropertyExistenceImpl other = (PropertyExistenceImpl) obj;
        if (!equalsStrings(selectorName, other.selectorName)) {
            return false;
        }
        String pn = normalizePropertyName(propertyName);
        String pn2 = normalizePropertyName(other.propertyName);
        return equalsStrings(pn, pn2);
    }
    
    private static boolean equalsStrings(String a, String b) {
        return a == null || b == null ? a == b : a.equals(b);
    }

    @Override
    public AstElement copyOf() {
        return new PropertyExistenceImpl(selectorName, propertyName);
    }
}
