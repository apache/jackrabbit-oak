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
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A condition to check if the property does not exist ("is null").
 * <p>
 * For Jackrabbit 2.x compatibility: if the property is relative (as in
 * "child/propertyName"), then this requires that the given child node exists.
 */
public class PropertyInexistenceImpl extends ConstraintImpl {

    private final String selectorName;
    private final String propertyName;
    private SelectorImpl selector;

    public PropertyInexistenceImpl(SelectorImpl selector, String selectorName, String propertyName) {
        this.selector = selector;
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }
    
    public PropertyInexistenceImpl(String selectorName, String propertyName) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }

    @Override
    public boolean evaluate() {
        boolean isRelative = propertyName.indexOf('/') >= 0;
        if (!isRelative) {
            return selector.currentProperty(propertyName) == null;
        }
        Tree t = selector.currentTree();
        if (t == null) {
            return true;
        }
        String pn = normalizePropertyName(propertyName);
        String relativePath = PathUtils.getParentPath(pn);
        String name = PathUtils.getName(pn);
        for (String p : PathUtils.elements(relativePath)) {
            if (t == null || !t.exists()) {
                return false;
            }
            if (p.equals("..")) {
                t = t.isRoot() ? null : t.getParent();
            } else if (p.equals(".")) {
                // same node
            } else {
                t = t.getChild(p);
            }
        }
        return t != null && t.exists() && !t.hasProperty(name);
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }
    
    @Override 
    public Map<DynamicOperandImpl, Set<StaticOperandImpl>> getInMap() {
        return Collections.emptyMap();
    }  

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return quote(selectorName) + '.' + quote(propertyName) + " is null";
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        // we don't support covering indexes, 
        // so there is no optimization anyway, and
        // we need to be careful with "property IS NULL"
        // because this might cause an index
        // to ignore the join condition "property = x"
        // for example in:
        // "select * from a left outer join b on a.x = b.y
        // where b.y is null"
        // must not result in the index to check for
        // "b.y is null", because that would alter the
        // result
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s.isOuterJoinRightHandSide()) {
            // we need to be careful with "property IS NULL"
            // because this might cause an index
            // to ignore the join condition "property = x"
            // for example in:
            // "select * from a left outer join b on a.x = b.y
            // where b.y is null"
            // must not check for "b.y is null" too early, 
            // because that would alter the result
            return;
        }
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
        PropertyInexistenceImpl other = (PropertyInexistenceImpl) obj;
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

}