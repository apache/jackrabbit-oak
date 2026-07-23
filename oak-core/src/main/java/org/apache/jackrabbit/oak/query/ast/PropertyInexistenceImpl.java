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
import java.util.Objects;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
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
    //OAK-6838
    private final boolean USE_OLD_INEXISTENCE_CHECK = Boolean.getBoolean("oak.useOldInexistenceCheck");

    private final PropertyValueImpl propertyValue;

    public PropertyInexistenceImpl(PropertyValueImpl propertyValue) {
        this.propertyValue = propertyValue;
    }

    @Override
    public boolean evaluate() {
        String propertyName = propertyValue.getPropertyName();
        boolean isRelative = propertyName.indexOf('/') >= 0;
        if (!isRelative) {
            return propertyValue.currentProperty() == null;
        }
        Tree t = propertyValue.getSelector().currentTree();
        if (t == null) {
            return true;
        }
        String pn = normalizePropertyName(propertyName);
        String relativePath = PathUtils.getParentPath(pn);
        String name = PathUtils.getName(pn);
        for (String p : PathUtils.elements(relativePath)) {
            if (t == null || !t.exists()) {
                return !USE_OLD_INEXISTENCE_CHECK;
            }
            if (p.equals("..")) {
                t = t.isRoot() ? null : t.getParent();
            } else if (p.equals(".")) {
                // same node
            } else {
                t = t.getChild(p);
            }
        }

        if (USE_OLD_INEXISTENCE_CHECK) {
            return t != null && t.exists() && !hasProperty(t, name);
        } else {
            return t == null || !t.exists() || !hasProperty(t, name);
        }
    }

    private boolean hasProperty(Tree t, String name) {
        PropertyState p = t.getProperty(name);
        if (p == null) {
            return false;
        }
        int requiredPropertyType = propertyValue.getPropertyType();
        return requiredPropertyType == PropertyType.UNDEFINED || requiredPropertyType == p.getType().tag();
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.emptySet();
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return propertyValue.getSelectors();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return propertyValue + " is null";
    }

    public void bindSelector(SourceImpl source) {
        propertyValue.bindSelector(source);
    }

    @Override
    public void restrict(FilterImpl f) {
        // we need to be careful with "property IS NULL"
        // because this might cause an index
        // to ignore the join condition "property = x"
        // for example in:
        // "select * from a left outer join b on a.x = b.y
        // where b.y is null"
        // must not result in the index to check for
        // "b.y is null", because that would alter the
        // result
        if (propertyValue.getSelector().isOuterJoinRightHandSide()) {
            return;
        }
        propertyValue.restrict(f, Operator.EQUAL, null);
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
        if (propertyValue.canRestrictSelector(s)) {
            s.restrictSelector(this);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass().getName(), propertyValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof PropertyInexistenceImpl)) {
            return false;
        }
        PropertyInexistenceImpl other = (PropertyInexistenceImpl) obj;
        return propertyValue.equals(other.propertyValue);
    }

    @Override
    public AstElement copyOf() {
        return new PropertyInexistenceImpl(propertyValue.createCopy());
    }
}
