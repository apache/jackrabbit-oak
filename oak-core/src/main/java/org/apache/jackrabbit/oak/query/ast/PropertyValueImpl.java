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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A property expression.
 */
public class PropertyValueImpl extends DynamicOperandImpl {

    private final String selectorName;
    private final String propertyName;
    private SelectorImpl selector;

    public PropertyValueImpl(String selectorName, String propertyName) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        // TODO quote property names?
        return getSelectorName() + '.' + propertyName;
    }

    @Override
    public PropertyState currentProperty() {
        if (propertyName.indexOf('/') < 0) {
            return selector.currentProperty(propertyName);
        }
        // TODO really support relative properties?
        Tree tree = getTree(selector.currentPath());
        for (String p : PathUtils.elements(PathUtils.getParentPath(propertyName))) {
            if (tree == null) {
                return null;
            }
            if (!tree.hasChild(p)) {
                return null;
            }
            tree = tree.getChild(p);
        }
        if (tree == null) {
            return null;
        }
        String name = PathUtils.getName(propertyName);
        if (!tree.hasProperty(name)) {
            return null;
        }
        return tree.getProperty(name);
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getSelector(selectorName);
        if (selector == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
    }

    @Override
    public void apply(FilterImpl f, Operator operator, CoreValue v) {
        if (f.getSelector() == selector) {
            if (operator == Operator.NOT_EQUAL && v != null) {
                // not supported
                return;
            }
            f.restrictProperty(propertyName, operator, v);
        }
    }

}
