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

import java.util.ArrayList;
import java.util.Locale;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.CoreValues;
import org.apache.jackrabbit.oak.plugins.memory.MultiPropertyState;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A property expression.
 */
public class PropertyValueImpl extends DynamicOperandImpl {

    private final String selectorName;
    private final String propertyName;
    private final int propertyType;
    private SelectorImpl selector;

    public PropertyValueImpl(String selectorName, String propertyName) {
        this(selectorName, propertyName, null);
    }

    public PropertyValueImpl(String selectorName, String propertyName, String propertyType) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.propertyType = propertyType == null ?
                PropertyType.UNDEFINED :
                SQL2Parser.getPropertyTypeFromName(propertyType);
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
        String s = quote(selectorName) + '.' + quote(propertyName);
        if (propertyType != PropertyType.UNDEFINED) {
            s = "property(" + s + ", '" +
                    PropertyType.nameFromValue(propertyType).toLowerCase(Locale.ENGLISH) +
                    "')";
        }
        return s;
    }

    @Override
    public PropertyState currentProperty() {
        boolean relative = propertyName.indexOf('/') >= 0;
        boolean asterisk = propertyName.equals("*");
        if (!relative && !asterisk) {
            PropertyState p = selector.currentProperty(propertyName);
            return matchesPropertyType(p) ? p : null;
        }
        Tree tree = getTree(selector.currentPath());
        if (tree == null) {
            return null;
        }
        if (relative) {
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
        }
        if (!asterisk) {
            String name = PathUtils.getName(propertyName);
            if (!tree.hasProperty(name)) {
                return null;
            }
            PropertyState p = tree.getProperty(name);
            return matchesPropertyType(p) ? p : null;
        }
        // asterisk - create a multi-value property
        // warning: the returned property state may have a mixed type
        // (not all values may have the same type)
        ArrayList<CoreValue> values = new ArrayList<CoreValue>();
        for (PropertyState p : tree.getProperties()) {
            if (matchesPropertyType(p)) {
                if (p.isArray()) {
                    values.addAll(CoreValues.getValues(p));
                } else {
                    values.add(CoreValues.getValue(p));
                }
            }
        }
        MultiPropertyState mv = new MultiPropertyState("*", values);
        return mv;
    }

    private boolean matchesPropertyType(PropertyState state) {
        if (state == null) {
            return false;
        }
        if (propertyType == PropertyType.UNDEFINED) {
            return true;
        }
        return state.getType().tag() == propertyType;
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
            f.restrictProperty(propertyName, operator, v);
            if (propertyType != PropertyType.UNDEFINED) {
                f.restrictPropertyType(propertyName, operator, propertyType);
            }
        }
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s == selector;
    }

}
