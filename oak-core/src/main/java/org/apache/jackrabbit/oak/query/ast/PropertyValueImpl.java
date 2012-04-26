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

import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

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
    public CoreValue currentValue() {
        if (propertyName.indexOf('/') < 0) {
            return selector.currentProperty(propertyName);
        }
        // TODO really support relative properties?
        NodeImpl n = selector.currentNode();
        String[] elements = PathUtils.split(propertyName);
        for (int i = 0; i < elements.length - 1; i++) {
            String p = elements[i];
            if (!n.exists(p)) {
                return null;
            }
            n = n.getNode(p);
        }
        String name = PathUtils.getName(propertyName);
        if (!n.hasProperty(name)) {
            return null;
        }
        // TODO data type mapping
        String value = n.getProperty(name);
        value = JsopTokenizer.decodeQuoted(value);
        return query.getValueFactory().createValue(value);

    }

    public void bindSelector(SourceImpl source) {
        selector = source.getSelector(selectorName);
        if (selector == null) {
            throw new RuntimeException("Unknown selector: " + selectorName);
        }
    }

    @Override
    public void apply(FilterImpl f, Operator operator, CoreValue v) {
        if (f.getSelector() == selector) {
            f.restrictProperty(propertyName, operator, v);
        }
    }

}
