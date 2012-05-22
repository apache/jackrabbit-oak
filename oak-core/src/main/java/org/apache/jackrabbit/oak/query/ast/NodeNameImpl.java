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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

public class NodeNameImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public NodeNameImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "NAME(" + getSelectorName() + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getSelector(selectorName);
        if (selector == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
    }

    @Override
    public CoreValue currentValue() {
        String name = PathUtils.getName(selector.currentPath());
        return query.getValueFactory().createValue(name);
    }

    @Override
    public void apply(FilterImpl f, Operator operator, CoreValue v) {
        if (!isName(v)) {
            throw new IllegalArgumentException("Invalid name value: " + v.toString());
        }
        String path = v.getString();
        if (PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("NAME() comparison with absolute path are not allowed: " + path);
        }
        // TODO normalize paths (./name > name)
        if (PathUtils.getDepth(path) > 1) {
            throw new IllegalArgumentException("NAME() comparison with relative path are not allowed: " + path);
        }
        // TODO support NAME(..) index conditions
    }

}
