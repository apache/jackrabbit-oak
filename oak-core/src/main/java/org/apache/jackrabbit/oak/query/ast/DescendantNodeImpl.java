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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.Filter;

/**
 * The "isdescendantnode(...)" condition.
 */
public class DescendantNodeImpl extends ConstraintImpl {

    private final String selectorName;
    private final String ancestorPath;
    private SelectorImpl selector;

    public DescendantNodeImpl(String selectorName, String ancestorPath) {
        this.selectorName = selectorName;
        this.ancestorPath = ancestorPath;
    }

    public String getSelectorName() {
        return selectorName;
    }

    public String getAncestorPath() {
        return ancestorPath;
    }

    @Override
    public boolean evaluate() {
        String p = selector.currentPath();
        String path = getAbsolutePath(ancestorPath);
        if (p == null || path == null) {
            return false;
        }
        return PathUtils.isAncestor(path, p);
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "isdescendantnode(" + getSelectorName() + ", " + quotePath(ancestorPath) + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector() == selector) {
            String path = getAbsolutePath(ancestorPath);
            f.restrictPath(path, Filter.PathRestriction.ALL_CHILDREN);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s == selector) {
            s.restrictSelector(this);
        }
    }

}
