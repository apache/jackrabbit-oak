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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * The "ischildnode(...)" condition.
 */
public class ChildNodeImpl extends ConstraintImpl {

    private final String selectorName;
    private final String parentPath;
    private SelectorImpl selector;

    public ChildNodeImpl(String selectorName, String parentPath) {
        this.selectorName = selectorName;
        this.parentPath = parentPath;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "ischildnode(" + quote(selectorName) + ", " + quote(parentPath) + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
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
    public boolean evaluate() {
        String p = selector.currentPath();
        String local = getLocalPath(p);
        if (local == null) {
            // not a local path
            return false;
        }
        // the parent of the root is the root,
        // so we need to special case this
        if (PathUtils.denotesRoot(local)) {
            return false;
        }
        String path = normalizePath(parentPath);
        return PathUtils.getParentPath(local).equals(path);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (selector.equals(f.getSelector())) {
            String path = normalizePath(parentPath);
            f.restrictPath(path, Filter.PathRestriction.DIRECT_CHILDREN);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (s.equals(selector)) {
            s.restrictSelector(this);
        }
    }

    @Override
    public AstElement copyOf() {
        return new ChildNodeImpl(selectorName, parentPath);
    }
}