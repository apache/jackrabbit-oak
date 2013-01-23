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
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * The "ischildnode(...)" join condition.
 */
public class ChildNodeJoinConditionImpl extends JoinConditionImpl {

    private final String childSelectorName;
    private final String parentSelectorName;
    private SelectorImpl childSelector;
    private SelectorImpl parentSelector;

    public ChildNodeJoinConditionImpl(String childSelectorName, String parentSelectorName) {
        this.childSelectorName = childSelectorName;
        this.parentSelectorName = parentSelectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "ischildnode(" + quote(childSelectorName) + 
                ", " + quote(parentSelectorName) + ')';
    }

    public void bindSelector(SourceImpl source) {
        parentSelector = source.getExistingSelector(parentSelectorName);
        childSelector = source.getExistingSelector(childSelectorName);
    }

    @Override
    public boolean evaluate() {
        String p = parentSelector.currentPath();
        String c = childSelector.currentPath();
        // the parent of the root is the root,
        // so we need to special case this
        return !PathUtils.denotesRoot(c) && PathUtils.getParentPath(c).equals(p);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector() == parentSelector) {
            String c = childSelector.currentPath();
            if (c == null && f.isPreparing() && childSelector.isPrepared()) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                c = KNOWN_PATH;
            }
            if (c != null) {
                f.restrictPath(PathUtils.getParentPath(c), Filter.PathRestriction.EXACT);
            }
        }
        if (f.getSelector() == childSelector) {
            String p = parentSelector.currentPath();
            if (p == null && f.isPreparing() && parentSelector.isPrepared()) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                p = KNOWN_PATH;
            }
            if (p != null) {
                f.restrictPath(p, Filter.PathRestriction.DIRECT_CHILDREN);
            }
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        // nothing to do
    }

}