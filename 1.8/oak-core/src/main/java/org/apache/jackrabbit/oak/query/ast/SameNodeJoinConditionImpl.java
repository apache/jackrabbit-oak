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

import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * The "issamenode(...)" join condition.
 */
public class SameNodeJoinConditionImpl extends JoinConditionImpl {

    private final String selector1Name;
    private final String selector2Name;
    private final String selector2Path;
    private SelectorImpl selector1;
    private SelectorImpl selector2;

    public SameNodeJoinConditionImpl(String selector1Name, String selector2Name,
            String selector2Path) {
        this.selector1Name = selector1Name;
        this.selector2Name = selector2Name;
        this.selector2Path = selector2Path;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("issamenode(");
        builder.append(quote(selector1Name));
        builder.append(", ");
        builder.append(quote(selector2Name));
        if (selector2Path != null) {
            builder.append(", ");
            builder.append(quote(selector2Path));
        }
        builder.append(')');
        return builder.toString();
    }

    public void bindSelector(SourceImpl source) {
        selector1 = source.getExistingSelector(selector1Name);
        selector2 = source.getExistingSelector(selector2Name);
    }

    @Override
    public boolean evaluate() {
        String p1 = selector1.currentPath();
        String p2 = selector2.currentPath();
        if (selector2Path.equals(".")) {
            return p1.equals(p2);
        }
        String pn = normalizePath(selector2Path);
        String p = PathUtils.concat(p2, pn);
        return p.equals(p1);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector().equals(selector1)) {
            String p2 = selector2.currentPath();
            if (p2 == null && f.isPreparing() && f.isPrepared(selector2)) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                f.restrictPath(KNOWN_PATH, Filter.PathRestriction.EXACT);
            } else if (p2 != null) {
                if (selector2Path.equals(".")) {
                    f.restrictPath(p2, Filter.PathRestriction.EXACT);
                } else {
                    String pn = normalizePath(selector2Path);
                    String p = PathUtils.concat(p2, pn);
                    f.restrictPath(p, Filter.PathRestriction.EXACT);
                }
            }
        }
        if (f.getSelector().equals(selector2)) {
            String p1 = selector1.currentPath();
            if (p1 == null && f.isPreparing() && f.isPrepared(selector1)) {
                // during the prepare phase, if the selector is already
                // prepared, then we would know the value
                f.restrictPath(KNOWN_PATH, Filter.PathRestriction.EXACT);
            } else if (p1 != null) {
                if (selector2Path.equals(".")) {
                    f.restrictPath(p1, Filter.PathRestriction.EXACT);
                }
            }
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        // nothing to do
    }
    
    @Override
    public boolean isParent(SourceImpl source) {
        return false;
    }
    
    @Override
    public boolean canEvaluate(Set<SourceImpl> available) {
        return available.contains(selector1) && available.contains(selector2);
    }

}
