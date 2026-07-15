/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.jetbrains.annotations.NotNull;

final class ContextImpl implements Context, Constants {

    private ContextImpl(){}

    static final Context INSTANCE = new ContextImpl();

    private static final String[] NODE_NAMES = new String[] {REP_PRINCIPAL_POLICY, REP_RESTRICTIONS};
    private static final String[] PROPERTY_NAMES = new String[] {REP_PRINCIPAL_NAME, REP_EFFECTIVE_PATH, REP_PRIVILEGES};
    private static final String[] NT_NAMES = new String[] {NT_REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_ENTRY, NT_REP_RESTRICTIONS};

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
        return definesTree(parent);
    }

    @Override
    public boolean definesContextRoot(@NotNull Tree tree) {
        return Utils.isPrincipalPolicyTree(tree);
    }

    @Override
    public boolean definesTree(@NotNull Tree tree) {
        return tree.exists() && (isNodeName(tree.getName()) || isNtName(tree));
    }

    @Override
    public boolean definesLocation(@NotNull TreeLocation location) {
        PropertyState p = location.getProperty();
        Tree tree = (p == null) ? location.getTree() : location.getParent().getTree();
        if (tree != null) {
            return (p == null) ? definesTree(tree) : definesProperty(tree, p);
        } else {
            if (isItemName(location.getName())) {
                return true;
            }
            TreeLocation parent = location.getParent();
            String parentName = parent.getName();
            return REP_PRINCIPAL_POLICY.equals(parentName) || REP_RESTRICTIONS.equals(parentName);
        }
    }

    @Override
    public boolean definesInternal(@NotNull Tree tree) {
        return false;
    }

    private static boolean isNodeName(@NotNull String name) {
        for (String n : NODE_NAMES) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPropertyName(@NotNull String name) {
        for (String n : PROPERTY_NAMES) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isItemName(@NotNull String name) {
        return isNodeName(name) || isPropertyName(name);
    }

    private static boolean isNtName(@NotNull Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        for (String n : NT_NAMES) {
            if (n.equals(ntName)) {
                return true;
            }
        }
        return false;
    }
}
