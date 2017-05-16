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
package org.apache.jackrabbit.oak.security.authorization;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

final class AuthorizationContext implements Context, AccessControlConstants, PermissionConstants {

    private static final String[] NODE_NAMES = POLICY_NODE_NAMES.toArray(new String[POLICY_NODE_NAMES.size()]);
    private static final String[] PROPERTY_NAMES = ACE_PROPERTY_NAMES.toArray(new String[ACE_PROPERTY_NAMES.size()]);
    private static final String[] NT_NAMES = Iterables.toArray(Iterables.concat(AC_NODETYPE_NAMES, PERMISSION_NODETYPE_NAMES), String.class);

    private static final Context INSTANCE = new AuthorizationContext();

    private AuthorizationContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
        return definesTree(parent);
    }

    @Override
    public boolean definesContextRoot(@Nonnull Tree tree) {
        String name = tree.getName();
        if (isNodeName(name)) {
            return NT_REP_ACL.equals(TreeUtil.getPrimaryTypeName(tree));
        } else {
            return REP_PERMISSION_STORE.equals(name);
        }
    }

    @Override
    public boolean definesTree(@Nonnull Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        return ntName != null && isNtName(ntName);
    }

    @Override
    public boolean definesLocation(@Nonnull TreeLocation location) {
        PropertyState p = location.getProperty();
        Tree tree = (p == null) ? location.getTree() : location.getParent().getTree();
        if (tree != null) {
            return (p == null) ? definesTree(tree) : definesProperty(tree, p);
        } else {
            return isItemName(location.getName()) || location.getPath().startsWith(PERMISSIONS_STORE_PATH);
        }
    }

    @Override
    public boolean definesInternal(@Nonnull Tree tree) {
        return PermissionConstants.REP_PERMISSION_STORE.equals(tree.getName());
    }

    private static boolean isNodeName(@Nonnull String name) {
        for (String n : NODE_NAMES) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isItemName(@Nonnull String name) {
        if (isNodeName(name)) {
            return true;
        }
        for (String n : PROPERTY_NAMES) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNtName(@Nonnull String name) {
        for (String n : NT_NAMES) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }
}
