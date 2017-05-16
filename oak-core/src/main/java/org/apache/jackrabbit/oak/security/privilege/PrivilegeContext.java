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
package org.apache.jackrabbit.oak.security.privilege;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

final class PrivilegeContext implements Context, PrivilegeConstants {

    private static final Context INSTANCE = new PrivilegeContext();

    private PrivilegeContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
        return PRIVILEGE_PROPERTY_NAMES.contains(property.getName()) && definesTree(parent);
    }

    @Override
    public boolean definesContextRoot(@Nonnull Tree tree) {
        return REP_PRIVILEGES.equals(tree.getName()) && NT_REP_PRIVILEGES.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    @Override
    public boolean definesTree(@Nonnull Tree tree) {
        return PRIVILEGE_NODETYPE_NAMES.contains(TreeUtil.getPrimaryTypeName(tree));
    }

    @Override
    public boolean definesLocation(@Nonnull TreeLocation location) {
        return Text.isDescendantOrEqual(PRIVILEGES_PATH, location.getPath());
    }

    @Override
    public boolean definesInternal(@Nonnull Tree tree) {
        return false;
    }

}
