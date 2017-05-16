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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

/**
 * Reads privilege definitions from the repository content without applying
 * any validation.
 */
class PrivilegeDefinitionReader implements PrivilegeConstants {

    private final Tree privilegesTree;

    PrivilegeDefinitionReader(@Nonnull Root root) {
        this.privilegesTree = root.getTree(PRIVILEGES_PATH);
    }

    /**
     * Read all registered privilege definitions from the content.
     *
     * @return All privilege definitions stored in the content.
     */
    @Nonnull
    Map<String, PrivilegeDefinition> readDefinitions() {
        Map<String, PrivilegeDefinition> definitions = new HashMap();
        for (Tree child : privilegesTree.getChildren()) {
            if (isPrivilegeDefinition(child)) {
                PrivilegeDefinition def = PrivilegeUtil.readDefinition(child);
                definitions.put(def.getName(), def);
            }
        }
        return definitions;
    }

    /**
     * Retrieve the privilege definition with the specified {@code privilegeName}.
     *
     * @param privilegeName The name of a registered privilege definition.
     * @return The privilege definition with the specified name or {@code null}
     *         if the name doesn't refer to a registered privilege.
     */
    @CheckForNull
    PrivilegeDefinition readDefinition(@Nonnull String privilegeName) {
        if (!privilegesTree.exists() || !privilegesTree.hasChild(privilegeName)) {
            return null;
        } else {
            Tree definitionTree = privilegesTree.getChild(privilegeName);
            return (isPrivilegeDefinition(definitionTree)) ? PrivilegeUtil.readDefinition(definitionTree) : null;
        }
    }

    private static boolean isPrivilegeDefinition(@Nonnull Tree tree) {
        return NT_REP_PRIVILEGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }
}