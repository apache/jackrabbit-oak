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
package org.apache.jackrabbit.oak.spi.security.privilege;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

/**
 * Privilege management related utility methods.
 */
public final class PrivilegeUtil implements PrivilegeConstants {

    private PrivilegeUtil() {}

    /**
     * Returns the root tree for all privilege definitions stored in the content
     * repository.
     *
     * @return The privileges root.
     */
    @Nonnull
    public static Tree getPrivilegesTree(Root root) {
        return root.getTree(PRIVILEGES_PATH);
    }

    /**
     * Reads the privilege definition stored in the specified definition tree.
     * Note, that this utility does not check the existence nor the node type
     * of the specified tree.
     *
     * @param definitionTree An existing tree storing a privilege definition.
     * @return A new instance of {@code PrivilegeDefinition}.
     */
    @Nonnull
    public static PrivilegeDefinition readDefinition(@Nonnull Tree definitionTree) {
        String name = definitionTree.getName();
        boolean isAbstract = TreeUtil.getBoolean(definitionTree, REP_IS_ABSTRACT);
        Iterable<String> declAggrNames = null;
        PropertyState property = definitionTree.getProperty(REP_AGGREGATES);
        if (property != null) {
            declAggrNames = property.getValue(Type.NAMES);
        }
        return new ImmutablePrivilegeDefinition(name, isAbstract, declAggrNames);
    }
}