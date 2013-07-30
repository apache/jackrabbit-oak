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

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * PrivilegeUtil... TODO
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
     * @param definitionTree
     * @return
     */
    @Nonnull
    public static PrivilegeDefinition readDefinition(@Nonnull Tree definitionTree) {
        String name = definitionTree.getName();
        boolean isAbstract = TreeUtil.getBoolean(definitionTree, REP_IS_ABSTRACT);
        String[] declAggrNames = TreeUtil.getStrings(definitionTree, REP_AGGREGATES);

        return new ImmutablePrivilegeDefinition(name, isAbstract, declAggrNames);
    }
}