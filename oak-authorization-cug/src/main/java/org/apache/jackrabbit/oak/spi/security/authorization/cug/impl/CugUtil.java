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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for this CUG implementation package.
 */
final class CugUtil implements CugConstants {

    private static final Logger log = LoggerFactory.getLogger(CugUtil.class);

    private CugUtil(){}

    static boolean hasCug(@NotNull Tree tree) {
        return tree.exists() && tree.hasChild(REP_CUG_POLICY);
    }

    static boolean hasCug(@Nullable NodeState state) {
        return state != null && state.hasChildNode(REP_CUG_POLICY);
    }

    static boolean hasCug(@Nullable NodeBuilder builder) {
        return builder != null && builder.hasChildNode(REP_CUG_POLICY);
    }

    @Nullable
    static Tree getCug(@NotNull Tree tree) {
        Tree cugTree = (CugUtil.hasCug(tree)) ? tree.getChild(REP_CUG_POLICY) : null;
        if (cugTree != null && NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(cugTree))) {
            return cugTree;
        } else {
            return null;
        }
    }

    static boolean definesCug(@NotNull Tree tree) {
        return tree.exists() && REP_CUG_POLICY.equals(tree.getName()) && NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    static boolean definesCug(@NotNull String name, @NotNull NodeState state) {
        return REP_CUG_POLICY.equals(name) && NT_REP_CUG_POLICY.equals(NodeStateUtils.getPrimaryTypeName(state));
    }

    static boolean definesCug(@NotNull Tree tree, @NotNull PropertyState property) {
        return REP_PRINCIPAL_NAMES.equals(property.getName()) && definesCug(tree);
    }

    static boolean hasNestedCug(@NotNull Tree cugTree) {
        return cugTree.hasProperty(CugConstants.HIDDEN_NESTED_CUGS);
    }

    static boolean isSupportedPath(@Nullable String oakPath, @NotNull Set<String> supportedPaths) {
        if (oakPath == null) {
            return false;
        } else {
            for (String supportedPath : supportedPaths) {
                if (Text.isDescendantOrEqual(supportedPath, oakPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    static Set<String> getSupportedPaths(@NotNull ConfigurationParameters params, @NotNull MountInfoProvider mountInfoProvider) {
        Set<String> supportedPaths = params.getConfigValue(CugConstants.PARAM_CUG_SUPPORTED_PATHS, Set.of());
        if (!supportedPaths.isEmpty() && mountInfoProvider.hasNonDefaultMounts()) {
            for (Mount mount : mountInfoProvider.getNonDefaultMounts()) {
                for (String path : supportedPaths) {
                    if (mount.isUnder(path)) {
                        log.error("Configured supported CUG path '{}' includes node store mount '{}'.", path, mount.getName());
                        throw new IllegalStateException();
                    } else if (mount.isMounted(path)) {
                        log.error("Configured supported CUG path '{}' is part of node store mount '{}'.", path, mount.getName());
                        throw new IllegalStateException();
                    }
                }
            }
        }
        return supportedPaths;
    }

    static int getImportBehavior(ConfigurationParameters config) {
        String importBehaviorStr = config.getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT);
        return ImportBehavior.valueFromString(importBehaviorStr);
    }
}
