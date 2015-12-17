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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * Utility methods for this CUG implementation package.
 */
final class CugUtil implements CugConstants {

    private CugUtil(){}

    public static boolean hasCug(@Nonnull Tree tree) {
        return tree.exists() && tree.hasChild(REP_CUG_POLICY);
    }

    public static boolean hasCug(@CheckForNull NodeState state) {
        return state != null && state.hasChildNode(REP_CUG_POLICY);
    }

    public static boolean hasCug(@CheckForNull NodeBuilder builder) {
        return builder != null && builder.hasChildNode(REP_CUG_POLICY);
    }

    @CheckForNull
    public static Tree getCug(@Nonnull Tree tree) {
        Tree cugTree = (CugUtil.hasCug(tree)) ? tree.getChild(REP_CUG_POLICY) : null;
        if (cugTree != null && NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(cugTree))) {
            return cugTree;
        } else {
            return null;
        }
    }

    public static boolean definesCug(@Nonnull Tree tree) {
        return tree.exists() && REP_CUG_POLICY.equals(tree.getName()) && NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    public static boolean definesCug(@Nonnull String name, @Nonnull NodeState state) {
        return REP_CUG_POLICY.equals(name) && NT_REP_CUG_POLICY.equals(NodeStateUtils.getPrimaryTypeName(state));
    }

    public static boolean definesCug(@Nonnull Tree tree, @Nonnull PropertyState property) {
        return REP_PRINCIPAL_NAMES.equals(property.getName()) && definesCug(tree);
    }

    public static boolean hasNestedCug(@Nonnull Tree cugTree) {
        return cugTree.hasProperty(CugConstants.HIDDEN_NESTED_CUGS);
    }

    public static boolean isSupportedPath(@Nullable String oakPath, @Nonnull ConfigurationParameters config) {
        if (oakPath == null) {
            return false;
        } else {
            for (String supportedPath : config.getConfigValue(CugConfiguration.PARAM_CUG_SUPPORTED_PATHS, new String[0])) {
                if (Text.isDescendantOrEqual(supportedPath, oakPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static int getImportBehavior(ConfigurationParameters config) {
        String importBehaviorStr = config.getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT);
        return ImportBehavior.valueFromString(importBehaviorStr);
    }
}