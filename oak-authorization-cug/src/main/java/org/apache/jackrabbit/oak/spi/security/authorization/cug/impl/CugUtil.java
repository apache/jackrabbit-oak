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

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * Utility methods for this CUG implementation package.
 */
final class CugUtil implements CugConstants {

    private CugUtil(){}

    public static boolean definesCug(@Nonnull Tree tree) {
        return tree.exists() && REP_CUG_POLICY.equals(tree.getName()) && NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    public static boolean definesCug(@Nonnull Tree tree, @Nonnull PropertyState property) {
        return REP_PRINCIPAL_NAMES.equals(property.getName()) && definesCug(tree);
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

    public static boolean registerCugNodeTypes(@Nonnull final Root root) {
        try {
            ReadOnlyNodeTypeManager ntMgr = new ReadOnlyNodeTypeManager() {
                @Override
                protected Tree getTypes() {
                    return root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
                }
            };
            if (!ntMgr.hasNodeType(NT_REP_CUG_POLICY)) {
                InputStream stream = CugConfiguration.class.getResourceAsStream("cug_nodetypes.cnd");
                try {
                    NodeTypeRegistry.register(root, stream, "cug node types");
                    return true;
                } finally {
                    stream.close();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read cug node types", e);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Unable to read cug node types", e);
        }
        return false;
    }
}