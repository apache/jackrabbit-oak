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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * CugUtil... TODO
 */
final class CugUtil {

    private CugUtil(){}


    public static boolean definesCug(@Nonnull Tree tree) {
        return tree.exists() && CugConstants.NT_REP_CUG_POLICY.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    public static boolean definesCug(@Nonnull Tree tree, @Nonnull PropertyState property) {
        return CugConstants.REP_PRINCIPAL_NAMES.equals(property.getName()) && definesCug(tree);
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