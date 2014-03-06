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
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.Text;

/**
 * Utility methods to evaluate permissions.
 */
public final class PermissionUtil implements PermissionConstants {

    private PermissionUtil() {}

    @CheckForNull
    public static String getParentPathOrNull(@Nonnull final String path) {
        if (path.length() <= 1) {
            return null;
        } else {
            int idx = path.lastIndexOf('/');
            if (idx == 0) {
                return "/";
            } else {
                return path.substring(0, idx);
            }
        }
    }

    @Nonnull
    public static String getEntryName(@Nullable String accessControlledPath) {
        String path = Strings.nullToEmpty(accessControlledPath);
        return String.valueOf(path.hashCode());
    }

    public static boolean checkACLPath(@Nonnull NodeBuilder node, @Nonnull String path) {
        PropertyState property = node.getProperty(REP_ACCESS_CONTROLLED_PATH);
        return property != null && path.equals(property.getValue(Type.STRING));
    }

    public static boolean checkACLPath(@Nonnull Tree node, @Nonnull String path) {
        PropertyState property = node.getProperty(REP_ACCESS_CONTROLLED_PATH);
        return property != null && path.equals(property.getValue(Type.STRING));
    }

    @Nonnull
    public static Tree getPermissionsRoot(@Nonnull Root root, @Nonnull String workspaceName) {
        return root.getTree(PERMISSIONS_STORE_PATH + '/' + workspaceName);
    }

    @Nonnull
    public static Tree getPrincipalRoot(@Nonnull Tree permissionsTree, @Nonnull String principalName) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principalName));
    }

    @CheckForNull
    public static String getPath(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        String path = null;
        if (parentBefore != null) {
            path = parentBefore.getPath();
        } else if (parentAfter != null) {
            path = parentAfter.getPath();
        }
        return path;
    }
}