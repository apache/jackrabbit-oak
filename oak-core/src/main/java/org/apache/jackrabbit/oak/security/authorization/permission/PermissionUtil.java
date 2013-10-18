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

import java.security.Principal;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.util.TreeLocation;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionUtil... TODO
 */
public final class PermissionUtil implements PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionUtil.class);

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
    public static ImmutableTree getPermissionsRoot(ImmutableRoot immutableRoot, String workspaceName) {
        return immutableRoot.getTree(PERMISSIONS_STORE_PATH + '/' + workspaceName);
    }

    @Nonnull
    public static Tree getPrincipalRoot(Tree permissionsTree, Principal principal) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
    }

    public static int getType(@Nonnull ImmutableTree tree, @Nullable PropertyState property) {
        // TODO: OAK-753 decide on where to filter out hidden items.
        // TODO: deal with hidden properties
        return tree.getType();
    }

    @CheckForNull
    public static TreeLocation createLocation(@Nonnull ImmutableRoot immutableRoot, @Nullable String oakPath) {
        if (oakPath != null && PathUtils.isAbsolute(oakPath)) {
            return TreeLocation.create(immutableRoot, oakPath);
        } else {
            log.debug("Unable to create location for path " + oakPath);
            return null;
        }
    }

    @Nonnull
    public static TreeLocation createLocation(@Nonnull Tree tree, @Nullable PropertyState property) {
        if (property == null) {
            return TreeLocation.create(tree);
        } else {
            return TreeLocation.create(tree, property);
        }
    }
}