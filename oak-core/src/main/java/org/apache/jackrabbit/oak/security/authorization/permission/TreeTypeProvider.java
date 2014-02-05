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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * <h3>TreeTypeProvider</h3>
  * For optimization purpose an Immutable tree will be associated with a
  * {@code TreeTypeProvider} that allows for fast detection of the following types
  * of Trees:
  *
  * <ul>
  *     <li>{@link #TYPE_HIDDEN}: a hidden tree whose name starts with ":".
  *     Please note that the whole subtree of a hidden node is considered hidden.</li>
  *     <li>{@link #TYPE_AC}: A tree that stores access control content
  *     and requires special access {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#READ_ACCESS_CONTROL permissions}.</li>
  *     <li>{@link #TYPE_VERSION}: if a given tree is located within
  *     any of the version related stores defined by JSR 283. Depending on the
  *     permission evaluation implementation those items require special treatment.</li>
  *     <li>{@link #TYPE_DEFAULT}: the default type for trees that don't
  *     match any of the upper types.</li>
  * </ul>
 */
public final class TreeTypeProvider {

    public static final int TYPE_NONE = 0;

    // regular trees
    public static final int TYPE_DEFAULT = 1;
    // version store(s) content
    public static final int TYPE_VERSION = 2;
    // repository internal content such as e.g. permissions store
    public static final int TYPE_INTERNAL = 4;
    // access control content
    public static final int TYPE_AC = 8;
    // hidden trees
    public static final int TYPE_HIDDEN = 16;

    private final Context contextInfo;

    public TreeTypeProvider(@Nonnull Context contextInfo) {
        this.contextInfo = contextInfo;
    }

    public int getType(ImmutableTree tree) {
        if (tree.isRoot()) {
            return TYPE_DEFAULT;
        } else {
            return getType(tree, getType(tree.getParent()));
        }
    }

    public int getType(ImmutableTree tree, int parentType) {
            if (tree.isRoot()) {
                return TYPE_DEFAULT;
            }

            int type;
            switch (parentType) {
                case TYPE_HIDDEN:
                    type = TYPE_HIDDEN;
                    break;
                case TYPE_VERSION:
                    type = TYPE_VERSION;
                    break;
                case TYPE_INTERNAL:
                    type = TYPE_INTERNAL;
                    break;
                case TYPE_AC:
                    type = TYPE_AC;
                    break;
                default:
                    String name = tree.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        type = TYPE_HIDDEN;
                    } else if (VersionConstants.VERSION_STORE_ROOT_NAMES.contains(name)) {
                        type = TYPE_VERSION;
                    } else if (PermissionConstants.REP_PERMISSION_STORE.equals(name)) {
                        type = TYPE_INTERNAL;
                    } else if (contextInfo.definesContextRoot(tree)) {
                        type = TYPE_AC;
                    } else {
                        type = TYPE_DEFAULT;
                    }
            }
            return type;
        }
}
