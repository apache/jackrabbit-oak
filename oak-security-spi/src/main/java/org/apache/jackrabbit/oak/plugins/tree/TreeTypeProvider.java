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
package org.apache.jackrabbit.oak.plugins.tree;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;

public final class TreeTypeProvider {

    private final TreeContext ctx;

    public TreeTypeProvider(@Nonnull TreeContext authorizationContext) {
        this.ctx = authorizationContext;
    }

    public TreeType getType(@Nonnull Tree tree) {
        if (tree.isRoot()) {
            return TreeType.DEFAULT;
        } else {
            TreeType type;
            if (tree instanceof TreeTypeAware) {
                type = ((TreeTypeAware) tree).getType();
                if (type == null) {
                    type = internalGetType(tree);
                    ((TreeTypeAware) tree).setType(type);
                }
            } else {
                type = internalGetType(tree);
            }
            return type;
        }
    }

    public TreeType getType(@Nonnull Tree tree, @Nonnull TreeType parentType) {
        if (tree.isRoot()) {
            return TreeType.DEFAULT;
        }

        TreeType type;
        if (tree instanceof TreeTypeAware) {
            type = ((TreeTypeAware) tree).getType();
            if (type == null) {
                type = internalGetType(tree, parentType);
                ((TreeTypeAware) tree).setType(type);
            }
        } else {
            type = internalGetType(tree, parentType);
        }
        return type;
    }

    private TreeType internalGetType(@Nonnull Tree tree) {
        Tree t = tree;
        while (!t.isRoot()) {
            TreeType type = internalGetType(t.getName(), t);
            // stop walking up the hierarchy as soon as a special type is found
            if (TreeType.DEFAULT != type) {
                return type;
            }
            t = t.getParent();
        }
        return TreeType.DEFAULT;
    }

    private TreeType internalGetType(@Nonnull Tree tree, @Nonnull TreeType parentType) {
        TreeType type;
        switch (parentType) {
            case HIDDEN:
                type = TreeType.HIDDEN;
                break;
            case VERSION:
                type = TreeType.VERSION;
                break;
            case INTERNAL:
                type = TreeType.INTERNAL;
                break;
            case ACCESS_CONTROL:
                type = TreeType.ACCESS_CONTROL;
                break;
            default:
                type = internalGetType(tree.getName(), tree);
        }
        return type;
    }

    private TreeType internalGetType(@Nonnull String name, @Nonnull Tree tree) {
        TreeType type;
        if (NodeStateUtils.isHidden(name)) {
            type = TreeType.HIDDEN;
        } else if (VersionConstants.VERSION_STORE_ROOT_NAMES.contains(name)) {
            type = (JcrConstants.JCR_SYSTEM.equals(tree.getParent().getName())) ?  TreeType.VERSION : TreeType.DEFAULT;
        } else if (ctx.definesInternal(tree)) {
            type = TreeType.INTERNAL;
        } else if (ctx.definesContextRoot(tree)) {
            type = TreeType.ACCESS_CONTROL;
        } else {
            type = TreeType.DEFAULT;
        }
        return type;
    }
}