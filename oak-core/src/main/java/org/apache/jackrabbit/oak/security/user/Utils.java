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
package org.apache.jackrabbit.oak.security.user;

import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

class Utils {

    private Utils() {}

    /**
     * TODO: clean up. workaround for OAK-426
     * <p>
     * Create the tree at the specified relative path including all missing
     * intermediate trees using the specified {@code primaryTypeName}. This
     * method treats ".." parent element and "." as current element and
     * resolves them accordingly; in case of a relative path containing parent
     * elements this may lead to tree creating outside the tree structure
     * defined by this {@code NodeUtil}.
     *
     * @param relativePath    A relative OAK path that may contain parent and
     *                        current elements.
     * @param primaryTypeName A oak name of a primary node type that is used
     *                        to create the missing trees.
     * @return The node util of the tree at the specified {@code relativePath}.
     * @throws AccessDeniedException If the any intermediate tree does not exist
     *                               and cannot be created.
     */
    @Nonnull
    static Tree getOrAddTree(@Nonnull Tree tree, @Nonnull String relativePath, @Nonnull String primaryTypeName) throws AccessDeniedException {
        if (PathUtils.denotesCurrent(relativePath)) {
            return tree;
        } else if (PathUtils.denotesParent(relativePath)) {
            return tree.getParent();
        } else if (relativePath.indexOf('/') == -1) {
            return TreeUtil.getOrAddChild(tree, relativePath, primaryTypeName);
        } else {
            Tree t = TreeUtil.getTree(tree, relativePath);
            if (t == null || !t.exists()) {
                Tree target = tree;
                for (String segment : Text.explode(relativePath, '/')) {
                    if (PathUtils.denotesParent(segment)) {
                        target = target.getParent();
                    } else if (target.hasChild(segment)) {
                        target = target.getChild(segment);
                    } else if (!PathUtils.denotesCurrent(segment)) {
                        target = TreeUtil.addChild(target, segment, primaryTypeName);
                    }
                }
                if (!target.exists()) {
                    throw new AccessDeniedException();
                }
                return target;
            } else {
                return t;
            }
        }
    }
}