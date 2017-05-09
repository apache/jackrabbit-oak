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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.Text;

/**
 * Utility class to determine the top-level CUG paths as recorded on the root
 * node.
 */
class TopLevelPaths implements CugConstants {

    static final long NONE = -1;
    static final long MAX_CNT = 10;

    private final Root root;

    private Boolean hasAny;
    private Long cnt;
    private String[] paths;

    TopLevelPaths(Root root) {
        this.root = root;
    }

    boolean hasAny() {
        if (hasAny == null) {
            Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
            hasAny = rootTree.hasProperty(HIDDEN_TOP_CUG_CNT) || CugUtil.hasCug(rootTree);
        }
        return hasAny;
    }

    boolean contains(@Nonnull String path) {
        if (!hasAny()) {
            return false;
        }
        if (PathUtils.denotesRoot(path)) {
            return true;
        }

        if (cnt == null) {
            Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
            PropertyState hiddenTopCnt = rootTree.getProperty(HIDDEN_TOP_CUG_CNT);
            if (hiddenTopCnt != null) {
                cnt = hiddenTopCnt.getValue(Type.LONG);
                if (cnt <= MAX_CNT) {
                    PropertyState hidden = root.getTree(PathUtils.ROOT_PATH).getProperty(HIDDEN_NESTED_CUGS);
                    paths = (hidden == null) ? new String[0] : Iterables.toArray(hidden.getValue(Type.STRINGS), String.class);
                } else {
                    paths = null;
                }
            } else {
                cnt = NONE;
            }
        }

        if (cnt == NONE) {
            return false;
        } if (cnt > MAX_CNT) {
            return true;
        } else if (paths != null) {
            for (String p : paths) {
                if (Text.isDescendantOrEqual(path, p)) {
                    return true;
                }
            }
        }
        return false;
    }
}