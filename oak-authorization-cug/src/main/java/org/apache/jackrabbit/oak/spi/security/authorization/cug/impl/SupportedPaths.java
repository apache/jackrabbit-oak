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
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PathUtils;

class SupportedPaths {

    private final String[] supportedPaths;
    private final String[] supportedAltPaths;

    private final boolean includesRootPath;

    SupportedPaths(@Nonnull Set<String> supportedPaths) {
        this.supportedPaths = supportedPaths.toArray(new String[supportedPaths.size()]);
        supportedAltPaths = new String[supportedPaths.size()];

        boolean foundRootPath = false;
        int i = 0;
        for (String p : supportedPaths) {
            if (PathUtils.denotesRoot(p)) {
                foundRootPath = true;
            } else {
                supportedAltPaths[i++] = p + '/';
            }
        }
        includesRootPath = foundRootPath;
    }

    /**
     * Test if the specified {@code path} is contained in any of the configured
     * supported paths for CUGs.
     *
     * @param path An absolute path.
     * @return {@code true} if the specified {@code path} is equal to or a
     * descendant of one of the configured supported paths.
     */
    boolean includes(@Nonnull String path) {
        if (supportedPaths.length == 0) {
            return false;
        }
        if (includesRootPath) {
            return true;
        }
        for (String p : supportedAltPaths) {
            if (path.startsWith(p)) {
                return true;
            }
        }
        for (String p : supportedPaths) {
            if (path.equals(p)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tests if further evaluation below {@code path} is required as one of the
     * configured supported paths is a descendant (e.g. there might be CUGs
     * in the subtree although the specified {@code path} does not directly
     * support CUGs.
     *
     * @param path An absolute path
     * @return {@code true} if there exists a configured supported path that is
     * a descendant of the given {@code path}.
     */
    boolean mayContainCug(@Nonnull String path) {
        if (supportedPaths.length == 0) {
            return false;
        }
        if (includesRootPath || PathUtils.denotesRoot(path)) {
            return true;
        }
        String path2 = path + '/';
        for (String sp : supportedPaths) {
            if (sp.startsWith(path2)) {
                return true;
            }
        }
        return false;
    }
}