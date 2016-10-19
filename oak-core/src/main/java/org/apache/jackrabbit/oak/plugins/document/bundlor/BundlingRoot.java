/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.PathUtils;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;

class BundlingRoot {
    private final String rootPath;
    private final DocumentBundlor documentBundlor;

    public BundlingRoot() {
        this(ROOT_PATH, null);
    }

    public BundlingRoot(String rootPath, @Nullable DocumentBundlor documentBundlor) {
        this.rootPath = rootPath;
        this.documentBundlor = documentBundlor;
    }

    public boolean bundlingEnabled(){
        return documentBundlor != null;
    }

    public String getPropertyPath(String path, String propertyName) {
        if (isBundled(path)){
            return PathUtils.concat(relativePath(path), propertyName);
        }
        //TODO Assert that in non bundling case path is of depth 1
        return propertyName;
    }

    public boolean isBundled(String childPath) {
        return bundlingEnabled() && documentBundlor.isBundled(relativePath(childPath));
    }

    public String getPath() {
        return rootPath;
    }

    private String relativePath(String nodePath){
        return PathUtils.relativize(rootPath, nodePath);
    }
}
