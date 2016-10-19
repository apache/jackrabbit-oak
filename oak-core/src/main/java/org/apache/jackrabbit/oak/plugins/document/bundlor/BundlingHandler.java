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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;

public class BundlingHandler {
    private final BundledTypesRegistry registry;
    private final String path;
    private final BundlingRoot root;

    public BundlingHandler(BundledTypesRegistry registry) {
        this(registry, new BundlingRoot(), ROOT_PATH);
    }

    private BundlingHandler(BundledTypesRegistry registry, BundlingRoot root, String path) {
        this.registry = registry;
        this.path = path;
        this.root = root;
    }

    public String getPropertyPath(String propertyName) {
        return root.getPropertyPath(path, propertyName);
    }

    public String getRootBundlePath() {
        return root.bundlingEnabled() ? root.getPath() : path;
    }

    public BundlingHandler childHandler(String name, NodeState state) {
        String childPath = PathUtils.concat(path, name);
        if (root.isBundled(childPath)) {
            return new BundlingHandler(registry, root, childPath);
        }

        //TODO For only add we should lookup (fully new add) we should lookup new bundlor
        //For update and delete we should always rely on existing bundlor config
        //TODO Check for pattern from state first
        DocumentBundlor bundlor = registry.getBundlor(state);
        return new BundlingHandler(registry, new BundlingRoot(childPath, bundlor), childPath);
    }

    public boolean isBundlingRoot() {
        return root.getPath().equals(path);
    }
}
