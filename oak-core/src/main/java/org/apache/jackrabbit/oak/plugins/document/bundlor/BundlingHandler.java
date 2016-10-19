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

import java.util.Collections;
import java.util.Set;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;

public class BundlingHandler {
    private final BundledTypesRegistry registry;
    private final String path;
    private final BundlingRoot root;
    private final Set<PropertyState> metaProps;

    public BundlingHandler(BundledTypesRegistry registry) {
        this(registry, new BundlingRoot(), ROOT_PATH, Collections.<PropertyState>emptySet());
    }

    private BundlingHandler(BundledTypesRegistry registry, BundlingRoot root, String path) {
        this(registry, root, path, Collections.<PropertyState>emptySet());
    }

    private BundlingHandler(BundledTypesRegistry registry, BundlingRoot root, String path, Set<PropertyState> metaProps) {
        this.registry = registry;
        this.path = path;
        this.root = root;
        this.metaProps = metaProps;
    }

    public String getPropertyPath(String propertyName) {
        return root.getPropertyPath(path, propertyName);
    }

    public Set<PropertyState> getMetaProps() {
        return metaProps;
    }

    public String getRootBundlePath() {
        return root.bundlingEnabled() ? root.getPath() : path;
    }

    public BundlingHandler childAdded(String name, NodeState state){
        String childPath = childPath(name);
        BundlingRoot childRoot;
        Set<PropertyState> metaProps = Collections.emptySet();
        if (root.isBundled(childPath)) {
            //TODO Add meta prop for bundled child node
            childRoot = root;
        } else {
            DocumentBundlor bundlor = registry.getBundlor(state);
            if (bundlor != null){
                PropertyState bundlorConfig = bundlor.asPropertyState();
                metaProps = Collections.singleton(bundlorConfig);
            }
            childRoot = new BundlingRoot(childPath, bundlor);
        }

        return new BundlingHandler(registry, childRoot, childPath, metaProps);
    }

    public BundlingHandler childDeleted(String name, NodeState state){
        String childPath = childPath(name);
        BundlingRoot childRoot;
        if (root.isBundled(childPath)) {
            //TODO Add meta prop for bundled child node
            childRoot = root;
        } else {
            childRoot = new BundlingRoot(childPath, getBundlorFromEmbeddedConfig(state));
        }
        return new BundlingHandler(registry, childRoot, childPath);
    }

    public BundlingHandler childChanged(String name, NodeState state){
        String childPath = childPath(name);
        BundlingRoot childRoot;
        if (root.isBundled(childPath)) {
            childRoot = root;
        } else {
            childRoot = new BundlingRoot(childPath, getBundlorFromEmbeddedConfig(state));
        }

        return new BundlingHandler(registry, childRoot,  childPath);
    }

    public boolean isBundlingRoot() {
        return root.getPath().equals(path);
    }

    private String childPath(String name){
        return PathUtils.concat(path, name);
    }

    @CheckForNull
    private static DocumentBundlor getBundlorFromEmbeddedConfig(NodeState state) {
        PropertyState bundlorConfig = state.getProperty(DocumentBundlor.META_PROP_PATTERN);
        DocumentBundlor bundlor = null;
        if (bundlorConfig != null){
            bundlor = DocumentBundlor.from(bundlorConfig);
        }
        return bundlor;
    }
}
