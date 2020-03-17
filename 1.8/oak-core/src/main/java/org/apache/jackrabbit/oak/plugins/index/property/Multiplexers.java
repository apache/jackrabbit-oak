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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.FilteringIndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

public class Multiplexers {

    static boolean RO_PRIVATE_UNIQUE_INDEX;

    private Multiplexers() {
    }

    static {
        // TODO OAK-4645 set default to true once the code is stable
        String ro = System.getProperty(
                "oak.multiplexing.readOnlyPrivateUniqueIndex", "false");
        RO_PRIVATE_UNIQUE_INDEX = Boolean.parseBoolean(ro);
    }

    /** Index storage strategy */
    private static final IndexStoreStrategy UNIQUE = new UniqueEntryStoreStrategy(
            INDEX_CONTENT_NODE_NAME);

    /** Index storage strategy */
    private static final IndexStoreStrategy MIRROR = new ContentMirrorStoreStrategy(
            INDEX_CONTENT_NODE_NAME);

    public static Set<IndexStoreStrategy> getStrategies(boolean unique,
            MountInfoProvider mountInfoProvider, NodeBuilder definition,
            String defaultName) {
        Iterable<String> children = definition.getChildNodeNames();
        return getStrategies(unique, mountInfoProvider, children, defaultName);
    }

    public static Set<IndexStoreStrategy> getStrategies(boolean unique,
            MountInfoProvider mountInfoProvider, NodeState definition,
            String defaultName) {
        Iterable<String> children = definition.getChildNodeNames();
        return getStrategies(unique, mountInfoProvider, children, defaultName);
    }

    private static Set<IndexStoreStrategy> getStrategies(boolean unique,
            MountInfoProvider mountInfoProvider, Iterable<String> children,
            String defaultName) {
        if (mountInfoProvider.hasNonDefaultMounts()) {
            Set<String> names = new HashSet<String>();
            // TODO should this be collected from the index def?
            for (String name : children) {
                if (isIndexStorageNode(name, defaultName)) {
                    names.add(name);
                }
            }
            names.remove(defaultName);
            Set<IndexStoreStrategy> strategies = new HashSet<IndexStoreStrategy>();
            for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
                String n = getNodeForMount(m, defaultName);
                names.remove(n);
                strategies.add(newStrategy(unique, false, n, m));
            }

            Mount defMount = mountInfoProvider.getDefaultMount();
            // TODO what to do with non-default names that are not covered by
            // the mount?
            for (String n : names) {
                strategies.add(newStrategy(unique, true, n, defMount));
            }
            // default mount
            strategies.add(newStrategy(unique, true, defaultName, defMount));
            return strategies;
        } else {
            return unique ? ImmutableSet.of(newUniqueStrategy(defaultName))
                    : ImmutableSet.of(newMirrorStrategy(defaultName));
        }
    }

    private static IndexStoreStrategy newUniqueStrategy(String defaultName) {
        if (INDEX_CONTENT_NODE_NAME.equals(defaultName)) {
            return UNIQUE;
        } else {
            return new UniqueEntryStoreStrategy(defaultName);
        }
    }

    private static IndexStoreStrategy newMirrorStrategy(String defaultName) {
        if (INDEX_CONTENT_NODE_NAME.equals(defaultName)) {
            return MIRROR;
        } else {
            return new ContentMirrorStoreStrategy(defaultName);
        }
    }

    private static IndexStoreStrategy newStrategy(boolean unique,
            boolean defaultMount, String name, Mount m) {
        Predicate<String> filter = newFilter(m);
        boolean readOnly = unique && !m.isDefault() && RO_PRIVATE_UNIQUE_INDEX;
        return unique ? new FilteringIndexStoreStrategy(
                new UniqueEntryStoreStrategy(name), filter, readOnly)
                : new FilteringIndexStoreStrategy(
                        new ContentMirrorStoreStrategy(name), filter);
    }

    private static Predicate<String> newFilter(final Mount m) {
        return new Predicate<String>() {

            @Override
            public boolean apply(String p) {
                return m.isMounted(p);
            }
        };
    }

    private static boolean isIndexStorageNode(String name, String defaultName) {
        return NodeStateUtils.isHidden(name)
                && (name.equals(defaultName) || name
                        .endsWith(asSuffix(defaultName)));
    }

    public static String getIndexNodeName(MountInfoProvider mountInfoProvider,
            String path, String defaultName) {
        Mount mount = mountInfoProvider.getMountByPath(path);
        return getNodeForMount(mount, defaultName);
    }

    public static String getNodeForMount(Mount mount, String defaultName) {
        if (mount.isDefault()) {
            return defaultName;
        }
        return ":" + mount.getPathFragmentName() + asSuffix(defaultName);
    }

    private static String asSuffix(String name) {
        return "-" + stripStartingColon(name);
    }

    public static String stripStartingColon(String name) {
        if (name.startsWith(":")) {
            return name.substring(1);
        }
        return name;
    }

}
