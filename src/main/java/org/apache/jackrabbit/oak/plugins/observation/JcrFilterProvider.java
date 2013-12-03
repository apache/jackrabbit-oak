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
package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Objects.toStringHelper;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR_STAR;

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.filter.ACFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventTypeFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.NodeTypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.Selectors;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.UuidPredicate;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;

/**
 * Provider for a filter filtering observation events according to a certain criterion.
 */
public class JcrFilterProvider implements FilterProvider {
    private static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;


    private final ReadOnlyNodeTypeManager ntManager;
    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuids;
    private final String[] ntNames;
    private final boolean includeSessionLocal;
    private final boolean includeClusterExternal;
    private final PermissionProvider permissionProvider;

    /**
     * Create a new instance of a {@code JcrFilterProvider} for certain criteria
     *
     * @param ntManager   node type manager
     * @param eventTypes  event types to include encoded as a bit mask
     * @param path        path to include
     * @param deep        {@code true} if descendants of {@code path} should be included.
     *                    {@code false} otherwise.
     * @param uuids       uuids to include
     * @param nodeTypeName              node type names to include
     * @param includeSessionLocal       include session local events if {@code true}.
     *                                  Exclude otherwise.
     * @param includeClusterExternal    include cluster external events if {@code true}.
     *                                  Exclude otherwise.
     * @param permissionProvider        permission provider to evaluate events against
     * @see javax.jcr.observation.ObservationManager#addEventListener(javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean) */
    public JcrFilterProvider(ReadOnlyNodeTypeManager ntManager, int eventTypes, String path,
            boolean deep, String[] uuids, String[] nodeTypeName,
            boolean includeSessionLocal, boolean includeClusterExternal,
            PermissionProvider permissionProvider) {
        this.ntManager = ntManager;
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuids = uuids;
        this.ntNames = nodeTypeName;
        this.includeSessionLocal = includeSessionLocal;
        this.includeClusterExternal = includeClusterExternal;
        this.permissionProvider = permissionProvider;
    }

    @Override
    public boolean includeCommit(String sessionId, CommitInfo info) {
        return (includeSessionLocal || !isLocal(sessionId, info))
            && (includeClusterExternal || !isExternal(info));
    }

    @Override
    public Filter getFilter(Tree beforeTree, Tree afterTree) {
        String relPath = PathUtils.relativize(afterTree.getPath(), path);
        String pathPattern = deep
            ? PathUtils.concat(relPath, STAR_STAR)
            : PathUtils.concat(relPath, STAR);

        List<Filter> filters = Lists.<Filter>newArrayList(
            new GlobbingPathFilter(beforeTree, afterTree, pathPattern)
        );

        if ((ALL_EVENTS & eventTypes) == 0) {
            return Filters.excludeAll();
        } else if ((ALL_EVENTS & eventTypes) != ALL_EVENTS) {
            filters.add(new EventTypeFilter(eventTypes));
        }

        if (uuids != null) {
            if (uuids.length == 0) {
                return Filters.excludeAll();
            } else {
                filters.add(new UniversalFilter(beforeTree, afterTree,
                        Selectors.PARENT, new UuidPredicate(uuids)));
            }
        }

        if (ntNames != null) {
            if (ntNames.length == 0) {
                return Filters.excludeAll();
            } else {
                filters.add(new UniversalFilter(beforeTree, afterTree,
                        Selectors.PARENT, new NodeTypePredicate(ntManager, ntNames)));
            }
        }

        filters.add(new ACFilter(beforeTree, afterTree, getTreePermission(afterTree)));
        return Filters.all(filters.toArray(new Filter[filters.size()]));
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("event types", eventTypes)
                .add("path", path)
                .add("deep", deep)
                .add("uuids", uuids)
                .add("node type names", ntNames)
                .add("includeSessionLocal", includeSessionLocal)
                .add("includeClusterExternal", includeClusterExternal)
            .toString();
    }

    //------------------------------------------------------------< private >---

    private TreePermission getTreePermission(Tree tree) {
        return tree.isRoot()
                ? permissionProvider.getTreePermission(tree, TreePermission.EMPTY)
                : permissionProvider.getTreePermission(tree, getTreePermission(tree.getParent()));
    }

    private static boolean isLocal(String sessionId, CommitInfo info) {
        return info != null && Objects.equal(info.getSessionId(), sessionId);
    }

    private static boolean isExternal(CommitInfo info) {
        return info == null;
    }

}
