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

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventTypeFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.NodeTypeFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.PathFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.UuidFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

/**
 * Provider for a filter filtering observation events according to a certain criterion.
 */
public class FilterProvider {
    private final ReadOnlyNodeTypeManager ntManager;
    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuids;
    private final String[] ntNames;
    private final boolean includeSessionLocal;
    private final boolean includeClusterExternal;

    /**
     * Create a new instance of a {@code FilterProvider} for certain criteria
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
     *
     * @see javax.jcr.observation.ObservationManager#addEventListener(javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean) */
    public FilterProvider(ReadOnlyNodeTypeManager ntManager, int eventTypes, String path,
            boolean deep, String[] uuids, String[] nodeTypeName,
            boolean includeSessionLocal, boolean includeClusterExternal) {
        this.ntManager = ntManager;
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuids = uuids;
        this.ntNames = nodeTypeName;
        this.includeSessionLocal = includeSessionLocal;
        this.includeClusterExternal = includeClusterExternal;
    }

    public boolean includeCommit(String sessionId, CommitInfo info) {
        return (includeSessionLocal || !isLocal(sessionId, info))
            && (includeClusterExternal || !isExternal(info));
    }

    public Filter getFilter(ImmutableTree afterTree) {
        return Filters.all(
                // TODO add filter based on access rights of the reading session
                new PathFilter(afterTree, path, deep),
                new EventTypeFilter(eventTypes),
                new UuidFilter(afterTree.getNodeState(), uuids),
                new NodeTypeFilter(afterTree, ntManager, ntNames));
    }

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

    private static boolean isLocal(String sessionId, CommitInfo info) {
        return info != null && Objects.equal(info.getSessionId(), sessionId);
    }

    private static boolean isExternal(CommitInfo info) {
        return info == null;
    }

}
