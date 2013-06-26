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

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Filter for filtering observation events according to a certain criterion.
 */
class EventFilter {
    private final ReadOnlyNodeTypeManager ntMgr;
    private final NamePathMapper namePathMapper;
    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuids;
    private final String[] nodeTypeOakName;
    private final boolean noLocal;

    /**
     * Create a new instance of a filter for a certain criterion
     * @param ntMgr
     * @param namePathMapper
     * @param eventTypes  event types to include encoded as a bit mask
     * @param path        path to include
     * @param deep        {@code true} if descendants of {@code path} should be included. {@code false} otherwise.
     * @param uuids       uuids to include
     * @param nodeTypeName  node type names to include
     * @param noLocal       exclude session local events if {@code true}. Include otherwise.
     * @throws NoSuchNodeTypeException  if any of the node types in {@code nodeTypeName} does not exist
     * @throws RepositoryException      if an error occurs while reading from the node type manager.
     * @see javax.jcr.observation.ObservationManager#addEventListener(javax.jcr.observation.EventListener,
     * int, String, boolean, String[], String[], boolean)
     */
    public EventFilter(ReadOnlyNodeTypeManager ntMgr,
            NamePathMapper namePathMapper, int eventTypes,
            String path, boolean deep, String[] uuids,
            String[] nodeTypeName, boolean noLocal)
            throws NoSuchNodeTypeException, RepositoryException {
        this.ntMgr = ntMgr;
        this.namePathMapper = namePathMapper;
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuids = uuids;
        this.nodeTypeOakName = validateNodeTypeNames(nodeTypeName);
        this.noLocal = noLocal;
    }

    /**
     * Match an event against this filter.
     * @param eventType  type of the event
     * @param path       path of the event
     * @param associatedParentNode  associated parent node of the event
     * @return  {@code true} if the filter matches this event. {@code false} otherwise.
     */
    public boolean include(int eventType, String path, @Nullable NodeState associatedParentNode) {
        return include(eventType)
                && include(path)
                && (associatedParentNode == null || includeByType(new ReadOnlyTree(associatedParentNode)))
                && (associatedParentNode == null || includeByUuid(associatedParentNode));
    }

    /**
     * Determine whether the children of a {@code path} would be matched by this filter
     * @param path  path whose children to test
     * @return  {@code true} if the children of {@code path} could be matched by this filter
     */
    public boolean includeChildren(String path) {
        String thisOakPath = namePathMapper.getOakPath(this.path);
        String thatOakPath = namePathMapper.getOakPath(path);

        return PathUtils.isAncestor(thatOakPath, thisOakPath) ||
                path.equals(thisOakPath) ||
                deep && PathUtils.isAncestor(thisOakPath, thatOakPath);
    }

    /**
     * @return  the no local flag of this filter
     */
    public boolean excludeLocal() {
        return noLocal;
    }

    /**
     * @return  path of this filter
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("types", eventTypes)
                .add("path", path)
                .add("deep", deep)
                .add("uuids", uuids)
                .add("node types", nodeTypeOakName)
                .add("noLocal", noLocal)
            .toString();
    }

    //-----------------------------< internal >---------------------------------

    private boolean include(int eventType) {
        return (this.eventTypes & eventType) != 0;
    }

    private boolean include(String path) {
        String thisOakPath = namePathMapper.getOakPath(this.path);
        String thatOakPath = namePathMapper.getOakPath(path);

        boolean equalPaths = thisOakPath.equals(thatOakPath);
        if (!deep && !equalPaths) {
            return false;
        }

        if (deep && !(PathUtils.isAncestor(thisOakPath, thatOakPath) || equalPaths)) {
            return false;
        }
        return true;
    }

    /**
     * Checks whether to include an event based on the type of the associated
     * parent node and the node type filter.
     *
     * @param associatedParentNode the associated parent node of the event.
     * @return whether to include the event based on the type of the associated
     *         parent node.
     */
    private boolean includeByType(Tree associatedParentNode) {
        if (nodeTypeOakName == null) {
            return true;
        } else {
            for (String oakName : nodeTypeOakName) {
                if (ntMgr.isNodeType(associatedParentNode, oakName)) {
                    return true;
                }
            }
            // filter has node types set but none matched
            return false;
        }
    }

    private boolean includeByUuid(NodeState associatedParentNode) {
        if (uuids == null) {
            return true;
        }
        if (uuids.length == 0) {
            return false;
        }

        PropertyState uuidProperty = associatedParentNode.getProperty(JcrConstants.JCR_UUID);
        if (uuidProperty == null) {
            return false;
        }

        String parentUuid = uuidProperty.getValue(Type.STRING);
        for (String uuid : uuids) {
            if (parentUuid.equals(uuid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Validates the given node type names.
     *
     * @param nodeTypeNames the node type names.
     * @return the node type names as oak names.
     * @throws javax.jcr.nodetype.NoSuchNodeTypeException if one of the node type names refers to
     *                                 an non-existing node type.
     * @throws javax.jcr.RepositoryException     if an error occurs while reading from the
     *                                 node type manager.
     */
    @CheckForNull
    private String[] validateNodeTypeNames(@Nullable String[] nodeTypeNames)
            throws NoSuchNodeTypeException, RepositoryException {
        if (nodeTypeNames == null) {
            return null;
        }
        String[] oakNames = new String[nodeTypeNames.length];
        for (int i = 0; i < nodeTypeNames.length; i++) {
            ntMgr.getNodeType(nodeTypeNames[i]);
            oakNames[i] = namePathMapper.getOakName(nodeTypeNames[i]);
        }
        return oakNames;
    }
}
