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

package org.apache.jackrabbit.oak.plugins.observation2;

import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.DEEP;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.NO_LOCAL;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.PATH;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.TYPE;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.UUID;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class EventFilter {
    private static final Logger log = LoggerFactory.getLogger(EventFilter.class);

    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuid;          // TODO implement filtering by uuid
    private final String[] nodeTypeNames;
    private final boolean noLocal;        // TODO implement filtering by noLocal

    public EventFilter(int eventTypes, String path, boolean deep, String[] uuid,
            String[] nodeTypeName, boolean noLocal) {
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuid = uuid;
        this.nodeTypeNames = nodeTypeName;
        this.noLocal = noLocal;
    }

    public boolean include(int eventType, String path, @Nullable String[] associatedType, ReadOnlyNodeTypeManager ntMgr) {
        return includeEventType(eventType)
                && includePath(path)
                && includeNodeType(associatedType, ntMgr);
    }

    //-----------------------------< internal >---------------------------------

    private boolean includeEventType(int eventType) {
        return (this.eventTypes & eventType) != 0;
    }

    private boolean includePath(String path) {
        String app = PathUtils.getParentPath(path);
        boolean equalPaths = this.path.equals(app);
        if (!deep && !equalPaths) {
            return false;
        }
        if (deep && !(PathUtils.isAncestor(this.path, app) || equalPaths)) {
            return false;
        }
        return true;
    }

    private boolean includeNodeType(String[] associatedParentTypes, ReadOnlyNodeTypeManager ntMgr) {
        if (nodeTypeNames == null || associatedParentTypes == null) {
            return true;
        }
        for (String type : nodeTypeNames) {
            for (String apt : associatedParentTypes) {
                if (ntMgr.isNodeType(apt, type)) {
                    return true;
                }
            }
        }
        return false;
    }

    void persist(Tree tree) {
        tree.setProperty(TYPE, eventTypes);
        tree.setProperty(PATH, path);
        tree.setProperty(DEEP, deep);
        if (uuid != null) {
            tree.setProperty(UUID, Arrays.asList(uuid), Type.STRINGS);
        }
        if (nodeTypeNames != null) {
            tree.setProperty(NODE_TYPES, Arrays.asList(nodeTypeNames), Type.STRINGS);
        }
        tree.setProperty(NO_LOCAL, noLocal);
    }
}
