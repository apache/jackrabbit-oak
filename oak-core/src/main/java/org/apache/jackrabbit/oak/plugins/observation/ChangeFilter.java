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
package org.apache.jackrabbit.oak.plugins.observation;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class ChangeFilter {
    private final int eventTypes;
    private final String path;
    private final boolean deep;
    private final String[] uuid;          // TODO implement filtering by uuid
    private final String[] nodeTypeName;  // TODO implement filtering by nodeTypeName
    private final boolean noLocal;        // TODO implement filtering by noLocal

    public ChangeFilter(int eventTypes, String path, boolean deep, String[] uuid, String[] nodeTypeName,
            boolean noLocal) {
        this.eventTypes = eventTypes;
        this.path = path;
        this.deep = deep;
        this.uuid = uuid;
        this.nodeTypeName = nodeTypeName;
        this.noLocal = noLocal;
    }

    public boolean include(int eventType) {
        return (this.eventTypes & eventType) != 0;
    }

    public boolean include(String path) {
        boolean equalPaths = this.path.equals(path);
        if (!deep && !equalPaths) {
            return false;
        }
        if (deep && !(PathUtils.isAncestor(this.path, path) || equalPaths)) {
            return false;
        }
        return true;
    }

    public boolean include(int eventType, String path, NodeState associatedParentNode) {
        return include(eventType) && include(path);
    }

    public boolean includeChildren(String path) {
        return PathUtils.isAncestor(path, this.path) ||
                path.equals(this.path) ||
                deep && PathUtils.isAncestor(this.path, path);
    }
}
