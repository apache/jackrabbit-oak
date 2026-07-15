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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

class TrackingDiff extends DefaultNodeStateDiff {

    final String path;
    final Set<String> added;
    final Set<String> deleted;
    final Set<String> modified;

    TrackingDiff() {
        this("/", new HashSet<String>(), new HashSet<String>(), new HashSet<String>());
    }

    private TrackingDiff(String path,
                         Set<String> added,
                         Set<String> deleted,
                         Set<String> modified) {
        this.path = path;
        this.added = added;
        this.deleted = deleted;
        this.modified = modified;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String p = PathUtils.concat(path, name);
        added.add(p);
        return after.compareAgainstBaseState(EMPTY_NODE, new TrackingDiff(p, added, deleted, modified));
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        String p = PathUtils.concat(path, name);
        modified.add(p);
        return after.compareAgainstBaseState(before, new TrackingDiff(p, added, deleted, modified));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String p = PathUtils.concat(path, name);
        deleted.add(p);
        return MISSING_NODE.compareAgainstBaseState(before, new TrackingDiff(p, added, deleted, modified));
    }
}
