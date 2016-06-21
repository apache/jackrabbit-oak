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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_PATH;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

class PathFilteringDiff extends ApplyDiff {
    private final PathFilter pathFilter;

    public PathFilteringDiff(NodeBuilder builder, PathFilter pathFilter) {
        super(builder);
        this.pathFilter = pathFilter;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        AbstractDocumentNodeState afterDoc = asDocumentState(after);
        String nextPath = afterDoc.getPath();
        PathFilter.Result result = pathFilter.filter(nextPath);
        if (result == PathFilter.Result.EXCLUDE){
            return true;
        }

        //We avoid this as we need to copy meta properties
        //super.childNodeAdded(name, after);

        NodeBuilder childBuilder = builder.child(name);
        copyMetaProperties(afterDoc, childBuilder);
        return after.compareAgainstBaseState(EMPTY_NODE,
                new PathFilteringDiff(childBuilder, pathFilter));
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        AbstractDocumentNodeState afterDoc = asDocumentState(after);
        String nextPath = afterDoc.getPath();
        if (pathFilter.filter(nextPath) != PathFilter.Result.EXCLUDE) {
            NodeBuilder childBuilder = builder.getChildNode(name);
            copyMetaProperties(afterDoc, childBuilder);
            return after.compareAgainstBaseState(
                    before, new PathFilteringDiff(builder.getChildNode(name), pathFilter));
        }
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String path = asDocumentState(before).getPath();
        if (pathFilter.filter(path) != PathFilter.Result.EXCLUDE) {
            return super.childNodeDeleted(name, before);
        }
        return true;
    }

    static void copyMetaProperties(AbstractDocumentNodeState state, NodeBuilder builder) {
        builder.setProperty(asPropertyState(PROP_REVISION, state.getRevision()));
        builder.setProperty(asPropertyState(PROP_LAST_REV, state.getLastRevision()));
        builder.setProperty(createProperty(PROP_PATH, state.getPath()));
    }

    private static PropertyState asPropertyState(String name, RevisionVector revision) {
        return createProperty(name, revision.asString());
    }

    private static AbstractDocumentNodeState asDocumentState(NodeState state){
        return (AbstractDocumentNodeState) state;
    }
}
