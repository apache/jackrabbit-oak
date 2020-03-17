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

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState.PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

class PathFilteringDiff extends ApplyDiff {
    private static final Logger logger = LoggerFactory.getLogger(PathFilteringDiff.class);
    private final DiffContext ctx;
    private final AbstractDocumentNodeState parent;

    public PathFilteringDiff(NodeBuilder builder, PathFilter pathFilter, List<String> metaPropNames, AbstractDocumentNodeState parent) {
        this(builder, new DiffContext(pathFilter, metaPropNames), parent);
    }

    private PathFilteringDiff(NodeBuilder builder, DiffContext ctx, AbstractDocumentNodeState parent) {
        super(builder);
        this.ctx = ctx;
        this.parent = parent;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        AbstractDocumentNodeState afterDoc = asDocumentState(after, name);
        String nextPath = afterDoc.getPath();
        PathFilter.Result result = ctx.pathFilter.filter(nextPath);
        if (result == PathFilter.Result.EXCLUDE){
            return true;
        }

        ctx.traversingNode(nextPath);
        //We avoid this as we need to copy meta properties
        //super.childNodeAdded(name, after);

        NodeBuilder childBuilder = builder.child(name);
        copyMetaProperties(afterDoc, childBuilder, ctx.metaPropNames);
        return after.compareAgainstBaseState(EMPTY_NODE,
                new PathFilteringDiff(childBuilder, ctx, afterDoc));
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        AbstractDocumentNodeState afterDoc = asDocumentState(after, name);
        String nextPath = afterDoc.getPath();
        if (ctx.pathFilter.filter(nextPath) != PathFilter.Result.EXCLUDE) {
            ctx.traversingNode(nextPath);
            NodeBuilder childBuilder = builder.getChildNode(name);
            copyMetaProperties(afterDoc, childBuilder, ctx.metaPropNames);
            return after.compareAgainstBaseState(
                    before, new PathFilteringDiff(builder.getChildNode(name), ctx, afterDoc));
        }
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String path = asDocumentState(before, name).getPath();
        if (ctx.pathFilter.filter(path) != PathFilter.Result.EXCLUDE) {
            return super.childNodeDeleted(name, before);
        }
        return true;
    }

    private AbstractDocumentNodeState asDocumentState(NodeState state, String name){
        checkArgument(state instanceof AbstractDocumentNodeState, "Node %s (%s) at [%s/%s] is not" +
                " of expected type i.e. AbstractDocumentNodeState. Parent %s (%s)",
                state, state.getClass(), parent.getPath(), name, parent, parent.getClass());
        return (AbstractDocumentNodeState) state;
    }

    static void copyMetaProperties(AbstractDocumentNodeState state, NodeBuilder builder, List<String> metaPropNames) {
        //Only set root revision on root node
        if (PathUtils.denotesRoot(state.getPath())) {
            builder.setProperty(asPropertyState(PROP_REVISION, state.getRootRevision()));
        }

        //LastRev would be set on each node
        builder.setProperty(asPropertyState(PROP_LAST_REV, state.getLastRevision()));

        for (String metaProp : metaPropNames){
            PropertyState ps = state.getProperty(metaProp);
            if (ps != null){
                builder.setProperty(ps);
            }
        }
    }

    private static PropertyState asPropertyState(String name, RevisionVector revision) {
        return createProperty(name, revision.asString());
    }

    private static class DiffContext {
        private long count;
        final PathFilter pathFilter;
        final List<String> metaPropNames;

        public DiffContext(PathFilter filter, List<String> metaPropNames) {
            this.pathFilter = filter;
            this.metaPropNames = metaPropNames;
        }

        public void traversingNode(String path){
            if (++count % 10000 == 0) {
                logger.info("Updating Secondary Store. Traversed #{} - {}", count, path);
            }
        }
    }
}
