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
package org.apache.jackrabbit.oak.plugins.segment;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

class SegmentNodeStoreBranch implements NodeStoreBranch {

    private final SegmentStore store;

    private final SegmentReader reader;

    private final SegmentWriter writer;

    private final CommitHook hook;

    private RecordId baseId;

    private RecordId rootId;

    SegmentNodeStoreBranch(
            SegmentStore store, SegmentReader reader, CommitHook hook) {
        this.store = store;
        this.reader = reader;
        this.writer = new SegmentWriter(store);
        this.hook = hook;
        this.baseId = store.getJournalHead();
        this.rootId = baseId;
    }

    @Override @Nonnull
    public NodeState getBase() {
        return new SegmentNodeState(reader, baseId);
    }

    @Override @Nonnull
    public synchronized NodeState getRoot() {
        return new SegmentNodeState(reader, rootId);
    }

    @Override
    public synchronized void setRoot(NodeState newRoot) {
        this.rootId = writer.writeNode(newRoot);
        writer.flush();
    }

    // FIXME: Proper rebase needed
    private class RebaseDiff implements NodeStateDiff {
    
        private final NodeBuilder builder;

        RebaseDiff(NodeBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void propertyAdded(PropertyState after) {
            PropertyState other = builder.getProperty(after.getName());
            if (other == null) {
                builder.setProperty(after);
            } else if (!other.equals(after)) {
                conflictMarker("addExistingProperty").setProperty(after);
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            PropertyState other = builder.getProperty(before.getName());
            if (other == null) {
                conflictMarker("changeDeletedProperty").setProperty(after);
            } else if (other.equals(before)) {
                builder.setProperty(after);
            } else if (!other.equals(after)) {
                conflictMarker("changeChangedProperty").setProperty(after);
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            PropertyState other = builder.getProperty(before.getName());
            if (other == null) {
                conflictMarker("deleteDeletedProperty").setProperty(before);
            } else if (other.equals(before)) {
                builder.removeProperty(before.getName());
            } else {
                conflictMarker("deleteChangedProperty").setProperty(before);
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (builder.hasChildNode(name)) {
                conflictMarker("addExistingNode").setNode(name, after);
            } else {
                builder.setNode(name, after);
            }
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (builder.hasChildNode(name)) {
                after.compareAgainstBaseState(
                        before, new RebaseDiff(builder.child(name)));
            } else {
                conflictMarker("changeDeletedNode").setNode(name, after);
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (!builder.hasChildNode(name)) {
                conflictMarker("deleteDeletedNode").setNode(name, before);
            } else if (before.equals(builder.child(name).getNodeState())) {
                builder.removeNode(name);
            } else {
                conflictMarker("deleteChangedNode").setNode(name, before);
            }
        }

        private NodeBuilder conflictMarker(String name) {
            return builder.child(":conflict").child(name);
        }

    }

    @Override
    public synchronized void rebase() {
        RecordId newBaseId = store.getJournalHead();
        NodeBuilder builder =
                new MemoryNodeBuilder(new SegmentNodeState(reader, newBaseId));
        getRoot().compareAgainstBaseState(getBase(), new RebaseDiff(builder));
        this.baseId = newBaseId;
        this.rootId = writer.writeNode(builder.getNodeState());
        writer.flush();
    }

    @Override @Nonnull
    public synchronized NodeState merge() throws CommitFailedException {
        RecordId originalBaseId = baseId;
        RecordId originalRootId = rootId;
        for (int i = 0; i < 10; i++) {
            if (i > 0) {
                baseId = originalBaseId;
                rootId = originalRootId;
                rebase();
            }
            RecordId headId =
                    writer.writeNode(hook.processCommit(getBase(), getRoot()));
            writer.flush();
            if (store.setJournalHead(rootId, baseId)) {
                baseId = headId;
                rootId = headId;
                return getRoot();
            }
        }
        throw new CommitFailedException();
    }

    @Override
    public boolean move(String source, String target) {
        if (PathUtils.isAncestor(source, target)) {
            return false;
        } else if (source.equals(target)) {
            return true;
        }

        NodeBuilder builder = getRoot().builder();

        NodeBuilder targetBuilder = builder;
        String targetParent = PathUtils.getParentPath(target);
        for (String name : PathUtils.elements(targetParent)) {
            if (targetBuilder.hasChildNode(name)) {
                targetBuilder = targetBuilder.child(name);
            } else {
                return false;
            }
        }
        String targetName = PathUtils.getName(target);
        if (targetBuilder.hasChildNode(targetName)) {
            return false;
        }

        NodeBuilder sourceBuilder = builder;
        String sourceParent = PathUtils.getParentPath(source);
        for (String name : PathUtils.elements(sourceParent)) {
            if (sourceBuilder.hasChildNode(name)) {
                sourceBuilder = sourceBuilder.child(name);
            } else {
                return false;
            }
        }
        String sourceName = PathUtils.getName(source);
        if (!sourceBuilder.hasChildNode(sourceName)) {
            return false;
        }

        NodeState sourceState = sourceBuilder.child(sourceName).getNodeState();
        targetBuilder.setNode(targetName, sourceState);
        sourceBuilder.removeNode(sourceName);

        setRoot(builder.getNodeState());
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        NodeBuilder builder = getRoot().builder();

        NodeBuilder targetBuilder = builder;
        String targetParent = PathUtils.getParentPath(target);
        for (String name : PathUtils.elements(targetParent)) {
            if (targetBuilder.hasChildNode(name)) {
                targetBuilder = targetBuilder.child(name);
            } else {
                return false;
            }
        }
        String targetName = PathUtils.getName(target);
        if (targetBuilder.hasChildNode(targetName)) {
            return false;
        }

        NodeBuilder sourceBuilder = builder;
        String sourceParent = PathUtils.getParentPath(source);
        for (String name : PathUtils.elements(sourceParent)) {
            if (sourceBuilder.hasChildNode(name)) {
                sourceBuilder = sourceBuilder.child(name);
            } else {
                return false;
            }
        }
        String sourceName = PathUtils.getName(source);
        if (!sourceBuilder.hasChildNode(sourceName)) {
            return false;
        }

        NodeState sourceState = sourceBuilder.child(sourceName).getNodeState();
        targetBuilder.setNode(targetName, sourceState);

        setRoot(builder.getNodeState());
        return true;
    }

}
