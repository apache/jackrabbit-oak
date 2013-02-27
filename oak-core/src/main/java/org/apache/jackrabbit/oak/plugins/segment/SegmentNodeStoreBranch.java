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

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.RebaseDiff;

class SegmentNodeStoreBranch implements NodeStoreBranch {

    private final SegmentStore store;

    private final String journal;

    private final SegmentReader reader;

    private final SegmentWriter writer;

    private RecordId baseId;

    private RecordId rootId;

    SegmentNodeStoreBranch(SegmentStore store, String journal, SegmentReader reader) {
        this.store = store;
        this.journal = journal;
        this.reader = reader;
        this.writer = new SegmentWriter(store, reader);
        this.baseId = store.getJournalHead(journal);
        this.rootId = baseId;
    }

    @Override @Nonnull
    public NodeState getBase() {
        return new SegmentNodeState(reader, baseId);
    }

    @Override @Nonnull
    public synchronized NodeState getHead() {
        return new SegmentNodeState(reader, rootId);
    }

    @Override
    public synchronized void setRoot(NodeState newRoot) {
        this.rootId = writer.writeNode(newRoot).getRecordId();
        writer.flush();
    }

    @Override
    public synchronized void rebase() {
        RecordId newBaseId = store.getJournalHead(journal);
        if (!baseId.equals(newBaseId)) {
            NodeBuilder builder =
                    new MemoryNodeBuilder(new SegmentNodeState(reader, newBaseId));
            getHead().compareAgainstBaseState(getBase(), new RebaseDiff(builder));
            this.baseId = newBaseId;
            this.rootId = writer.writeNode(builder.getNodeState()).getRecordId();
            writer.flush();
        }
    }

    @Override @Nonnull
    public synchronized NodeState merge(CommitHook hook)
            throws CommitFailedException {
        RecordId originalBaseId = baseId;
        RecordId originalRootId = rootId;
        long backoff = 1;
        for (int i = 0; i < 10; i++) {
            // rebase to latest head and apply commit hooks
            rebase();
            RecordId headId = writer.writeNode(
                    hook.processCommit(getBase(), getHead())).getRecordId();
            writer.flush();

            // use optimistic locking to update the journal
            if (store.setJournalHead(journal, headId, baseId)) {
                baseId = headId;
                rootId = headId;
                return getHead();
            }

            // someone else was faster, so clear state and try again later
            baseId = originalBaseId;
            rootId = originalRootId;

            // use exponential backoff to reduce contention
            try {
                TimeUnit.MICROSECONDS.sleep(backoff);
                backoff *= 2;
            } catch (InterruptedException e) {
                throw new CommitFailedException("Commit was interrupted", e);
            }
        }
        throw new CommitFailedException("System overloaded, try again later");
    }

    @Override
    public boolean move(String source, String target) {
        if (PathUtils.isAncestor(source, target)) {
            return false;
        } else if (source.equals(target)) {
            return true;
        }

        NodeBuilder builder = getHead().builder();

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
        NodeBuilder builder = getHead().builder();

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
