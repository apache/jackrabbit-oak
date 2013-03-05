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

import java.util.Random;

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

    private static final Random RANDOM = new Random();

    private final SegmentStore store;

    private final Journal journal;

    private final SegmentWriter writer;

    private RecordId baseId;

    private RecordId rootId;

    SegmentNodeStoreBranch(SegmentStore store, Journal journal) {
        this.store = store;
        this.journal = journal;
        this.writer = new SegmentWriter(store);
        this.baseId = journal.getHead();
        this.rootId = baseId;
    }

    @Override @Nonnull
    public NodeState getBase() {
        return new SegmentNodeState(store, baseId);
    }

    @Override @Nonnull
    public synchronized NodeState getHead() {
        return new SegmentNodeState(store, rootId);
    }

    @Override
    public synchronized void setRoot(NodeState newRoot) {
        this.rootId = writer.writeNode(newRoot).getRecordId();
        writer.flush();
    }

    @Override
    public synchronized void rebase() {
        RecordId newBaseId = journal.getHead();
        if (!baseId.equals(newBaseId)) {
            NodeBuilder builder =
                    new MemoryNodeBuilder(new SegmentNodeState(store, newBaseId));
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
        while (baseId != rootId) {
            // apply commit hooks on the rebased changes
            RecordId headId = writer.writeNode(
                    hook.processCommit(getBase(), getHead())).getRecordId();
            writer.flush();

            // use optimistic locking to update the journal
            if (journal.setHead(baseId, headId)) {
                baseId = headId;
                rootId = headId;
            } else {
                // someone else was faster, so restore state and retry later
                baseId = originalBaseId;
                rootId = originalRootId;

                // use exponential backoff to reduce contention
                if (backoff < 10000) {
                    try {
                        Thread.sleep(backoff, RANDOM.nextInt(1000000));
                        backoff *= 2;
                    } catch (InterruptedException e) {
                        throw new CommitFailedException(
                                "Commit was interrupted", e);
                    }
                } else {
                    throw new CommitFailedException(
                            "System overloaded, try again later");
                }

                // rebase to latest head before trying again
                rebase();
            }
        }

        return getHead();
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
