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
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentNodeStoreBranch extends AbstractNodeStoreBranch {

    private static final Random RANDOM = new Random();

    private final SegmentNodeStore store;

    private final SegmentWriter writer;

    private SegmentNodeState base;

    private SegmentNodeState head;

    SegmentNodeStoreBranch(SegmentNodeStore store, SegmentWriter writer) {
        this.store = store;
        this.writer = writer;
        this.base = store.getRoot();
        this.head = base;
    }

    @Override @Nonnull
    public NodeState getBase() {
        return base;
    }

    @Override @Nonnull
    public synchronized NodeState getHead() {
        return head;
    }

    @Override
    public synchronized void setRoot(NodeState newRoot) {
        head = writer.writeNode(newRoot);
        writer.flush();
    }

    @Override
    public synchronized void rebase() {
        SegmentNodeState newBase = store.getRoot();
        if (!base.getRecordId().equals(newBase.getRecordId())) {
            NodeBuilder builder = newBase.builder();
            head.compareAgainstBaseState(
                    base, new ConflictAnnotatingRebaseDiff(builder));
            base = newBase;
            head = writer.writeNode(builder.getNodeState());
            writer.flush();
        }
    }

    @Override @Nonnull
    public synchronized NodeState merge(CommitHook hook)
            throws CommitFailedException {
        SegmentNodeState originalBase = base;
        SegmentNodeState originalHead = head;
        long backoff = 1;
        while (base != head) {
            // apply commit hooks on the rebased changes
            SegmentNodeState root = writer.writeNode(
                    hook.processCommit(base, head));
            writer.flush();

            // use optimistic locking to update the journal
            if (store.setHead(base, root)) {
                base = root;
                head = root;
            } else {
                // someone else was faster, so restore state and retry later
                base = originalBase;
                head = originalHead;

                // use exponential backoff to reduce contention
                if (backoff < 10000) {
                    try {
                        Thread.sleep(backoff, RANDOM.nextInt(1000000));
                        backoff *= 2;
                    } catch (InterruptedException e) {
                        throw new CommitFailedException(
                                "Segment", 1, "Commit was interrupted", e);
                    }
                } else {
                    throw new CommitFailedException(
                            "Segment", 2,
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
        targetBuilder.setChildNode(targetName, sourceState);
        sourceBuilder.getChildNode(sourceName).remove();

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
        targetBuilder.setChildNode(targetName, sourceState);

        setRoot(builder.getNodeState());
        return true;
    }

}
