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
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class AsyncIndexUpdate implements Runnable {

    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    /**
     * Name of the hidden node under which information about the
     * checkpoints seen and indexed by each async indexer is kept.
     */
    private static final String ASYNC = ":async";

    private static final long DEFAULT_LIFETIME = TimeUnit.HOURS.toMillis(1);

    private static final CommitFailedException CONCURRENT_UPDATE =
            new CommitFailedException("Async", 1, "Concurrent update detected");

    private final String name;

    private final NodeStore store;

    private final IndexEditorProvider provider;

    private final long lifetime = DEFAULT_LIFETIME; // TODO: make configurable

    /** Flag to avoid repeatedly logging failure warnings */
    private boolean failing = false;

    public AsyncIndexUpdate(
            @Nonnull String name,
            @Nonnull NodeStore store,
            @Nonnull IndexEditorProvider provider) {
        this.name = checkNotNull(name);
        this.store = checkNotNull(store);
        this.provider = checkNotNull(provider);
    }

    @Override
    public synchronized void run() {
        log.debug("Running background index task {}", name);
        NodeStoreBranch branch = store.branch();
        NodeBuilder builder = branch.getHead().builder();
        NodeBuilder async = builder.child(ASYNC);

        NodeState before = null;
        final PropertyState state = async.getProperty(name);
        if (state != null && state.getType() == STRING) {
            before = store.retrieve(state.getValue(STRING));
        }
        if (before == null) {
            before = MISSING_NODE;
        }

        String checkpoint = store.checkpoint(lifetime);
        NodeState after = store.retrieve(checkpoint);
        if (after != null) {
            CommitFailedException exception = EditorDiff.process(
                    new IndexUpdate(provider, name, after, builder),
                    before, after);
            if (exception == null) {
                try {
                    async.setProperty(name, checkpoint);
                    branch.setRoot(builder.getNodeState());
                    branch.merge(new CommitHook() {
                        @Override @Nonnull
                        public NodeState processCommit(
                                NodeState before, NodeState after)
                                throws CommitFailedException {
                            // check for concurrent updates by this async task
                            PropertyState stateAfterRebase =
                                    before.getChildNode(ASYNC).getProperty(name);
                            if (Objects.equal(state, stateAfterRebase)) {
                                return after;
                            } else {
                                throw CONCURRENT_UPDATE;
                            }
                        }
                    });
                } catch (CommitFailedException e) {
                    if (e != CONCURRENT_UPDATE) {
                        exception = e;
                    }
                }
            }

            if (exception != null) {
                if (!failing) {
                    log.warn("Index update " + name + " failed", exception);
                }
                failing = true;
            } else {
                if (failing) {
                    log.info("Index update " + name + " no longer fails");
                }
                failing = false;
            }
        }
    }

}
