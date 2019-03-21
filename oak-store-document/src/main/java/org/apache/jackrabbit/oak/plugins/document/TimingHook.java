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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A commit hook that measures the time it took to process the commit. This
 * hook only reports timing for successfully processed commits. Those that fail
 * with a {@link CommitFailedException} are not reported.
 */
class TimingHook implements CommitHook {

    private static final Logger LOG = LoggerFactory.getLogger(TimingHook.class);

    private final CommitHook hook;
    private final Listener listener;

    static CommitHook wrap(CommitHook hook, Listener listener) {
        return new TimingHook(hook, listener);
    }

    private TimingHook(@NotNull CommitHook hook,
                       @NotNull Listener listener) {
        this.hook = checkNotNull(hook);
        this.listener = checkNotNull(listener);
    }

    @Override
    public @NotNull NodeState processCommit(NodeState before,
                                            NodeState after,
                                            CommitInfo info)
            throws CommitFailedException {
        long start = System.nanoTime();
        NodeState state = hook.processCommit(before, after, info);
        processed(System.nanoTime() - start);
        return state;
    }

    private void processed(long timeNano) {
        try {
            listener.processed(timeNano, TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            LOG.warn("Listener failed with exception", t);
        }
    }

    interface Listener {

        void processed(long time, TimeUnit unit);
    }
}
