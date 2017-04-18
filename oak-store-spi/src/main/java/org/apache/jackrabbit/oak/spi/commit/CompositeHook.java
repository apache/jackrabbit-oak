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
package org.apache.jackrabbit.oak.spi.commit;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Composite commit hook. Maintains a list of component hooks and takes
 * care of calling them in proper sequence.
 */
public class CompositeHook implements CommitHook {

    public static CommitHook compose(@Nonnull Collection<CommitHook> hooks) {
        switch (hooks.size()) {
        case 0:
            return EmptyHook.INSTANCE;
        case 1:
            return hooks.iterator().next();
        default:
            return new CompositeHook(hooks);
        }
    }

    private final Collection<CommitHook> hooks;

    private CompositeHook(@Nonnull Collection<CommitHook> hooks) {
        this.hooks = hooks;
    }

    public CompositeHook(CommitHook... hooks) {
        this(Arrays.asList(hooks));
    }

    @Nonnull
    @Override
    public NodeState processCommit(
            NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        NodeState newState = after;
        for (CommitHook hook : hooks) {
            newState = hook.processCommit(before, newState, info);
        }
        return newState;
    }

}
