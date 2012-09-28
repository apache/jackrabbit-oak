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
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Composite commit hook. Maintains a list of component hooks and takes
 * care of calling them in proper sequence.
 */
public class CompositeHook implements CommitHook {

    private final List<CommitHook> hooks;

    public CompositeHook(List<CommitHook> hooks) {
        this.hooks = hooks;
    }

    public CompositeHook(CommitHook... hooks) {
        this(Arrays.asList(hooks));
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeState newState = after;
        for (CommitHook hook : hooks) {
            newState = hook.processCommit(before, newState);
        }
        return newState;
    }

}
