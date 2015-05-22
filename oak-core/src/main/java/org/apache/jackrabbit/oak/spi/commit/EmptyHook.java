/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Basic commit hook implementation that by default doesn't do anything.
 * This class has a dual purpose:
 * <ol>
 * <li>The static {@link #INSTANCE} instance can be used as a "null object"
 * in cases where another commit hook has not been configured, thus avoiding
 * the need for extra code for such cases.</li>
 * <li>Other commit hook implementations can extend this class and gain
 * improved forwards-compatibility to possible changes in the
 * {@link CommitHook} interface. For example if it is later decided that
 * new arguments are needed in the hook methods, this class is guaranteed
 * to implement any new method signatures in a way that falls gracefully
 * back to any earlier behavior.</li>
 * </ol>
 */
public class EmptyHook implements CommitHook {

    /**
     * Static instance of this class, useful as a "null object".
     */
    public static final CommitHook INSTANCE = new EmptyHook();

    @Nonnull
    @Override
    public NodeState processCommit(
            NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        return after;
    }

}
