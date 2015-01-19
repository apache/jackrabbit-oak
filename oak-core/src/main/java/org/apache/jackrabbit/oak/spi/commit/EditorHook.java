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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This commit hook implementation processes changes to be committed
 * using the {@link Editor} instance provided by the {@link EditorProvider}
 * passed to the constructor.
 *
 * @since Oak 0.7
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#Commit_editors"
 *         >Commit editors</a>
 */
public class EditorHook implements CommitHook {

    private final EditorProvider provider;

    public EditorHook(@Nonnull EditorProvider provider) {
        this.provider = checkNotNull(provider);
    }

    @Override @Nonnull
    public NodeState processCommit(
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull CommitInfo info) throws CommitFailedException {
        checkNotNull(before);
        checkNotNull(after);
        checkNotNull(info);

        NodeBuilder builder = after.builder();
        Editor editor = provider.getRootEditor(before, after, builder, info);
        CommitFailedException exception =
                EditorDiff.process(editor, before, after);
        if (exception == null) {
            return builder.getNodeState();
        } else {
            throw exception;
        }
    }

    @Override
    public String toString() {
        return "EditorHook : (" + provider + ")";
    }
}
