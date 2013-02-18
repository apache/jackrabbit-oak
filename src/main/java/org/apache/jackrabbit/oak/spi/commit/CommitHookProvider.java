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

import javax.annotation.Nonnull;

/**
 * {@code CommitHookProvider} TODO
 * <p/>
 * FIXME: needs re-evaluation and review once we add support for multiple workspaces (OAK-118)
 */
public interface CommitHookProvider {

    /**
     * Create a new {@code CommitHook} to deal with modifications on the
     * workspace with the specified {@code workspaceName}.
     *
     * @param workspaceName The name of the workspace.
     * @return A CommitHook instance.
     */
    @Nonnull
    CommitHook getCommitHook(@Nonnull String workspaceName);

    /**
     * Default implementation that returns an {@code EmptyHook}.
     */
    final class Empty implements CommitHookProvider {

        @Override
        public CommitHook getCommitHook(String workspaceName) {
            return EmptyHook.INSTANCE;
        }
    }
}