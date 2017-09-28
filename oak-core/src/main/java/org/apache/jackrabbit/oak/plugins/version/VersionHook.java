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
package org.apache.jackrabbit.oak.plugins.version;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.annotations.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.spi.commit.CompositeHook.compose;

/**
 * This class gathers together editors related to handling version storage:
 * <ol>
 *     <li>
 *         {@link VersionEditorProvider}
 *         <ul>
 *             <li>
 *                 {@link VersionEditor} - creates version history, handles
 *                 checking-in, checking-out and restoring, prevents a
 *                 checked-in node from being modified,
 *             </li>
 *             <li>
 *                 {@link VersionStorageEditor} - validates changes on the
 *                 version storage,
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *         {@link VersionableCollector} - collects all existing versionable
 *         UUIDs, so assigned histories won't be removed in the next step,
 *     </li>
 *     <li>
 *         {@link OrphanedVersionCleaner} - removes all histories that are
 *         empty and have no longer a parent versionable node.
 *     </li>
 * </ol>
 *
 */
@Component(service = CommitHook.class)
public class VersionHook implements CommitHook {

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
        Set<String> existingVersionables = newHashSet();

        List<EditorProvider> providers = newArrayList();
        providers.add(new VersionEditorProvider());
        providers.add(new VersionableCollector.Provider(existingVersionables));
        providers.add(new OrphanedVersionCleaner.Provider(existingVersionables));

        return compose(transform(providers, new Function<EditorProvider, CommitHook>() {
            @Nullable
            @Override
            public CommitHook apply(@Nullable EditorProvider input) {
                return new EditorHook(input);
            }
        })).processCommit(before, after, info);
    }
}
