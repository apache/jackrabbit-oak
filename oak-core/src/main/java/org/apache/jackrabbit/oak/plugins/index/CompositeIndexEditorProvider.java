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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Aggregation of a list of editor providers into a single provider.
 */
public class CompositeIndexEditorProvider implements IndexEditorProvider {

    @Nonnull
    public static IndexEditorProvider compose(
            @Nonnull Collection<IndexEditorProvider> providers) {
        if (providers.isEmpty()) {
            return new IndexEditorProvider() {
                @Override
                public Editor getIndexEditor(
                        @Nonnull String type, @Nonnull NodeBuilder builder, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback) {
                    return null;
                }
            };
        } else if (providers.size() == 1) {
            return providers.iterator().next();
        } else {
            return new CompositeIndexEditorProvider(
                    ImmutableList.copyOf(providers));
        }
    }

    private final List<IndexEditorProvider> providers;

    private CompositeIndexEditorProvider(List<IndexEditorProvider> providers) {
        this.providers = providers;
    }

    public CompositeIndexEditorProvider(IndexEditorProvider... providers) {
        this(Arrays.asList(providers));
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder builder, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        List<Editor> indexes = Lists.newArrayList();
        for (IndexEditorProvider provider : providers) {
            Editor e = provider.getIndexEditor(type, builder, root, callback);
            if (e != null) {
                indexes.add(e);
            }
        }
        return CompositeEditor.compose(indexes);
    }
}
