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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * TODO document
 */
public class CompositeIndexHookProvider implements IndexHookProvider {

    @Nonnull
    public static IndexHookProvider compose(
            @Nonnull Collection<IndexHookProvider> providers) {
        if (providers.isEmpty()) {
            return new IndexHookProvider() {
                @Override
                public List<? extends IndexHook> getIndexHooks(String type,
                        NodeBuilder builder) {
                    return ImmutableList.of();
                }
            };
        } else if (providers.size() == 1) {
            return providers.iterator().next();
        } else {
            return new CompositeIndexHookProvider(
                    ImmutableList.copyOf(providers));
        }
    }

    private final List<IndexHookProvider> providers;

    private CompositeIndexHookProvider(List<IndexHookProvider> providers) {
        this.providers = providers;
    }

    public CompositeIndexHookProvider(IndexHookProvider... providers) {
        this(Arrays.asList(providers));
    }

    @Override
    @Nonnull
    public List<? extends IndexHook> getIndexHooks(String type,
            NodeBuilder builder) {
        List<IndexHook> indexes = Lists.newArrayList();
        for (IndexHookProvider provider : providers) {
            indexes.addAll(provider.getIndexHooks(type, builder));
        }
        return indexes;
    }
}
