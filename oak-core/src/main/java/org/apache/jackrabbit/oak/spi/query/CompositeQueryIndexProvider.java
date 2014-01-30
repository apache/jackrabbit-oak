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
package org.apache.jackrabbit.oak.spi.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This {@code QueryIndexProvider} aggregates a list of query index providers
 * into a single query index provider.
 */
public class CompositeQueryIndexProvider implements QueryIndexProvider {

    private final List<QueryIndexProvider> providers;

    private CompositeQueryIndexProvider(List<QueryIndexProvider> providers) {
        this.providers = providers;
    }

    public CompositeQueryIndexProvider(QueryIndexProvider... providers) {
        this(Arrays.asList(providers));
    }

    @Nonnull
    public static QueryIndexProvider compose(
            @Nonnull Collection<QueryIndexProvider> providers) {
        if (providers.isEmpty()) {
            return new QueryIndexProvider() {
                @Override
                public List<QueryIndex> getQueryIndexes(NodeState nodeState) {
                    return ImmutableList.of();
                }
            };
        } else if (providers.size() == 1) {
            return providers.iterator().next();
        } else {
            return new CompositeQueryIndexProvider(
                    ImmutableList.copyOf(providers));
        }
    }

    @Override @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<QueryIndex> indexes = Lists.newArrayList();
        for (QueryIndexProvider provider : providers) {
            indexes.addAll(provider.getQueryIndexes(nodeState));
        }
        return indexes;
    }
    
    @Override
    public String toString() {
        return getClass().getName() + ": " + providers.toString();
    }

}
