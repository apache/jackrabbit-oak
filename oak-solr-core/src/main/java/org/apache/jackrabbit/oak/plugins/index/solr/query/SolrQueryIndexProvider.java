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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@link QueryIndexProvider} for {@link SolrQueryIndex}
 */
public class SolrQueryIndexProvider implements QueryIndexProvider {

    private final SolrServerProvider solrServerProvider;

    private final OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private final QueryIndex.NodeAggregator aggregator;

    public SolrQueryIndexProvider(@Nonnull SolrServerProvider solrServerProvider, @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider,
                                  @Nullable QueryIndex.NodeAggregator nodeAggregator) {
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.solrServerProvider = solrServerProvider;
        this.aggregator = nodeAggregator;
    }

    public SolrQueryIndexProvider(@Nonnull SolrServerProvider solrServerProvider, @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this(solrServerProvider, oakSolrConfigurationProvider, null);
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return ImmutableList.of(new SolrQueryIndex(aggregator, oakSolrConfigurationProvider, solrServerProvider));
    }

}
