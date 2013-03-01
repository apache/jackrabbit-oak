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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.buildIndexDefinitions;

/**
 * {@link QueryIndexProvider} for {@link SolrQueryIndex}
 */
@Component
@Service(QueryIndexProvider.class)
public class SolrQueryIndexProvider implements QueryIndexProvider {

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndexProvider.class);

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private SolrServerProvider solrServerProvider;

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    public SolrQueryIndexProvider() {
    }

    public SolrQueryIndexProvider(SolrServerProvider solrServerProvider, OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.solrServerProvider = solrServerProvider;
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        for (IndexDefinition child : buildIndexDefinitions(nodeState, "/",
                SolrQueryIndex.TYPE)) {
            if (log.isDebugEnabled()) {
                log.debug("found a Solr index definition {}", child);
            }
            try {
                if (solrServerProvider != null && oakSolrConfigurationProvider != null) {
                    tempIndexes.add(new SolrQueryIndex(child, solrServerProvider.getSolrServer(), oakSolrConfigurationProvider.getConfiguration()));
                }
            } catch (Exception e) {
                log.error("unable to create Solr query index at {} due to {}", new Object[]{child.getPath(), e});
            }
        }
        return tempIndexes;
    }
}
