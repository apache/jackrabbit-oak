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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import java.util.List;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Service that provides {@link SolrIndexHookProvider} based {@link IndexHook}s.
 *
 * @see org.apache.jackrabbit.oak.plugins.index.IndexHookProvider
 */
@Component
@Service(IndexHookProvider.class)
public class SolrIndexHookProvider implements IndexHookProvider {

    private final Logger log = LoggerFactory.getLogger(SolrIndexHookProvider.class);

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private SolrServerProvider solrServerProvider;

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    public SolrIndexHookProvider() {
    }

    public SolrIndexHookProvider(SolrServerProvider solrServerProvider, OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this.solrServerProvider = solrServerProvider;
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
    }

    @Override
    @Nonnull
    public List<? extends IndexHook> getIndexHooks(
            String type, NodeBuilder builder, NodeState root) {
        if (SolrQueryIndex.TYPE.equals(type) && solrServerProvider != null && oakSolrConfigurationProvider != null) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Creating a Solr index hook");
                }
                IndexHook indexHook = new SolrIndexDiff(builder, solrServerProvider.getSolrServer(), oakSolrConfigurationProvider.getConfiguration());
                return ImmutableList.of(indexHook);
            } catch (Exception e) {
                log.error("unable to create Solr IndexHook ", e);
            }
        }
        return ImmutableList.of();
    }

}