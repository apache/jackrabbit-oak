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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Default implementation of {@link SolrHookFactory}
 */
@Component
@Service(value = SolrHookFactory.class)
public class DefaultSolrHookFactory implements SolrHookFactory {

    @Reference
    private SolrServerProvider solrServerProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    @Override
    public IndexHook createIndexHook(String path, NodeBuilder builder) throws Exception {
        return new SolrIndexDiff(builder, solrServerProvider.getSolrServer(), oakSolrConfigurationProvider.getConfiguration());
    }

    @Override
    public CommitHook createCommitHook() throws Exception {
        return new SolrCommitHook(solrServerProvider.getSolrServer());
    }
}
