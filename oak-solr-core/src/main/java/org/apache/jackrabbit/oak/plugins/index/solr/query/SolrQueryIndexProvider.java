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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

/**
 * {@link QueryIndexProvider} for {@link SolrQueryIndex}
 */
public class SolrQueryIndexProvider implements QueryIndexProvider {

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndexProvider.class);

    private SolrServerProvider solrServerProvider;

    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    public SolrQueryIndexProvider(SolrServerProvider solrServerProvider, OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.solrServerProvider = solrServerProvider;
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {

        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        if (solrServerProvider == null || oakSolrConfigurationProvider == null) {
            return tempIndexes;
        }
        NodeState definitions = nodeState.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            PropertyState type = definition.getProperty(TYPE_PROPERTY_NAME);
            if (type != null
                    && SolrQueryIndex.TYPE.equals(type.getValue(Type.STRING))) {
                if (log.isDebugEnabled()) {
                    log.debug("found a Solr index definition {}", entry.getName());
                }
                try {
                    SolrServer solrServer = solrServerProvider.getSolrServer();
                    // the query engine should be returned only if the serve is alive, otherwise other indexes should be used
                    if (solrServer != null && 0 == solrServer.ping().getStatus()) {
                        tempIndexes.add(new SolrQueryIndex(
                                entry.getName(),
                                solrServer,
                                oakSolrConfigurationProvider.getConfiguration()));
                    }
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        log.error("unable to create Solr query index at " + entry.getName(), e);
                    }
                }

            }
        }
        return tempIndexes;
    }

}
