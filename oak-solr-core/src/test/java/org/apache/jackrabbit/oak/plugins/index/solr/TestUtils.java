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
package org.apache.jackrabbit.oak.plugins.index.solr;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexDiff;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrNodeStateConfiguration;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import com.google.common.collect.ImmutableList;

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.buildIndexDefinitions;

/**
 * Utility class for tests
 */
public class TestUtils {

    static final String SOLR_HOME_PATH = "target/test-classes/solr";
    static final String SOLRCONFIG_PATH = "target/test-classes/solr/solr.xml";

    public static QueryIndexProvider getTestQueryIndexProvider(final SolrServer solrServer, final OakSolrConfiguration configuration) {
        return new QueryIndexProvider() {
            @Nonnull
            @Override
            public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
                List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
                for (IndexDefinition child : buildIndexDefinitions(nodeState, "/",
                        SolrQueryIndex.TYPE)) {
                    try {
                        tempIndexes.add(new SolrQueryIndex(child, solrServer, configuration));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return tempIndexes;
            }
        };
    }

    public static IndexHookProvider getTestIndexHookProvider(final SolrServer solrServer, final OakSolrConfiguration configuration) {
        return new IndexHookProvider() {
            @Nonnull
            @Override
            public List<? extends IndexHook> getIndexHooks(String s, NodeBuilder nodeBuilder) {
                if (SolrQueryIndex.TYPE.equals(s)) {
                    try {
                        IndexHook indexHook = new SolrIndexDiff(nodeBuilder, solrServer, configuration);
                        return ImmutableList.of(indexHook);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ImmutableList.of();
            }
        };

    }

    public static SolrServer createSolrServer() throws Exception {
        CoreContainer coreContainer = new CoreContainer(SOLR_HOME_PATH);
        coreContainer.load(SOLR_HOME_PATH, new File(SOLRCONFIG_PATH));
        return new EmbeddedSolrServer(coreContainer, "oak");
    }


    public static OakSolrConfiguration getTestConfiguration(NodeState nodeState) {
        return new OakSolrNodeStateConfiguration(nodeState);
    }
}
