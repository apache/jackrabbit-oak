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
package org.apache.jackrabbit.oak.jcr;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;

import static org.junit.Assert.assertNotNull;

public class SolrOakRepositoryStub extends OakSegmentTarRepositoryStub {

    public SolrOakRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
    }

    @Override
    protected void preCreateRepository(Jcr jcr) {
        File f = new File("target" + File.separatorChar + "queryjcrtest-" + System.currentTimeMillis());
        final SolrClient solrServer;
        try {
            solrServer = new EmbeddedSolrServerProvider(new EmbeddedSolrServerConfiguration(f.getPath(), "oak")).getSolrServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SolrServerProvider solrServerProvider = new SolrServerProvider() {
            @Override
            public void close() throws IOException {

            }

            @CheckForNull
            @Override
            public SolrClient getSolrServer() throws Exception {
                return solrServer;
            }

            @Override
            public SolrClient getIndexingSolrServer() throws Exception {
                return solrServer;
            }

            @Override
            public SolrClient getSearchingSolrServer() throws Exception {
                return solrServer;
            }
        };
        try {
            assertNotNull(solrServer);
            // safely remove any previous document on the index
            solrServer.deleteByQuery("*:*");
            solrServer.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Nonnull
            @Override
            public CommitPolicy getCommitPolicy() {
                return CommitPolicy.HARD;
            }
        };
        OakSolrConfigurationProvider oakSolrConfigurationProvider = new DefaultSolrConfigurationProvider(configuration);
        jcr.with(new SolrIndexInitializer(false))
                .with(AggregateIndexProvider.wrap(new SolrQueryIndexProvider(solrServerProvider, oakSolrConfigurationProvider)))
                .with(new NodeStateSolrServersObserver())
                .with(new SolrIndexEditorProvider(solrServerProvider, oakSolrConfigurationProvider));
    }
}
