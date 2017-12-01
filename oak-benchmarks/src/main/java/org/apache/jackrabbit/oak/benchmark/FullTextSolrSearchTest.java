/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullTextSolrSearchTest extends FullTextSearchTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private SolrServerProvider serverProvider;
    private String server;

    public FullTextSolrSearchTest(File dump, boolean flat, boolean doReport, Boolean storageEnabled, String server) {
        super(dump, flat, doReport, storageEnabled);
        this.server = server;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        initializeProvider();
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    OakSolrConfigurationProvider configurationProvider = new OakSolrConfigurationProvider() {
                        @Nonnull
                        public OakSolrConfiguration getConfiguration() {
                            return new DefaultSolrConfiguration() {
                                @Override
                                public int getRows() {
                                    return 50;
                                }
                            };
                        }
                    };
                    oak.with(new SolrQueryIndexProvider(serverProvider, configurationProvider))
                        .with(new NodeStateSolrServersObserver())
                        .with(new SolrIndexEditorProvider(serverProvider, configurationProvider))
                        .with(new SolrIndexInitializer(false));
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    private void initializeProvider() throws Exception {
        if (server == null || "default".equals(server)) {
            log.info("spawning Solr locally");
            serverProvider = createEmbeddedSolrServerProvider(true);
        } else if (server != null && "embedded".equals(server)) {
            log.info("using embedded Solr");
            serverProvider = createEmbeddedSolrServerProvider(false);
        } else if (server != null && (server.startsWith("http") || server.matches("\\w+\\:\\d{3,5}"))) {
            log.info("using remote Solr {}", server);
            RemoteSolrServerConfiguration remoteSolrServerConfiguration = new RemoteSolrServerConfiguration(
                    server, "oak", 2, 2, null, 10, 10, server);
            serverProvider = remoteSolrServerConfiguration.getProvider();
        } else {
            throw new IllegalArgumentException("server parameter value must be either 'embedded', 'default', an URL or an host:port String");
        }
    }

    private EmbeddedSolrServerProvider createEmbeddedSolrServerProvider(boolean http) throws Exception {
        String tempDirectoryPath = FileUtils.getTempDirectoryPath();
        File solrHome = new File(tempDirectoryPath, "solr" + System.nanoTime());
        EmbeddedSolrServerConfiguration embeddedSolrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome.getAbsolutePath(), "oak");
        if (http) {
            embeddedSolrServerConfiguration = embeddedSolrServerConfiguration.withHttpConfiguration("/solr", 8983);
        }
        EmbeddedSolrServerProvider embeddedSolrServerProvider = embeddedSolrServerConfiguration.getProvider();
        SolrClient solrServer = embeddedSolrServerProvider.getSolrServer();
        if (storageEnabled != null && !storageEnabled) {
            // change schema.xml and reload the core
            File schemaXML = new File(solrHome.getAbsolutePath() + "/oak/conf", "schema.xml");
            InputStream inputStream = getClass().getResourceAsStream("/solr/oak/conf/schema.xml");
            String schemaString = IOUtils.toString(inputStream).replace("<dynamicField name=\"*\" type=\"text_general\" indexed=\"true\" stored=\"true\" multiValued=\"true\"/>",
                    "<dynamicField name=\"*\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>");
            FileOutputStream fileOutputStream = new FileOutputStream(schemaXML);
            IOUtils.copy(new StringReader(schemaString), fileOutputStream);
            fileOutputStream.flush();
            ((EmbeddedSolrServer) solrServer).getCoreContainer().reload("oak");
        }
        return embeddedSolrServerProvider;
    }

    @Override
    protected void afterSuite() throws Exception {
        SolrClient solrServer = serverProvider.getSolrServer();
        if (solrServer != null) {
            solrServer.shutdown();
        }
    }
}
