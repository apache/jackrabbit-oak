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

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import static org.junit.Assert.assertTrue;

/**
 * Utility class for tests
 */
public class TestUtils
        implements SolrServerProvider, OakSolrConfigurationProvider {

    static final String SOLR_HOME_PATH = "/solr";

    public static SolrServer createSolrServer() {
        String homePath = SolrServerProvider.class.getResource(SOLR_HOME_PATH).getFile();
        CoreContainer coreContainer = new CoreContainer(homePath);
        try {
            coreContainer.load();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new EmbeddedSolrServer(coreContainer, "oak");
    }

    public static void cleanDataDir() {
        String path = TestUtils.class.getResource("/solr/oak/data").getFile();
        File file = new File(path);
        if (file.exists()) {
            assertTrue(file.delete());
        }
    }

    public static OakSolrConfiguration getTestConfiguration() {
        return new DefaultSolrConfiguration() {
            @Override
            public CommitPolicy getCommitPolicy() {
                return CommitPolicy.HARD;
            }
        };
    }

    private final SolrServer solrServer = createSolrServer();

    private final OakSolrConfiguration configuration = getTestConfiguration();

    @Override
    public SolrServer getSolrServer() {
        return solrServer;
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        return configuration;
    }

}
