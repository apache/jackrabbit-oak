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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.RemoteSolrServerProvider;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration}
 */
public class SolrServerConfigurationTest {

    @Test
    public void testCreateRemoteServerFromConfig() throws Exception {
        SolrServerConfiguration<RemoteSolrServerProvider> remoteSolrServerProviderSolrServerConfiguration =
                new RemoteSolrServerConfiguration(null, null, 1, 1, null, 10, 10, null);
        RemoteSolrServerProvider remoteSolrServerProvider = remoteSolrServerProviderSolrServerConfiguration.getProvider();
        assertNotNull(remoteSolrServerProvider);
    }

    @Test
    public void testCreteEmbeddedServerFromConfig() throws Exception {
        SolrServerConfiguration<EmbeddedSolrServerProvider> embeddedSolrServerSolrServerConfiguration = new EmbeddedSolrServerConfiguration(null, null);
        EmbeddedSolrServerProvider embeddedSolrServerProvider = embeddedSolrServerSolrServerConfiguration.getProvider();
        assertNotNull(embeddedSolrServerProvider);
    }
}
