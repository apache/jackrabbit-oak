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
package org.apache.jackrabbit.oak.plugins.index.solr;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;
import org.junit.After;
import org.junit.Before;

/**
 * Test base class for Oak-Solr
 */
public abstract class SolrBaseTest {

    protected NodeStore store;
    protected TestUtils provider;
    protected SolrClient server;
    protected OakSolrConfiguration configuration;
    protected EditorHook hook;
    private ContentRepository repository;

    @Before
    public void setUp() throws Exception {
        store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        provider = new TestUtils();
        server = provider.getSolrServer();
        configuration = provider.getConfiguration();
        hook = new EditorHook(new IndexUpdateProvider(
                new SolrIndexEditorProvider(provider, provider)));
        Oak oak = new Oak().with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new SolrIndexInitializer(false)) // synchronous
                .with(new SolrQueryIndexProvider(provider, provider))
                .with(new NodeStateSolrServersObserver())
                .with(new SolrIndexEditorProvider(provider, provider));
        repository = oak
                .createContentRepository();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null && server.ping() != null) {
            server.deleteByQuery("*:*");
            server.commit();
            server = null;
        }
    }

    protected Root createRoot() throws LoginException, NoSuchWorkspaceException {
        return repository.login(null, null).getLatestRoot();
    }

}