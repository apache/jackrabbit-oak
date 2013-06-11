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

import javax.security.auth.Subject;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.junit.After;
import org.junit.Before;

/**
 * Test base class for Oak-Solr
 */
public abstract class SolrBaseTest {

    protected KernelNodeStore store;
    protected NodeState state;
    protected TestUtils provider;
    protected SolrServer server;
    protected OakSolrConfiguration configuration;
    protected EditorHook hook;

    @Before
    public void setUp() throws Exception {
        MicroKernel microKernel = new MicroKernelImpl();
        store = new KernelNodeStore(microKernel);
        state = createInitialState(microKernel);
        provider = new TestUtils();
        server = provider.getSolrServer();
        configuration = provider.getConfiguration();
        hook = new EditorHook(new IndexUpdateProvider(
                new SolrIndexEditorProvider(provider, provider)));
    }

    @After
    public void tearDown() throws Exception {
        if (server != null && server.ping() != null) {
            server.deleteByQuery("*:*");
            server.commit();
            server = null;
        }
    }

    protected RootImpl createRootImpl() {
        return new RootImpl(store, hook, PostCommitHook.EMPTY, "solr-query-engine-it", new Subject(),
                new OpenSecurityProvider(), new CompositeQueryIndexProvider());
    }

    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{} +\"y\":{} +\"z\":{} " +
                "+\"oak:index\":{\"solr\":{\"type\":\"solr\"}}";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        return store.getRoot();
    }

}
