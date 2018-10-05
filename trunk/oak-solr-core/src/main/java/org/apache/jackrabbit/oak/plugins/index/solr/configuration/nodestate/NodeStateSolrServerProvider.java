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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate;

import java.io.IOException;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;

/**
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider} using configuration stored in a repository
 * node named "server" as a child of a {@code oak:queryIndexDefinition} node (e.g. under /../a/b/solrIndex/server)
 * having {@code type = solr}
 */
public class NodeStateSolrServerProvider implements SolrServerProvider {

    private final NodeState nodeState;
    private SolrServerProvider provider;

    public NodeStateSolrServerProvider(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    private void checkProviderInitialization() throws IllegalAccessException, java.lang.reflect.InvocationTargetException, InstantiationException {
        synchronized (nodeState) {
            if (provider == null) {
                this.provider = new NodeStateSolrServerConfigurationProvider(nodeState).getSolrServerConfiguration().getProvider();
            }
        }
    }

    @Override
    public SolrClient getSolrServer() throws Exception {
        checkProviderInitialization();
        return provider.getSolrServer();
    }

    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        checkProviderInitialization();
        return provider.getIndexingSolrServer();
    }

    @Override
    public SolrClient getSearchingSolrServer() throws Exception {
        checkProviderInitialization();
        return provider.getSearchingSolrServer();
    }

    @Override
    public void close() throws IOException {
        try {
            getSolrServer().shutdown();
        } catch (Exception e) {
            // do nothing
        } try {
            getIndexingSolrServer().shutdown();
        } catch (Exception e) {
            // do nothing
        } try {
            getSearchingSolrServer().shutdown();
        } catch (Exception e) {
            // do nothing
        }
    }

    @Override
    public String toString() {
        return "NodeStateSolrServerProvider{" +
                "nodeStateChildren=" + Iterables.toString(nodeState.getChildNodeNames()) +
                ", provider=" + provider +
                '}';
    }
}
