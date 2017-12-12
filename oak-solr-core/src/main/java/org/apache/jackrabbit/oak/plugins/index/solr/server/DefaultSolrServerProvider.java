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
package org.apache.jackrabbit.oak.plugins.index.solr.server;

import java.io.IOException;
import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class DefaultSolrServerProvider implements SolrServerProvider {

    private SolrClient solrServer;
    private SolrClient indexingSolrServer;

    @CheckForNull
    @Override
    public SolrClient getSolrServer() throws Exception {
        if (solrServer == null) {
            solrServer = new HttpSolrServer(getUrl());
        }
        return solrServer;
    }

    @CheckForNull
    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        if (indexingSolrServer == null) {
            indexingSolrServer = new ConcurrentUpdateSolrServer(getUrl(), 1000, 4);
        }
        return indexingSolrServer;
    }

    @CheckForNull
    @Override
    public SolrClient getSearchingSolrServer() throws Exception {
        return getSolrServer();
    }

    private String getUrl() {
        return SolrServerConfigurationDefaults.LOCAL_BASE_URL + ':' +
                SolrServerConfigurationDefaults.HTTP_PORT + SolrServerConfigurationDefaults.CONTEXT +
                '/' + SolrServerConfigurationDefaults.CORE_NAME;
    }

    @Override
    public void close() throws IOException {
        try {
            SolrClient solrServer = getSolrServer();
            if (solrServer != null) {
                solrServer.shutdown();
            }
        } catch (Exception e) {
            // do nothing
        } try {
            SolrClient indexingSolrServer = getIndexingSolrServer();
            if (indexingSolrServer != null) {
                indexingSolrServer.shutdown();
            }
        } catch (Exception e) {
            // do nothing
        } try {
            getSearchingSolrServer().shutdown();
        } catch (Exception e) {
            // do nothing
        }
    }
}
