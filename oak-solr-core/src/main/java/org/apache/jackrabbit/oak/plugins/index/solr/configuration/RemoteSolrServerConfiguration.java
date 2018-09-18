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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.index.solr.server.RemoteSolrServerProvider;

/**
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration} for the {@link org.apache.jackrabbit.oak.plugins.index.solr.server.RemoteSolrServerProvider}
 */
public class RemoteSolrServerConfiguration extends SolrServerConfiguration<RemoteSolrServerProvider> {

    private final String solrConfDir;
    private final int socketTimeout;
    private final int connectionTimeout;
    private final String[] solrHttpUrls;
    private final String solrZkHost;
    private final String solrCollection;
    private final int solrShardsNo;
    private final int solrReplicationFactor;

    public RemoteSolrServerConfiguration(String solrZkHost, String solrCollection, int solrShardsNo, int solrReplicationFactor,
                                         String solrConfDir, int socketTimeout, int connectionTimeout, String... solrHttpUrls) {
        this.socketTimeout = socketTimeout;
        this.connectionTimeout = connectionTimeout;
        this.solrHttpUrls = solrHttpUrls;
        this.solrZkHost = solrZkHost;
        this.solrCollection = solrCollection;
        this.solrShardsNo = solrShardsNo;
        this.solrConfDir = solrConfDir;
        this.solrReplicationFactor = solrReplicationFactor;
    }

    public String[] getSolrHttpUrls() {
        return solrHttpUrls;
    }

    public String getSolrZkHost() {
        return solrZkHost;
    }

    public String getSolrCollection() {
        return solrCollection;
    }

    public int getSolrShardsNo() {
        return solrShardsNo;
    }

    public int getSolrReplicationFactor() {
        return solrReplicationFactor;
    }

    public String getSolrConfDir() {
        return solrConfDir;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    @Override
    public String toString() {
        return "RemoteSolrServerConfiguration{" +
                "solrConfDir='" + solrConfDir + '\'' +
                ", socketTimeout=" + socketTimeout +
                ", connectionTimeout=" + connectionTimeout +
                ", solrHttpUrls=" + Arrays.toString(solrHttpUrls) +
                ", solrZkHost='" + solrZkHost + '\'' +
                ", solrCollection='" + solrCollection + '\'' +
                ", solrShardsNo=" + solrShardsNo +
                ", solrReplicationFactor=" + solrReplicationFactor +
                '}';
    }
}
