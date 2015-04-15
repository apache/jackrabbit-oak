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

import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;

/**
 * Configuration parameters for starting a {@link org.apache.solr.client.solrj.SolrServer}
 */
public class EmbeddedSolrServerConfiguration extends SolrServerConfiguration<EmbeddedSolrServerProvider> {

    private final String solrHomePath;
    private final String coreName;
    private HttpConfiguration httpConfiguration;

    public EmbeddedSolrServerConfiguration(String solrHomePath, String coreName) {
        this.solrHomePath = solrHomePath;
        this.coreName = coreName;
    }

    public EmbeddedSolrServerConfiguration withHttpConfiguration(String context, Integer httpPort) {
        if (context != null && context.length() > 0 && httpPort != null && httpPort > 0) {
            this.httpConfiguration = new HttpConfiguration(context, httpPort);
        }
        return this;
    }

    /**
     * get the Solr home path where all the configuration files are stored
     *
     * @return a <code>String</code> representing a path to the Solr home.
     */
    public String getSolrHomePath() {
        return solrHomePath;
    }

    /**
     * get the default core name to use for the Solr server
     *
     * @return a <code>String</code> representing the core name
     */
    public String getCoreName() {
        return coreName;
    }

    /**
     * get the {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration.HttpConfiguration} holding parameters for enabling Solr
     * server with HTTP bindings
     *
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration.HttpConfiguration} or <code>null</code> if not set
     */
    public HttpConfiguration getHttpConfiguration() {
        return httpConfiguration;
    }

    public static class HttpConfiguration {
        private final String context;
        private final Integer httpPort;

        HttpConfiguration(String context, Integer httpPort) {
            this.context = context;
            this.httpPort = httpPort;
        }

        public String getContext() {
            return context;
        }

        public Integer getHttpPort() {
            return httpPort;
        }

        @Override
        public String toString() {
            return "HttpConfiguration{" +
                    "context='" + context + '\'' +
                    ", httpPort=" + httpPort +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "EmbeddedSolrServerConfiguration{" +
                "solrHomePath='" + solrHomePath + '\'' +
                ", coreName='" + coreName + '\'' +
                ", httpConfiguration=" + httpConfiguration +
                '}';
    }
}
