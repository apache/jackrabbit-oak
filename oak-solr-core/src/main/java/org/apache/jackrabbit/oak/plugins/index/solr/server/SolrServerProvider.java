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

import javax.annotation.CheckForNull;

import org.apache.solr.client.solrj.SolrServer;

/**
 * Provider of {@link org.apache.solr.client.solrj.SolrServer}s instances
 */
public interface SolrServerProvider {

    /**
     * provides an already initialized {@link org.apache.solr.client.solrj.SolrServer} to be used for either searching or
     * indexing, or both.
     *
     * @return a {@link org.apache.solr.client.solrj.SolrServer} instance
     * @throws Exception if anything goes wrong while initializing the {@link org.apache.solr.client.solrj.SolrServer}
     */
    SolrServer getSolrServer() throws Exception;

    /**
     * provides an already initialized {@link org.apache.solr.client.solrj.SolrServer} specifically configured to be
     * used for indexing.
     *
     * @return a {@link org.apache.solr.client.solrj.SolrServer} instance
     * @throws Exception if anything goes wrong while initializing the {@link org.apache.solr.client.solrj.SolrServer}
     */
    @CheckForNull
    SolrServer getIndexingSolrServer() throws Exception;

    /**
     * provides an already initialized {@link org.apache.solr.client.solrj.SolrServer} specifically configured to be
     * used for searching.
     *
     * @return a {@link org.apache.solr.client.solrj.SolrServer} instance
     * @throws Exception if anything goes wrong while initializing the {@link org.apache.solr.client.solrj.SolrServer}
     */
    @CheckForNull
    SolrServer getSearchingSolrServer() throws Exception;
}
