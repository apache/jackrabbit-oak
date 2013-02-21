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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Provider interface for {@link SolrCommitHook}s and {@link SolrIndexHook}s
 */
public interface SolrHookFactory {

    /**
     * create a {@link SolrIndexHook} to index data on a Solr server
     *
     * @param path the path the created {@link SolrIndexHook} should work on
     * @param builder the {@link NodeBuilder} to get {@link org.apache.jackrabbit.oak.spi.state.NodeState}s
     * @return the created {@link IndexHook}
     * @throws Exception if any failres happen during the hook creation
     */
    public IndexHook createIndexHook(String path, NodeBuilder builder) throws Exception;

    /**
     * create a {@link SolrCommitHook} to index data on a Solr server
     *
     * @return the created {@link SolrCommitHook}
     * @throws Exception if any failres happen during the hook creation
     */
    public CommitHook createCommitHook() throws Exception;

}
