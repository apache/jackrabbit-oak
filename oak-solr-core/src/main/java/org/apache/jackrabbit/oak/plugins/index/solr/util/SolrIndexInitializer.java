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
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

/**
 * A {@link org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer} for Solr index
 */
public class SolrIndexInitializer implements RepositoryInitializer {

    private static final String SOLR_IDX = "solr";
    private static final String ASYNC = "async";

    private final String async;
    private final String name;
    private final boolean reindex;

    /**
     * build Solr index definition with defaults (name = solr, reindex enabled, asynchronous):
     */
    public SolrIndexInitializer() {
        this.name = SOLR_IDX;
        this.async = ASYNC;
        this.reindex = true;
    }

    /**
     * build Solr index definition with a specific node name and defaults (reindex enabled, asynchronous):
     *
     * @param name the name of the node holding the Solr index definition
     */
    public SolrIndexInitializer(String name) {
        this.name = name;
        this.async = ASYNC;
        this.reindex = true;
    }

    /**
     * build Solr index definition by specifying if it should be async or not
     *
     * @param async if <code>true</code> for the index to be asynchronous, <code>false</code> to make
     *              it synchronous
     */
    public SolrIndexInitializer(boolean async) {
        this.name = SOLR_IDX;
        this.async = async ? ASYNC : null;
        this.reindex = true;
    }

    /**
     * build Solr index definition by specifying all the configurable parameters
     *
     * @param async   if <code>true</code> for the index to be asynchronous, <code>false</code> to make
     *                it synchronous
     * @param name    the name of the node holding the Solr index definition
     * @param reindex <code>true</code> if the reindexing should be enabled
     */
    public SolrIndexInitializer(boolean async, String name, boolean reindex) {
        this.name = name;
        this.async = async ? ASYNC : null;
        this.reindex = reindex;
    }

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        if (builder.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
                && !builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME).hasChildNode(SOLR_IDX)) {
            NodeBuilder indexDefinitionsNode = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
            if (!indexDefinitionsNode.hasChildNode(name)) {
                NodeBuilder solrIndexDefinitionNode = indexDefinitionsNode.child(name);
                solrIndexDefinitionNode.setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME)
                        .setProperty(IndexConstants.TYPE_PROPERTY_NAME, SOLR_IDX)
                        .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, reindex);
                if (async != null) {
                    solrIndexDefinitionNode.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, async);
                }
            }

        }
    }

    public static boolean isSolrIndexNode(NodeState node) {
        return SolrQueryIndex.TYPE.equals(node.getString(TYPE_PROPERTY_NAME));
    }
}
