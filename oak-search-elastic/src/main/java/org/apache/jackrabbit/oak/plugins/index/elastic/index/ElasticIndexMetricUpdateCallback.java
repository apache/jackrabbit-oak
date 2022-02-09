/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PropertyUpdateCallback} implementation to get notifications from the indexing process and update the relevant
 * metrics.
 */
public class ElasticIndexMetricUpdateCallback implements PropertyUpdateCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexMetricUpdateCallback.class);

    private final String indexPath;

    private final ElasticIndexTracker indexTracker;

    public ElasticIndexMetricUpdateCallback(String indexPath, ElasticIndexTracker indexTracker) {
        this.indexPath = indexPath;
        this.indexTracker = indexTracker;
    }

    @Override
    public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd,
                                @Nullable PropertyState before, @Nullable PropertyState after) {
        // do nothing
    }

    @Override
    public void done() {
        ElasticIndexNode indexNode = indexTracker.acquireIndexNode(indexPath);
        if (indexNode != null) {
            try {
                String indexName = indexNode.getDefinition().getIndexFullName();
                if (indexName != null) {
                    indexTracker.getElasticMetricHandler().markDocuments(indexName, indexNode.getIndexStatistics().numDocs());
                    indexTracker.getElasticMetricHandler().markSize(indexName, indexNode.getIndexStatistics().size());
                }
            } catch (Exception e) {
                LOG.warn("Unable to store metrics for {}", indexNode.getDefinition().getIndexPath(), e);
            } finally {
                indexNode.release();
            }
        }
    }
}
