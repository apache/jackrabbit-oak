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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collection;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.IndexLookup;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

class LuceneIndexLookupUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexLookupUtil.class);

    static final Predicate<NodeState> LUCENE_INDEX_DEFINITION_PREDICATE =
            state -> TYPE_LUCENE.equals(state.getString(TYPE_PROPERTY_NAME));

    private LuceneIndexLookupUtil() {
    }

    /**
     * Returns the path of the first Lucene index node which supports
     * fulltext search
     */
    public static String getOldFullTextIndexPath(NodeState root, Filter filter, IndexTracker tracker) {
        Collection<String> indexPaths = getLuceneIndexLookup(root).collectIndexNodePaths(filter, false);
        for (String path : indexPaths) {
            NodeState node = NodeStateUtils.getNode(root, path);
            if (IndexDefinition.determineIndexFormatVersion(node) != IndexFormatVersion.V1) {
                // shortcut to avoid reading the index definition if not needed
                continue;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Old V1 lucene index found at {}", path);
            }
            IndexDefinition indexDefinition = tracker.getIndexDefinition(path);
            if (indexDefinition != null && indexDefinition.isFullTextEnabled()
                    && indexDefinition.getVersion() == IndexFormatVersion.V1) {
                return path;
            }
        }
        return null;
    }

    public static IndexLookup getLuceneIndexLookup(NodeState root) {
        return new IndexLookup(root, LUCENE_INDEX_DEFINITION_PREDICATE);
    }
}
