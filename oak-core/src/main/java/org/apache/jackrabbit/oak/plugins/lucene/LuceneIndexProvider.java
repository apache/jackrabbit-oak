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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider for Lucene indexes.
 */
public class LuceneIndexProvider implements QueryIndexProvider,
        LuceneIndexConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneIndexProvider.class);

    private final String indexPath;

    public LuceneIndexProvider(String indexPath) {
        this.indexPath = indexPath;
    }

    @Override
    public List<QueryIndex> getQueryIndexes(NodeStore store) {
        if (!PathUtils.isValid(indexPath)) {
            LOG.warn("index path is not valid {}", indexPath);
            return Collections.<QueryIndex> emptyList();
        }
        NodeBuilder builder = IndexUtils.getChildBuilder(store, indexPath);
        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        for (IndexDefinition child : IndexUtils.buildIndexDefinitions(
                store.getRoot(), indexPath, TYPE)) {
            // add :data node
            builder.getChildBuilder(child.getName()).getChildBuilder(
                    INDEX_DATA_CHILD_NAME);
            tempIndexes.add(new LuceneIndex(store, child));
        }
        return tempIndexes;
    }
}
