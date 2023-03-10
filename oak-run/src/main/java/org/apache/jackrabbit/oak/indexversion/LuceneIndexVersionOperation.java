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
package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LuceneIndexVersionOperation extends IndexVersionOperation {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexVersionOperation.class);

    public LuceneIndexVersionOperation(IndexName indexName) {
        super(indexName);
    }

    @Override
    protected IndexVersionOperation getIndexVersionOperationInstance(IndexName indexName) {
        return new LuceneIndexVersionOperation(indexName);
    }

    @Override
    protected boolean checkIfDisabledIndexCanBeMarkedForDeletion(NodeState indexNode) {
        // check if index node exists in read-only repo, if not, return true (and disabled index can be fully deleted), otherwise return false (and no action to be performed).
        return !isHiddenOakMountExists(indexNode) ? true : false;
    }

    @Override
    protected IndexName getActiveIndex(List<IndexName> reverseSortedIndexNameList, String parentPath, NodeState rootNode) {
        // iterate all indexes from high version to lower version to find the active index, then remove it from the reverseSortedIndexNameList
        for (int i = 0; i < reverseSortedIndexNameList.size(); i++) {
            IndexName indexNameObject = reverseSortedIndexNameList.get(i);
            String indexName = indexNameObject.getNodeName();
            String indexPath = PathUtils.concat(parentPath, PathUtils.getName(indexName));
            if (IndexName.isIndexActive(indexPath, rootNode)) {
                LOG.info("Found active index '{}'", indexPath);
                reverseSortedIndexNameList.remove(i);
                return indexNameObject;
            } else {
                LOG.info("The index '{}' isn't active", indexPath);
            }
        }
        return null;
    }
}
