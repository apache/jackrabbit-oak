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

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.List;

public class ElasticIndexVersionOperation extends IndexVersionOperation{
    public ElasticIndexVersionOperation(IndexName indexName) {
        super(indexName);
    }

    @Override
    protected IndexVersionOperation getIndexVersionOperationInstance(IndexName indexName) {
        return new ElasticIndexVersionOperation(indexName);
    }

    @Override
    protected boolean checkIfDisabledIndexCanBeMarkedForDeletion(NodeState indexNode) {
        // As of now there is no way to check for elastic if a disabled index can be safely deleted or not
        // since there is no hidden oak mount present as in the case of lucene.
        // So always return false so as to not delete any disabled base index by mistake which might be needed later on.
        return false;
    }

    @Override
    protected IndexName getActiveIndex(List<IndexName> reverseSortedIndexNameList, String parentPath, NodeState rootNode) {
        // ES doesn't have a concept of active/inactive index as of now (no hidden oak mount to detect this)
        // So here we simply return the highest versioned index
        return reverseSortedIndexNameList.remove(0);
    }
}
