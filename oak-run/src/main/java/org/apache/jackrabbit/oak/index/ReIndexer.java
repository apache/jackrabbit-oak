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

package org.apache.jackrabbit.oak.index;

import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

class ReIndexer {
    private final IndexHelper indexHelper;

    public ReIndexer(IndexHelper indexHelper) {
        this.indexHelper = checkNotNull(indexHelper);
    }

    public void reindex() throws IOException, CommitFailedException {
        new SimpleAsyncReindexer(indexHelper, indexHelper.getIndexPaths(), createIndexEditorProvider()).reindex();
    }

    private IndexEditorProvider createIndexEditorProvider() throws IOException {
        //Need to list all used editors otherwise async index run fails with
        //MissingIndexEditor exception. Better approach would be to change lane for
        //those indexes and then do reindexing
        NodeCounterEditorProvider counter = new NodeCounterEditorProvider();
        IndexEditorProvider lucene = indexHelper.getLuceneIndexHelper().createEditorProvider();
        IndexEditorProvider property = new PropertyIndexEditorProvider().with(indexHelper.getMountInfoProvider());

        return CompositeIndexEditorProvider.compose(asList(lucene, property, counter));
    }
}
