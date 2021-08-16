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
package org.apache.jackrabbit.oak.index.indexer.document;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class DocumentStoreIndexer extends DocumentStoreIndexerBase implements Closeable {

    private final ExtendedIndexHelper extendedIndexHelper;

    public DocumentStoreIndexer(ExtendedIndexHelper extendedIndexHelper, IndexerSupport indexerSupport) throws IOException {
        super(extendedIndexHelper, indexerSupport);
        this.extendedIndexHelper = extendedIndexHelper;
        setProviders();
    }

    private NodeStateIndexerProvider createLuceneIndexProvider() throws IOException {
        return new LuceneIndexerProvider(extendedIndexHelper, indexerSupport);
    }

    protected List<NodeStateIndexerProvider> createProviders() throws IOException {
        List<NodeStateIndexerProvider> providers = ImmutableList.of(
                createLuceneIndexProvider()
        );

        providers.forEach(closer::register);
        return providers;
    }

    @Override
    protected void preIndexOpertaions(List<NodeStateIndexer> indexers) {
        // NOOP
    }
}
