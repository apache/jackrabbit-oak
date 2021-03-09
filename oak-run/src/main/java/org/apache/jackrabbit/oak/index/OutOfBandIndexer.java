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

import java.io.File;
import java.io.IOException;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class OutOfBandIndexer extends OutOfBandIndexerBase {
    private final ExtendedIndexHelper extendedIndexHelper;

    //TODO Support for providing custom index definition i.e. where definition is not
    //present in target repository

    public OutOfBandIndexer(ExtendedIndexHelper extendedIndexHelper, IndexerSupport indexerSupport) {
        super(extendedIndexHelper,indexerSupport);
        this.extendedIndexHelper = checkNotNull(extendedIndexHelper);
    }

    protected IndexEditorProvider createIndexEditorProvider() throws IOException {
        IndexEditorProvider lucene = createLuceneEditorProvider();
        IndexEditorProvider property = createPropertyEditorProvider();

        return CompositeIndexEditorProvider.compose(asList(lucene, property));
    }

    private IndexEditorProvider createPropertyEditorProvider() throws IOException {
        SegmentPropertyIndexEditorProvider provider =
                new SegmentPropertyIndexEditorProvider(new File(getLocalIndexDir(), "propertyIndexStore"));
        provider.with(extendedIndexHelper.getMountInfoProvider());
        closer.register(provider);
        return provider;
    }

    private IndexEditorProvider createLuceneEditorProvider() throws IOException {
        LuceneIndexHelper luceneIndexHelper = extendedIndexHelper.getLuceneIndexHelper();
        DirectoryFactory dirFactory = new FSDirectoryFactory(getLocalIndexDir());
        luceneIndexHelper.setDirectoryFactory(dirFactory);
        LuceneIndexEditorProvider provider = luceneIndexHelper.createEditorProvider();
        provider.setWriterConfig(luceneIndexHelper.getWriterConfigForReindex());
        return provider;
    }

}
