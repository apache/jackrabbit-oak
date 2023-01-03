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
package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class AsyncIndexerLucene extends AsyncIndexerBase {

    private static final Logger log = LoggerFactory.getLogger(AsyncIndexerLucene.class);
    private final ExtendedIndexHelper extendedIndexHelper;
    private final boolean enableCowCor;
    public AsyncIndexerLucene(ExtendedIndexHelper extendedIndexHelper, boolean enableCowCor, Closer close, List<String> names, long delay) {
        super(extendedIndexHelper, close, names, delay);
        this.extendedIndexHelper = extendedIndexHelper;
        this.enableCowCor = enableCowCor;
    }

    @Override
    public IndexEditorProvider getIndexEditorProvider() {
        try {
            return CompositeIndexEditorProvider
                    .compose(Arrays.asList(createLuceneEditorProvider(), new NodeCounterEditorProvider()));
        } catch (IOException e) {
            log.error("Exception while initializing IndexEditorProvider", e);
            return null;
        }
    }

    private IndexEditorProvider createLuceneEditorProvider() throws IOException {
        LuceneIndexEditorProvider provider;
        if (enableCowCor) {
            LuceneIndexHelper luceneIndexHelper = extendedIndexHelper.getLuceneIndexHelper();
            provider = luceneIndexHelper.createEditorProvider();
            provider.setWriterConfig(luceneIndexHelper.getWriterConfigForReindex());
        } else {
            provider = new LuceneIndexEditorProvider();
        }
        return provider;
    }

}
