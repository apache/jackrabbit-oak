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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.datastore.DataStoreTextWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExtendedIndexHelper extends IndexHelper {

    private LuceneIndexHelper luceneIndexHelper;
    private ExtractedTextCache extractedTextCache;

    public ExtendedIndexHelper(NodeStore store, BlobStore blobStore, Whiteboard whiteboard,
                               File outputDir, File workDir, List<String> indexPaths) {
        super(store, blobStore, whiteboard, outputDir, workDir, indexPaths);
    }

    public LuceneIndexHelper getLuceneIndexHelper(){
        if (luceneIndexHelper == null) {
            luceneIndexHelper = new LuceneIndexHelper(this);
            closer.register(luceneIndexHelper);
        }
        return luceneIndexHelper;
    }

    @Override
    protected void bindIndexInfoProviders(IndexInfoServiceImpl indexInfoService) {
        indexInfoService.bindInfoProviders(new LuceneIndexInfoProvider(store, getAsyncIndexInfoService(), workDir));
        indexInfoService.bindInfoProviders(new PropertyIndexInfoProvider(store));
    }

    public ExtractedTextCache getExtractedTextCache() {
        if (extractedTextCache == null) {
            extractedTextCache = new ExtractedTextCache(FileUtils.ONE_MB * 5, TimeUnit.HOURS.toSeconds(5));
        }
        return extractedTextCache;
    }

    public void setPreExtractedTextDir(File dir) throws IOException {
        getExtractedTextCache().setExtractedTextProvider(new DataStoreTextWriter(dir, true));
    }
}
