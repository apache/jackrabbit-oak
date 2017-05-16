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
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexPrinter;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class IndexHelper {
    private final NodeStore store;
    private final File outputDir;
    private final File workDir;
    private IndexInfoServiceImpl indexInfoService;
    private IndexPathService indexPathService;
    private AsyncIndexInfoService asyncIndexInfoService;
    private final List<String> indexPaths;

    IndexHelper(NodeStore store, File outputDir, File workDir, List<String> indexPaths) {
        this.store = store;
        this.outputDir = outputDir;
        this.workDir = workDir;
        this.indexPaths = indexPaths;
    }

    public File getOutputDir() {
        return outputDir;
    }

    public IndexPrinter getIndexPrinter() {
        return new IndexPrinter(getIndexInfoService(), getAsyncIndexInfoService());
    }

    public IndexDefinitionPrinter getIndexDefnPrinter() {
        return new IndexDefinitionPrinter(store, getIndexPathService());
    }

    private IndexPathService getIndexPathService() {
        if (indexPathService == null) {
            if (indexPaths.isEmpty()) {
                indexPathService = new IndexPathServiceImpl(store);
            } else {
                indexPathService = () -> indexPaths;
            }
        }
        return indexPathService;
    }

    private AsyncIndexInfoService getAsyncIndexInfoService() {
        if (asyncIndexInfoService == null) {
            asyncIndexInfoService = new AsyncIndexInfoServiceImpl(store);
        }
        return asyncIndexInfoService;
    }

    private IndexInfoService getIndexInfoService() {
        if (indexInfoService == null) {
            indexInfoService = new IndexInfoServiceImpl(store, getIndexPathService());
            bindIndexInfoProviders(indexInfoService);
        }
        return indexInfoService;
    }

    private void bindIndexInfoProviders(IndexInfoServiceImpl indexInfoService) {
        indexInfoService.bindInfoProviders(new LuceneIndexInfoProvider(store, getAsyncIndexInfoService(), workDir));
        indexInfoService.bindInfoProviders(new PropertyIndexInfoProvider(store));
    }
}
