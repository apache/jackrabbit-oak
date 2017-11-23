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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LuceneIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;


class IndexImporterSupport {
    private final IndexHelper indexHelper;
    private final NodeStore nodeStore;

    public IndexImporterSupport(IndexHelper indexHelper) {
        this.indexHelper = indexHelper;
        this.nodeStore = indexHelper.getNodeStore();
    }

    public void importIndex(File importDir) throws IOException, CommitFailedException {
        IndexImporter importer = new IndexImporter(nodeStore, importDir, createIndexEditorProvider(), createLock());
        addImportProviders(importer);
        importer.importIndex();
    }

    private void addImportProviders(IndexImporter importer) {
        importer.addImporterProvider(new LuceneIndexImporter(indexHelper.getGCBlobStore()));
    }

    private AsyncIndexerLock createLock() {
        if (nodeStore instanceof Clusterable) {
            return new ClusterNodeStoreLock(nodeStore);
        }
        //For oak-run usage with non Clusterable NodeStore indicates that NodeStore is not
        //active. So we can use a noop lock implementation as there is no concurrent run
        return AsyncIndexerLock.NOOP_LOCK;
    }

    private IndexEditorProvider createIndexEditorProvider() throws IOException {
        MountInfoProvider mip = indexHelper.getMountInfoProvider();
        //Later we can add support for property index and other indexes here
        return new CompositeIndexEditorProvider(
                createLuceneEditorProvider(),
                new PropertyIndexEditorProvider().with(mip),
                new ReferenceEditorProvider().with(mip)
        );
    }

    private IndexEditorProvider createLuceneEditorProvider() throws IOException {
        return indexHelper.getLuceneIndexHelper().createEditorProvider();
    }
}
