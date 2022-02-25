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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.File;
import java.io.IOException;

public abstract class IndexImporterSupportBase {

    protected final NodeStore nodeStore;
    protected final IndexHelper indexHelper;

    public IndexImporterSupportBase(IndexHelper indexHelper) {
        this.nodeStore = indexHelper.getNodeStore();
        this.indexHelper = indexHelper;
    }

    public void importIndex(File importDir) throws IOException, CommitFailedException {
        IndexImporter importer = new IndexImporter(nodeStore, importDir, createIndexEditorProvider(), createLock());
        addImportProviders(importer);
        importer.importIndex();
    }

    private AsyncIndexerLock createLock() {
        if (nodeStore instanceof Clusterable) {
            return new ClusterNodeStoreLock(nodeStore);
        }
        //For oak-run usage with non Clusterable NodeStore indicates that NodeStore is not
        //active. So we can use a noop lock implementation as there is no concurrent run
        return AsyncIndexerLock.NOOP_LOCK;
    }

    protected abstract IndexEditorProvider createIndexEditorProvider() throws IOException;

    protected abstract void addImportProviders(IndexImporter importer);


}
