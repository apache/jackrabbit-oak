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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.ReindexOperations;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

public class LuceneIndexImporter implements IndexImporterProvider {
    private GarbageCollectableBlobStore blobStore;

    public LuceneIndexImporter(){

    }

    public LuceneIndexImporter(GarbageCollectableBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public void importIndex(NodeState root, NodeBuilder definitionBuilder, File indexDir) throws IOException {
        LocalIndexDir localIndex = new LocalIndexDir(indexDir);

        //TODO The indexFormatVersion would be considered latest. Need to be revisited
        //if off line indexing uses older Lucene

        definitionBuilder.getChildNode(IndexDefinition.STATUS_NODE).remove();

        ReindexOperations reindexOps = new ReindexOperations(root, definitionBuilder, localIndex.getJcrPath());
        IndexDefinition definition = reindexOps.apply(true);

        for (File dir : localIndex.dir.listFiles(File::isDirectory)) {
            String jcrName = localIndex.indexMeta.getJcrNameFromFSName(dir.getName());
            if (jcrName != null) {
                copyDirectory(definition, definitionBuilder, jcrName, dir);
            }
        }
    }

    @Override
    public String getType() {
        return TYPE_LUCENE;
    }

    public void setBlobStore(GarbageCollectableBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    private void copyDirectory(IndexDefinition definition, NodeBuilder definitionBuilder, String jcrName, File dir)
            throws IOException {
        try (Closer closer = Closer.create()) {
            Directory sourceDir = FSDirectory.open(dir);
            closer.register(sourceDir);

            //Remove any existing directory as in import case
            //the builder can have existing hidden node structures
            //So remove the ones which are being imported and leave
            // //others as is
            definitionBuilder.getChildNode(jcrName).remove();

            Directory targetDir = new OakDirectory(definitionBuilder, jcrName, definition, false, blobStore);
            closer.register(targetDir);

            for (String file : sourceDir.listAll()) {
                sourceDir.copy(targetDir, file, file, IOContext.DEFAULT);
            }
        }
    }
}
