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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedSortedTreeStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Predicate;

public class TreeNodeStoreBuilder {

    public static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    private static final String TREE_STORE_DIRECTORY = "tree-store";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File workDir;
    private BlobStore blobStore;
    private Predicate<String> pathPredicate = path -> true;

    private final boolean compressionEnabled = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    private final Compression algorithm = compressionEnabled ? Compression.LZ4 : Compression.NO;
    private RevisionVector rootRevision = null;
    private DocumentNodeStore nodeStore = null;
    private MongoDocumentStore mongoDocumentStore = null;

    public TreeNodeStoreBuilder(File workDir) {
        this.workDir = workDir;
    }

    public TreeNodeStoreBuilder withBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
        return this;
    }

    public TreeNodeStoreBuilder withPathPredicate(Predicate<String> pathPredicate) {
        this.pathPredicate = pathPredicate;
        return this;
    }

    public TreeNodeStoreBuilder withRootRevision(RevisionVector rootRevision) {
        this.rootRevision = rootRevision;
        return this;
    }

    public TreeNodeStoreBuilder withNodeStore(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
        return this;
    }

    public TreeNodeStoreBuilder withMongoDocumentStore(MongoDocumentStore mongoDocumentStore) {
        this.mongoDocumentStore = mongoDocumentStore;
        return this;
    }

    private void logFlags() {
        log.info("Compression enabled while sorting : {} ({})", compressionEnabled, OAK_INDEXER_USE_ZIP);
    }

    public File createSortedStoreFile() throws IOException {
        File treeStoreDirectory = Files.createTempDirectory(workDir.toPath(), TREE_STORE_DIRECTORY).toFile();
        PipelinedSortedTreeStrategy strategy = new PipelinedSortedTreeStrategy(
                mongoDocumentStore, nodeStore, rootRevision,
                blobStore, treeStoreDirectory, algorithm, pathPredicate
        );
        return strategy.createSortedStoreFile();

    }

    public TreeStore build() throws IOException, CompositeException {
        // TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }

}
