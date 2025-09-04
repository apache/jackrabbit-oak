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
package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.commons.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

public abstract class IndexStoreSortStrategyBase implements IndexStoreSortStrategy {
    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Directory where sorted files will be created.
     */
    private final File storeDir;
    private final Compression algorithm;
    private final Predicate<String> pathPredicate;
    private final Set<String> preferredPaths;
    private final String checkpoint;
    private static final String DEFAULT_INDEX_STORE_TYPE = "FlatFileStore";

    public IndexStoreSortStrategyBase(File storeDir, Compression algorithm, Predicate<String> pathPredicate,
                                      Set<String> preferredPaths, String checkpoint) {
        this.storeDir = storeDir;
        this.algorithm = algorithm;
        this.pathPredicate = pathPredicate;
        this.preferredPaths = preferredPaths;
        this.checkpoint = checkpoint;
    }

    @Override
    public File getStoreDir() {
        return storeDir;
    }

    @Override
    public Compression getAlgorithm() {
        return algorithm;
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStoreType() {
        return DEFAULT_INDEX_STORE_TYPE;
    }

    @Override
    public String getCheckpoint() {
        return checkpoint;
    }

    @Override
    public Set<String> getPreferredPaths() {
        return preferredPaths;
    }

    @Override
    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }

    @Override
    public File createMetadataFile() throws IOException {
        IndexStoreMetadata indexStoreMetadata = new IndexStoreMetadata(checkpoint, getStoreType(), getStrategyName(), preferredPaths);
        File metadataFile = new IndexStoreMetadataOperatorImpl<IndexStoreMetadata>().createMetadataFile(indexStoreMetadata, storeDir, algorithm);
        log.info("Created metadataFile:{} with strategy:{} ", metadataFile.getPath(), this.getStoreType());
        return metadataFile;
    }
}
