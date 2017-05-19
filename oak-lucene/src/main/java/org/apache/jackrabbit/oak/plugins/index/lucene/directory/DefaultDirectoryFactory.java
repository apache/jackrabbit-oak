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

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;

public class DefaultDirectoryFactory implements DirectoryFactory {
    private final IndexCopier indexCopier;
    private final GarbageCollectableBlobStore blobStore;

    public DefaultDirectoryFactory(@Nullable  IndexCopier indexCopier, @Nullable GarbageCollectableBlobStore blobStore) {
        this.indexCopier = indexCopier;
        this.blobStore = blobStore;
    }

    @Override
    public Directory newInstance(IndexDefinition definition, NodeBuilder builder,
                                 String dirName, boolean reindex) throws IOException {
        Directory directory = IndexWriterUtils.newIndexDirectory(definition, builder, dirName,
                indexCopier != null, blobStore);
        if (indexCopier != null) {
            directory = indexCopier.wrapForWrite(definition, directory, reindex, dirName);
        }
        return directory;
    }

    @Override
    public boolean remoteDirectory() {
        return indexCopier == null;
    }
}
