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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexHelper implements Closeable {
    private static final String PROP_BUFFER_SIZE = "oak.index.ramBufferSizeMB";
    private static final int BUFFER_SIZE_DEFAULT = 32;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexHelper indexHelper;
    private IndexCopier indexCopier;
    private DirectoryFactory directoryFactory;


    LuceneIndexHelper(IndexHelper indexHelper) {
        this.indexHelper = indexHelper;
    }

    public LuceneIndexEditorProvider createEditorProvider() throws IOException {
        LuceneIndexEditorProvider editor;
        if (directoryFactory != null) {
            editor = new LuceneIndexEditorProvider(
                    getIndexCopier(),
                    indexHelper.getExtractedTextCache(),
                    null,
                    indexHelper.getMountInfoProvider()
            ) {
                @Override
                protected DirectoryFactory newDirectoryFactory(BlobDeletionCallback blobDeletionCallback) {
                    return directoryFactory;
                }
            };
        } else {
            editor = new LuceneIndexEditorProvider(
                    getIndexCopier(),
                    indexHelper.getExtractedTextCache(),
                    null,
                    indexHelper.getMountInfoProvider()
            );
        }

        editor.setBlobStore(indexHelper.getGCBlobStore());

        return editor;
    }

    public LuceneIndexWriterConfig getWriterConfigForReindex() {
        int buffSize = Integer.getInteger(PROP_BUFFER_SIZE, BUFFER_SIZE_DEFAULT);

        log.info("Setting RAMBufferSize for LuceneIndexWriter (configurable via " +
                "system property '{}') to {} MB", PROP_BUFFER_SIZE, buffSize);

        return new LuceneIndexWriterConfig(buffSize);
    }

    public void setDirectoryFactory(DirectoryFactory directoryFactory) {
        this.directoryFactory = directoryFactory;
    }

    private IndexCopier getIndexCopier() throws IOException {
        if (indexCopier == null) {
            File indexWorkDir = new File(indexHelper.getWorkDir(), "indexWorkDir");
            indexCopier = new IndexCopier(indexHelper.getExecutor(), indexWorkDir, true);
        }
        return indexCopier;
    }

    @Override
    public void close() throws IOException {
        if (indexCopier != null) {
            indexCopier.close();
        }
    }
}
