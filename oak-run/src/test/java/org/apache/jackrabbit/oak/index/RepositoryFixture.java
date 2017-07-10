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

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

public class RepositoryFixture implements Closeable {
    private final File dir;
    private Repository repository;
    private FileStore fileStore;
    private NodeStore nodeStore;
    private Whiteboard whiteboard;

    public RepositoryFixture(File dir) {
        this(dir, null);
    }

    public RepositoryFixture(File dir, NodeStore nodeStore) {
        this.dir = dir;
        this.nodeStore = nodeStore;
    }

    public Repository getRepository() throws IOException {
        if (repository == null) {
            repository = createRepository();
        }
        return repository;
    }

    public Session getAdminSession() throws IOException, RepositoryException {
        return getRepository().login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    public NodeStore getNodeStore() throws IOException {
        if (nodeStore == null) {
            nodeStore = createNodeStore();
        }
        return nodeStore;
    }

    public AsyncIndexUpdate getAsyncIndexUpdate(String laneName) {
        return (AsyncIndexUpdate) getService(whiteboard, Runnable.class,
                (runnable) -> runnable instanceof AsyncIndexUpdate && laneName.equals(((AsyncIndexUpdate)runnable).getName()));
    }

    @Override
    public void close() throws IOException {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
            repository = null;
        }

        if (fileStore != null) {
            fileStore.close();
            fileStore = null;
        }

        whiteboard = null;
    }

    public File getDir() {
        return dir;
    }

    private Repository createRepository() throws IOException {
        Oak oak = new Oak(getNodeStore());

        oak.withAsyncIndexing("async", 3600); //Effectively disable async indexing
        configureLuceneProvider(oak);

        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        whiteboard = oak.getWhiteboard();
        return repository;
    }

    private void configureLuceneProvider(Oak oak) throws IOException {
        LuceneIndexEditorProvider ep = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        oak.with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(ep);
    }

    private NodeStore createNodeStore() throws IOException {
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(dir);
        try {
            fileStore = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        }
        return SegmentNodeStoreBuilders.builder(fileStore).build();
    }
}
