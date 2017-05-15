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

package org.apache.jackrabbit.oak.run.cli;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import javax.sql.DataSource;

import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;
import org.apache.jackrabbit.oak.console.BlobStoreFixture;
import org.apache.jackrabbit.oak.console.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class NodeStoreFixtureProvider {
    private static final long MB = 1024 * 1024;

    public static NodeStoreFixture create(Options options) throws Exception {
        return create(options, !options.getOptionBean(CommonOptions.class).isReadWrite());
    }

    public static NodeStoreFixture create(Options options, boolean readOnly) throws Exception {
        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);

        Closer closer = Closer.create();
        BlobStoreFixture blobFixture = BlobStoreFixtureProvider.create(options);
        BlobStore blobStore = null;
        if (blobFixture != null) {
            blobStore = blobFixture.getBlobStore();
            closer.register(blobFixture);
        }

        NodeStore store = null;
        if (commonOpts.isMongo() || commonOpts.isRDB()) {
            store = configureDocumentMk(options, blobStore, closer, readOnly);
        } else {
            store = configureSegment(options, blobStore, closer, readOnly);
        }

        return new SimpleNodeStoreFixture(store, closer);
    }


    private static NodeStore configureDocumentMk(Options options,
                                                 BlobStore blobStore, Closer closer,
                                                 boolean readOnly) throws UnknownHostException {
        DocumentMK.Builder builder = new DocumentMK.Builder();

        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }

        DocumentNodeStoreOptions docStoreOpts = options.getOptionBean(DocumentNodeStoreOptions.class);

        builder.setClusterId(docStoreOpts.getClusterId());

        if (readOnly) {
            builder.setReadOnlyMode();
        }

        int cacheSize = docStoreOpts.getCacheSize();
        if (cacheSize != 0) {
            builder.memoryCacheSize(cacheSize * MB);
        }

        if (docStoreOpts.disableBranchesSpec()) {
            builder.disableBranches();
        }

        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);

        if (commonOpts.isMongo()) {
            MongoClientURI uri = new MongoClientURI(commonOpts.getStoreArg());
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: "
                        + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            closer.register(asCloseable(mongo));
            builder.setMongoDB(mongo.getDB());
        } else if (commonOpts.isRDB()) {
            RDBStoreOptions rdbOpts = options.getOptionBean(RDBStoreOptions.class);
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(commonOpts.getStoreArg(),
                    rdbOpts.getUser(), rdbOpts.getPassword());
            builder.setRDBConnection(ds);
        }

        return builder.getNodeStore();
    }

    private static NodeStore configureSegment(Options options, BlobStore blobStore, Closer closer, boolean readOnly)
            throws IOException, InvalidFileStoreVersionException {

        String path = options.getOptionBean(CommonOptions.class).getStoreArg();
        FileStoreBuilder builder = fileStoreBuilder(new File(path)).withMaxFileSize(256);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        NodeStore nodeStore;
        if (readOnly) {
            ReadOnlyFileStore fileStore = builder.buildReadOnly();
            closer.register(fileStore);
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        } else {
            FileStore fileStore = builder.build();
            closer.register(fileStore);
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        }

        return nodeStore;
    }

    private static Closeable asCloseable(final MongoConnection con) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                con.close();
            }
        };
    }

    private static class SimpleNodeStoreFixture implements NodeStoreFixture {
        private final Closer closer;
        private final NodeStore nodeStore;

        private SimpleNodeStoreFixture(NodeStore nodeStore, Closer closer) {
            this.closer = closer;
            this.nodeStore = nodeStore;
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public void close() throws IOException {
            closer.close();
        }
    }
}
