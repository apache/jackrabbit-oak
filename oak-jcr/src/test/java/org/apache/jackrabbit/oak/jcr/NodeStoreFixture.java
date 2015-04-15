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
package org.apache.jackrabbit.oak.jcr;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.sql.DataSource;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * NodeStore fixture for parametrized tests.
 */
public abstract class NodeStoreFixture {

    public static final NodeStoreFixture SEGMENT_MK = new SegmentFixture();

    public static final NodeStoreFixture DOCUMENT_NS = createDocumentFixture("mongodb://localhost:27017/oak");

    public static final NodeStoreFixture DOCUMENT_RDB = new NodeStoreFixture() {

        private DataSource ds;
        private String fname = (new File("target")).isDirectory() ? "target/" : "";

        @Override
        public NodeStore createNodeStore() {
            String id = UUID.randomUUID().toString();
            this.ds = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:file:./" + fname + id, "sa", "");
            return new DocumentMK.Builder().
                    setRDBConnection(this.ds).
                    setPersistentCache("target/persistentCache,time").                        
                    getNodeStore();
        }

        @Override
        public NodeStore createNodeStore(int clusterNodeId) {
            try {
                this.ds = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:file:./" + fname + "oaknodes-" + clusterNodeId, "sa", "");
                return new DocumentMK.Builder().
                        setRDBConnection(this.ds).
                        setPersistentCache("target/persistentCache,time").                        
                        getNodeStore();
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore) nodeStore).dispose();
            }
            if (this.ds instanceof Closeable) {
                try {
                    ((Closeable)this.ds).close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    };

    public static NodeStoreFixture createDocumentFixture(final String uri) {
        return new DocumentFixture(uri);
    }

    /**
     * Creates a new empty {@link NodeStore} instance. An implementation must
     * ensure the returned node store is indeed empty and is independent from
     * instances returned from previous calls to this method.
     *
     * @return a new node store instance.
     */
    public abstract NodeStore createNodeStore();

    /**
     * Create a new cluster node that is attached to the same backend storage.
     * 
     * @param clusterNodeId the cluster node id
     * @return the node store, or null if clustering is not supported
     */
    public NodeStore createNodeStore(int clusterNodeId) {
        return null;
    }

    public abstract void dispose(NodeStore nodeStore);

    public boolean isAvailable() {
        return true;
    }

    public static class SegmentFixture extends NodeStoreFixture {
        private final SegmentStore store;

        public SegmentFixture() {
            this(null);
        }

        public SegmentFixture(SegmentStore store) {
            this.store = store;
        }

        @Override
        public NodeStore createNodeStore() {
            return new SegmentNodeStore(store == null ? new MemoryStore() : store);
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }
    }

    public static class DocumentFixture extends NodeStoreFixture {
        public static final String DEFAULT_URI = "mongodb://localhost:27017/oak";

        private final String uri;
        private final boolean inMemory;
        private final BlobStore blobStore;

        public DocumentFixture(String uri, boolean inMemory, BlobStore blobStore) {
            this.uri = uri;
            this.inMemory = inMemory;
            this.blobStore = blobStore;
        }

        public DocumentFixture(String uri) {
            this(uri, true, null);
        }

        public DocumentFixture() {
            this(DEFAULT_URI, false, null);
        }

        private static NodeStore createNodeStore(String uri, BlobStore blobStore) {
            MongoConnection connection;
            try {
                connection = new MongoConnection(uri);
                DB mongoDB = connection.getDB();
                DocumentMK.Builder builder = new DocumentMK.Builder();
                if(blobStore != null){
                    builder.setBlobStore(blobStore);
                }
                builder.setPersistentCache("target/persistentCache,time");
                builder.setMongoDB(mongoDB);
                return builder.getNodeStore();
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public NodeStore createNodeStore() {
            if (inMemory) {
                return new DocumentMK.Builder().getNodeStore();
            } else {
                return createNodeStore(uri + '-' + System.nanoTime(), blobStore);
            }
        }

        @Override
        public NodeStore createNodeStore(int clusterNodeId) {
            return createNodeStore(uri, blobStore);
        }

        @Override
        public boolean isAvailable() {
            // FIXME is there a better way to check whether MongoDB is available?
            NodeStore nodeStore = createNodeStore(uri, blobStore);
            if (nodeStore == null) {
                return false;
            } else {
                dispose(nodeStore);
                return true;
            }
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore) nodeStore).dispose();
            }
        }
    }
}
