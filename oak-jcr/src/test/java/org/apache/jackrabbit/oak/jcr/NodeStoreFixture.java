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
import java.io.IOException;
import java.util.UUID;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoMK;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoNodeStore;
import org.apache.jackrabbit.oak.plugins.mongomk.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.mongodb.DB;

/**
 * NodeStore fixture for parametrized tests.
 */
public abstract class NodeStoreFixture {

    public static final NodeStoreFixture SEGMENT_MK = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            return new SegmentNodeStore(new MemoryStore());
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }
    };

    public static final NodeStoreFixture MONGO_MK = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            return new CloseableNodeStore(new MongoMK.Builder().open());
        }
        
        @Override
        public NodeStore createNodeStore(int clusterNodeId) {
            MongoConnection connection;
            try {
                connection = new MongoConnection("mongodb://localhost:27017/oak");
                DB mongoDB = connection.getDB();
                MongoMK mk = new MongoMK.Builder()
                                .setMongoDB(mongoDB).open();
                return new CloseableNodeStore(mk);
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof Closeable) {
                try {
                    ((Closeable) nodeStore).close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    public static final NodeStoreFixture MONGO_NS = createMongoFixture("mongodb://localhost:27017/oak");

    public static final NodeStoreFixture MONGO_JDBC = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            String id = UUID.randomUUID().toString();
            return new MongoMK.Builder().setRDBConnection("jdbc:h2:mem:" + id, "sa", "").getNodeStore();
        }

        @Override
        public NodeStore createNodeStore(int clusterNodeId) {
            try {
                return new MongoMK.Builder().setRDBConnection("jdbc:h2:mem:oaknodes-" + clusterNodeId, "sa", "").getNodeStore();
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof MongoNodeStore) {
                ((MongoNodeStore) nodeStore).dispose();
            }
        }
    };

    public static final NodeStoreFixture MK_IMPL = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            return new KernelNodeStore(new MicroKernelImpl());
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }
    };
    
    public static NodeStoreFixture createMongoFixture(final String uri) {
        return new NodeStoreFixture() {
            @Override
            public NodeStore createNodeStore() {
                return new MongoMK.Builder().getNodeStore();
            }
            
            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                MongoConnection connection;
                try {
                    connection = new MongoConnection(uri);
                    DB mongoDB = connection.getDB();
                    return new MongoMK.Builder()
                                    .setMongoDB(mongoDB).getNodeStore();
                } catch (Exception e) {
                    return null;
                }
            }

            @Override
            public void dispose(NodeStore nodeStore) {
                if (nodeStore instanceof MongoNodeStore) {
                    ((MongoNodeStore) nodeStore).dispose();
                }
            }
        };
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

    private static class CloseableNodeStore
            extends KernelNodeStore implements Closeable {

        private final MongoMK kernel;

        public CloseableNodeStore(MongoMK kernel) {
            super(kernel);
            this.kernel = kernel;
        }

        @Override
        public void close() throws IOException {
            kernel.dispose();
        }
    }
}
