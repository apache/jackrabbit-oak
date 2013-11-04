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
package org.apache.jackrabbit.oak;

import static org.apache.jackrabbit.oak.kernel.KernelNodeStore.DEFAULT_CACHE_SIZE;

import java.io.Closeable;
import java.io.IOException;

import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoMK;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * NodeStore fixture for parametrized tests.
 */
public abstract class NodeStoreFixture {

    public static final NodeStoreFixture SEGMENT_MK = new NodeStoreFixture() {
        private final ResettableObserver observer = new ResettableObserver();

        @Override
        public String toString() {
            return "SegmentMK Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new SegmentNodeStore(new MemoryStore(), observer);
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }

        @Override
        public void setObserver(Observer observer) {
            this.observer.setObserver(observer);
        }
    };

    public static final NodeStoreFixture MONGO_MK = new NodeStoreFixture() {
        private final ResettableObserver observer = new ResettableObserver();

        @Override
        public String toString() {
            return "MongoMK Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new CloseableNodeStore(new MongoMK.Builder().open(), observer);
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

        @Override
        public void setObserver(Observer observer) {
            this.observer.setObserver(observer);
        }
    };

    public static final NodeStoreFixture MONGO_NS = new NodeStoreFixture() {
        private final ResettableObserver observer = new ResettableObserver();

        @Override
        public String toString() {
            return "MongoNS Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new MongoMK.Builder().getNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof MongoNodeStore) {
                ((MongoNodeStore) nodeStore).dispose();
            }
        }

        @Override
        public void setObserver(Observer observer) {
            this.observer.setObserver(observer);
        }
    };

    public static final NodeStoreFixture MK_IMPL = new NodeStoreFixture() {
        private final ResettableObserver observer = new ResettableObserver();

        @Override
        public String toString() {
            return "MKImpl Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new KernelNodeStore(new MicroKernelImpl(), DEFAULT_CACHE_SIZE, observer);
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }

        @Override
        public void setObserver(Observer observer) {
            this.observer.setObserver(observer);
        }
    };

    public abstract NodeStore createNodeStore();

    public abstract void dispose(NodeStore nodeStore);

    public abstract void setObserver(Observer observer);

    private static class CloseableNodeStore
            extends KernelNodeStore implements Closeable {

        private final MongoMK kernel;

        public CloseableNodeStore(MongoMK kernel, Observer observer) {
            super(kernel, DEFAULT_CACHE_SIZE, observer);
            this.kernel = kernel;
        }

        @Override
        public void close() throws IOException {
            kernel.dispose();
        }
    }

    private static class ResettableObserver implements Observer {
        private Observer observer;

        @Override
        public void contentChanged(NodeState before, NodeState after) {
            if (observer != null) {
                observer.contentChanged(before, after);
            }
        }

        public void setObserver(Observer observer) {
            this.observer = observer;
        }
    }
}
