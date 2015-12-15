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

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * NodeStore fixture for parametrized tests.
 */
public abstract class NodeStoreFixture {

    public static final NodeStoreFixture SEGMENT_MK = new NodeStoreFixture() {
        @Override
        public String toString() {
            return "SegmentMK Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            try {
                return new SegmentNodeStore(new MemoryStore());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }
    };

    public static final NodeStoreFixture MONGO_NS = new NodeStoreFixture() {
        @Override
        public String toString() {
            return "MongoNS Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new DocumentMK.Builder().getNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore) nodeStore).dispose();
            }
        }
    };

    public static final NodeStoreFixture MEMORY_NS = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            return new MemoryNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) { }
    };

    public abstract NodeStore createNodeStore();

    public abstract void dispose(NodeStore nodeStore);

}
