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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.util.HashMap;

import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.UNIQUE_KEY_SIZE;
import static org.junit.Assert.assertEquals;

public class StreamingOakDirectoryTest extends OakDirectoryTestBase {
    @Override
    void assertBlobSizeInWrite(PropertyState jcrData, int blobSize, int fileSize) {
        Blob blob = jcrData.getValue(BINARY);
        assertEquals(fileSize + UNIQUE_KEY_SIZE, blob.length());
    }

    @Override
    OakDirectoryBuilder getOakDirectoryBuilder(NodeBuilder builder, IndexDefinition indexDefinition) {
        return new OakDirectoryBuilder(builder, indexDefinition, true);
    }

    @Override
    MemoryBlobStore getBlackHoleBlobStore() {
        return new BlackHoleBlobStoreForLargeBlobs();
    }

    private static class BlackHoleBlobStoreForLargeBlobs extends MemoryBlobStore {
        private HashMap<String, Integer> map = new HashMap<>();

        @Override
        protected synchronized void storeBlock(byte[] digest, int level, byte[] data) {
            if (level == 0) {
                String id = StringUtils.convertBytesToHex(digest);
                if (!map.containsKey(id)) {
                    map.put(id, data.length);
                }
            } else {
                super.storeBlock(digest, level, data);
            }
        }

        @Override
        protected byte[] readBlockFromBackend(BlockId id) {
            Integer length = map.get(StringUtils.convertBytesToHex(id.getDigest()));
            if (length != null) {
                return new byte[length];
            } else {
                return super.readBlockFromBackend(id);
            }
        }
    }
}
