/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.segment.file.FileBlob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static junit.framework.Assert.assertTrue;

public class ExternalBlobTest {

    private SegmentStore store;
    private SegmentNodeStore nodeStore;
    private FileBlob fileBlob;

    @Test
    public void testCreateAndRead() throws Exception {
        SegmentNodeStore nodeStore = getNodeStore();

        NodeState state = nodeStore.getRoot().getChildNode("hello");
        if (!state.exists()) {
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.child("hello");
            nodeStore.merge(builder, EmptyHook.INSTANCE, null);
        }

        Blob blob = getFileBlob();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.getChildNode("hello").setProperty("world", blob);
        nodeStore.merge(builder, EmptyHook.INSTANCE, null);

        state = nodeStore.getRoot().getChildNode("hello");
        blob = state.getProperty("world").getValue(Type.BINARY);

        assertTrue("Blob written and read must be equal",
                AbstractBlob.equal(blob, getFileBlob()));
    }

    @After
    public void close() {
        if (store != null) {
            store.close();
        }
    }

    protected SegmentNodeStore getNodeStore() throws IOException {
        if (nodeStore == null) {
            store = new FileStore(new File("target", "ExternalBlobTest"), 256, false);
            nodeStore = new SegmentNodeStore(store);
        }
        return nodeStore;
    }

    private FileBlob getFileBlob() throws IOException {
        if (fileBlob == null) {
            File file = File.createTempFile("blob", "tmp");
            file.deleteOnExit();

            byte[] data = new byte[2345];
            new Random().nextBytes(data);
            FileUtils.writeByteArrayToFile(file, data);

            fileBlob = new FileBlob(file.getPath());
        }
        return fileBlob;
    }
}
