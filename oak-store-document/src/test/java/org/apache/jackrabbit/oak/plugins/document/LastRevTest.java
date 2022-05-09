/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Checks if _lastRev entries are correctly set and updated.
 */
public class LastRevTest {

    @Test
    public void lastRev() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder()
                .setAsyncDelay(0).getNodeStore();
        DocumentStore docStore = store.getDocumentStore();

        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            NodeBuilder child = root.child("child-" + i);
            for (int j = 0; j < 10; j++) {
                child.child("test-" + j);
            }
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();

        for (int i = 0; i < 10; i++) {
            String parentPath = "/child-" + i;
            assertLastRevSize(docStore, parentPath, 0);
            for (int j = 0; j < 10; j++) {
                String path = parentPath + "/test-" + j;
                assertLastRevSize(docStore, path, 0);
            }
        }

        store.dispose();
    }

    private static void assertLastRevSize(DocumentStore store,
                                          String path, int size) {
        NodeDocument doc = store.find(NODES, getIdFromPath(path));
        assertNotNull(doc);
        assertEquals("_lastRev: " + doc.getLastRev(), size, doc.getLastRev().size());
    }

}
