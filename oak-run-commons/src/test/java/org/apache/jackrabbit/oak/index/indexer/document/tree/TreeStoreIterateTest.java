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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TreeStoreIterateTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void buildAndIterateTest() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder,
                new NodeStateEntryReader(new MemoryBlobStore()), 1);
        try {
            store.getSession().init();
            store.putNode("/test", "{}");
            store.putNode("/test-node", "{}");
            store.putNode("/test-node/child/node", "{}");
            store.putNode("/test-node/child/node/test", "{}");
            store.putNode("/test/child", "{}");
            Iterator<NodeStateEntry> it = store.iterator();
            NodeStateEntry e = it.next();
            assertEquals("/test", e.getPath());
            e = it.next();
            assertEquals("/test-node", e.getPath());
            Iterator<String> it2 = e.getNodeState().getChildNodeNames().iterator();
            assertFalse(it2.hasNext());
            e = it.next();
            assertEquals("/test-node/child/node", e.getPath());
            it2 = e.getNodeState().getChildNodeNames().iterator();
            assertTrue(it2.hasNext());
            e = it.next();
            assertEquals("/test-node/child/node/test", e.getPath());
            e = it.next();
            assertEquals("/test/child", e.getPath());
            assertFalse(it.hasNext());
        } finally {
            store.close();
        }
    }

    @Test
    public void test() throws IOException {
        TreeStore store = new TreeStore("test",
                new File("test.lz4"),
                new NodeStateEntryReader(new MemoryBlobStore()), 1);
        Iterator<String> itPath = store.iteratorOverPaths();

        while(itPath.hasNext()) {
            String path = itPath.next();
            System.out.println(path);
        }

        itPath = store.iteratorOverPaths();
        while(itPath.hasNext()) {
            String path = itPath.next();
            log(path);
            NodeState nodeState = store.getNodeState(path);
            HashSet<String> seen = new HashSet<>();
            recurseAllChildren(seen, path, nodeState, 10);
        }
        store.close();
    }

    static long lastLog = System.currentTimeMillis();
    private static void log(String msg) {
        long now = System.currentTimeMillis();
        if (now - lastLog > 2000) {
            lastLog = now;
            System.out.println(msg);
        }
    }

    private static void recurseAllChildren(HashSet<String> seen, String path, NodeState ns, int depth) {
        log("  " + path);
        if (depth == 0) {
            return;
        }
        for (ChildNodeEntry cne : ns.getChildNodeEntries()) {
            String cp = path;
            if (!cp.endsWith("/")) {
                cp += "/";
            }
            cp += cne.getName();
            recurseAllChildren(seen, cp, cne.getNodeState(), depth - 1);
        }
    }

}
