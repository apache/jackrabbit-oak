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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.Iterator;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;

/**
 * A command line utility for the tree store.
 */
public class TreeStoreUtils {

    public static void main(String... args) throws IOException {
        String dir = args[0];
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        try (TreeStore treeStore = new TreeStore("utils", new File(dir), entryReader, 16)) {
            TreeSession session = treeStore.getSession();
            Store store = treeStore.getStore();
            if (store.keySet().isEmpty()) {
                session.init();
                String fileName = args[1];
                try (BufferedReader lineReader = new BufferedReader(
                        new FileReader(fileName, StandardCharsets.UTF_8))) {
                    int count = 0;
                    long start = System.nanoTime();
                    while (true) {
                        String line = lineReader.readLine();
                        if (line == null) {
                            break;
                        }
                        count++;
                        if (count % 1000000 == 0) {
                            long time = System.nanoTime() - start;
                            System.out.println(count + " " + (time / count) + " ns/entry");
                        }
                        int index = line.indexOf('|');
                        if (index < 0) {
                            throw new IllegalArgumentException("| is missing: " + line);
                        }
                        String path = line.substring(0, index);
                        String value = line.substring(index + 1);
                        session.put(path, value);

                        if (!path.equals("/")) {
                            String nodeName = PathUtils.getName(path);
                            String parentPath = PathUtils.getParentPath(path);
                            session.put(parentPath + "\t" + nodeName, "");
                        }

                    }
                }
                session.flush();
                store.close();
            }
            Iterator<NodeStateEntry> it = treeStore.iterator();
            long nodeCount = 0;
            long childNodeCount = 0;
            long start = System.nanoTime();
            while (it.hasNext()) {
                NodeStateEntry e = it.next();
                childNodeCount += e.getNodeState().getChildNodeCount(Long.MAX_VALUE);
                nodeCount++;
                if (nodeCount % 1000000 == 0) {
                    long time = System.nanoTime() - start;
                    System.out.println("Node count: " + nodeCount +
                            " child node count: " + childNodeCount +
                            " speed: " + (time / nodeCount) + " ns/entry");
                }
            }
            System.out.println("Node count: " + nodeCount + " Child node count: " + childNodeCount);
        }
    }

}
