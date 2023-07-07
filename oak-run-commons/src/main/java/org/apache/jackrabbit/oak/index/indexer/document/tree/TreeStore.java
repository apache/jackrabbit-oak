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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class TreeStore implements Iterable<NodeStateEntry>, Closeable {

    private final Store store;
    private final Session session;
    private final NodeStateEntryReader entryReader;

    public TreeStore(File directory, NodeStateEntryReader entryReader) {
        this.entryReader = entryReader;
        this.store = StoreBuilder.build("type=file\n" + "dir=" + directory.getAbsolutePath());
        this.session = new Session(store);
    }

    @Override
    public void close() throws IOException {
        session.flush();
        store.close();
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        Iterator<Entry<String, String>> it = session.iterator();
        return new Iterator<NodeStateEntry>() {

            NodeStateEntry current;

            {
                fetch();
            }

            private void fetch() {
                while (it.hasNext()) {
                    Entry<String, String> e = it.next();
                    if (e.getValue().isEmpty()) {
                        continue;
                    }
                    current = getNodeStateEntry(e.getKey(), e.getValue());
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public NodeStateEntry next() {
                NodeStateEntry result = current;
                fetch();
                return result;
            }

        };
    }

    NodeStateEntry getNodeStateEntry(String path) {
        return new NodeStateEntryBuilder(getNodeState(path), path).build();
    }

    NodeStateEntry getNodeStateEntry(String path, String value) {
        return new NodeStateEntryBuilder(getNodeState(path, value), path).build();
    }

    NodeState getNodeState(String path) {
        String value = session.get(path);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(path);
        }
        return getNodeState(path, value);
    }

    NodeState getNodeState(String path, String value) {
        String line = path + "|" + value;
        NodeStateEntry entry = entryReader.read(line);
        NodeState wrapped = new TreeStoreNodeState(entry.getNodeState(), path, this);
        return wrapped;
    }

    /**
     * The child node entry for the given path.
     *
     * @param path the path, e.g. /hello/world
     * @return the child node entry, e.g. /hello<tab>world
     */
    public static String toChildNodeEntry(String path) {
        if (path.equals("/")) {
            return "\t";
        }
        String nodeName = PathUtils.getName(path);
        String parentPath = PathUtils.getParentPath(path);
        return parentPath + "\t" + nodeName;
    }

    /**
     * The child node entry for the given parent and child.
     *
     * @param path the parentPath, e.g. /hello
     * @param childName the name of the child node, e.g. world
     * @return the child node entry, e.g. /hello<tab>world
     */
    public static String toChildNodeEntry(String parentPath, String childName) {
        return parentPath + "\t" + childName;
    }

    public Session getSession() {
        return session;
    }

}
