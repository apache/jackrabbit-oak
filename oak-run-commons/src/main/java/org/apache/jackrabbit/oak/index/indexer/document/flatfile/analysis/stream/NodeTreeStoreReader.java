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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A reader for tree store files.
 */
public class NodeTreeStoreReader implements NodeDataReader {

    private final TreeStore treeStore;
    private final Iterator<String> pathIterator;
    private final long fileSize;

    public static NodeDataReader open(String fileName) {
        BlobStore blobStore = null;
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        File file = new File(fileName);
        TreeStore treeStore = new TreeStore("reader", file, entryReader, 32);
        return new NodeTreeStoreReader(treeStore, file.length());
    }

    private NodeTreeStoreReader(TreeStore treeStore, long fileSize) {
        this.treeStore = treeStore;
        this.fileSize = fileSize;
        this.pathIterator = treeStore.iteratorOverPaths();
    }

    @Override
    public void close() throws IOException {
        treeStore.close();
    }

    @Override
    public NodeData readNode() throws IOException {
        if (!pathIterator.hasNext()) {
            return null;
        }
        String path = pathIterator.next();
        List<String> pathElements = new ArrayList<>();
        PathUtils.elements(path).forEach(pathElements::add);
        String nodeJson = treeStore.getSession().get(path);
        return new NodeData(pathElements, NodeLineReader.parse(nodeJson));
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public int getProgressPercent() {
        return 0;
    }

}
