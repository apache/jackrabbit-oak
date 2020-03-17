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

import java.io.File;
import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene.isIndexDirName;
import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene.isSuggestIndexDirName;

public class LuceneIndexDumper {
    private final NodeState rootState;
    private final String indexPath;
    private final File baseDir;
    private long size;
    private File indexDir;

    /**
     * Constructs the dumper for copying Lucene index contents
     *
     * @param rootState rootState of repository
     * @param indexPath path of index
     * @param baseDir directory under which index contents would be copied to. Dumper
     *                would create a sub directory based on index path and then copy
     *                the index content under that directory
     */
    public LuceneIndexDumper(NodeState rootState, String indexPath, File baseDir) {
        this.rootState = rootState;
        this.indexPath = indexPath;
        this.baseDir = baseDir;
    }

    public void dump() throws IOException {
        try (Closer closer = Closer.create()) {
            NodeState idx = NodeStateUtils.getNode(rootState, indexPath);
            IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx, indexPath).build();
            indexDir = DirectoryUtils.createIndexDir(baseDir, indexPath);
            IndexMeta meta = new IndexMeta(indexPath);

            for (String dirName : idx.getChildNodeNames()) {
                if (NodeStateUtils.isHidden(dirName) &&
                        (isIndexDirName(dirName) || isSuggestIndexDirName(dirName))) {
                    copyContent(idx, defn, meta, indexDir, dirName, closer);
                }
            }

            DirectoryUtils.writeMeta(indexDir, meta);
        }
    }

    public long getSize() {
        return size;
    }

    public File getIndexDir() {
        return indexDir;
    }

    private void copyContent(NodeState idx, IndexDefinition defn, IndexMeta meta, File dir, String dirName, Closer closer) throws IOException {
        File idxDir = DirectoryUtils.createSubDir(dir, dirName);

        meta.addDirectoryMapping(dirName, idxDir.getName());

        Directory sourceDir = new OakDirectory(new ReadOnlyBuilder(idx), dirName, defn, true);
        Directory targetDir = FSDirectory.open(idxDir);

        closer.register(sourceDir);
        closer.register(targetDir);

        for (String file : sourceDir.listAll()) {
            sourceDir.copy(targetDir, file, file, IOContext.DEFAULT);
            size += sourceDir.fileLength(file);
        }
    }
}
