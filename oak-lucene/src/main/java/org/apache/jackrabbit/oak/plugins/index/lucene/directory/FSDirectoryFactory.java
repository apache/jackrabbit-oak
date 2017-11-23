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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory.INDEX_METADATA_FILE_NAME;

public class FSDirectoryFactory implements DirectoryFactory {
    private final File baseDir;

    public FSDirectoryFactory(File baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public Directory newInstance(IndexDefinition definition, NodeBuilder builder,
                                 String dirName, boolean reindex) throws IOException {
        File indexDir = DirectoryUtils.createIndexDir(baseDir, definition.getIndexPath());
        File readMe = new File(indexDir, INDEX_METADATA_FILE_NAME);
        File subDir = DirectoryUtils.createSubDir(indexDir, dirName);
        FileUtils.forceMkdir(subDir);

        IndexMeta meta;
        if (!readMe.exists()) {
            meta = new IndexMeta(definition.getIndexPath());
        } else {
            meta = new IndexMeta(readMe);
        }
        meta.addDirectoryMapping(dirName, subDir.getName());
        DirectoryUtils.writeMeta(indexDir, meta);
        return FSDirectory.open(subDir, NoLockFactory.getNoLockFactory());
    }

    @Override
    public boolean remoteDirectory() {
        return false;
    }
}
