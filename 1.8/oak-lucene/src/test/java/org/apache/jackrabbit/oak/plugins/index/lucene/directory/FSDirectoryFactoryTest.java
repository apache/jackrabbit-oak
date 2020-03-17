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
import java.io.FileFilter;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.*;

public class FSDirectoryFactoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder idx = new IndexDefinitionBuilder().build().builder();

    @Test
    public void singleIndex() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(root, idx.getNodeState(), "/fooIndex").build();
        FSDirectoryFactory factory = new FSDirectoryFactory(temporaryFolder.getRoot());

        Directory dir = factory.newInstance(defn, idx, ":data", false);
        dir.close();

        IndexRootDirectory idxDir = new IndexRootDirectory(temporaryFolder.getRoot());
        List<LocalIndexDir> indexes = idxDir.getAllLocalIndexes();
        assertEquals(1, indexes.size());
        assertEquals("/fooIndex", indexes.get(0).getJcrPath());
        assertTrue(new File(indexes.get(0).dir, "data").exists());
    }


    @Test
    public void multiIndexWithSimilarPaths() throws Exception{
        IndexDefinition defn1 = IndexDefinition.newBuilder(root, idx.getNodeState(), "/content/a/en_us/oak:index/fooIndex").build();
        IndexDefinition defn2 = IndexDefinition.newBuilder(root, idx.getNodeState(), "/content/b/en_us/oak:index/fooIndex").build();

        FSDirectoryFactory factory = new FSDirectoryFactory(temporaryFolder.getRoot());
        factory.newInstance(defn1, idx, ":data", false).close();
        factory.newInstance(defn2, idx, ":data", false).close();

        IndexRootDirectory idxDir = new IndexRootDirectory(temporaryFolder.getRoot());
        List<LocalIndexDir> indexes = idxDir.getAllLocalIndexes();

        assertEquals(2, indexes.size());
        List<String> idxPaths  = indexes.stream().map (LocalIndexDir::getJcrPath).collect(Collectors.toList());
        assertThat(idxPaths, hasItems("/content/a/en_us/oak:index/fooIndex", "/content/b/en_us/oak:index/fooIndex"));
    }

    @Test
    public void reuseExistingDir() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(root, idx.getNodeState(), "/fooIndex").build();
        FSDirectoryFactory factory = new FSDirectoryFactory(temporaryFolder.getRoot());

        Directory dir = factory.newInstance(defn, idx, ":data", false);
        File fsDir1 = ((FSDirectory)dir).getDirectory();
        dir.close();

        Directory dir2 = factory.newInstance(defn, idx, ":data", false);
        File fsDir2 = ((FSDirectory)dir2).getDirectory();
        dir2.close();

        assertEquals(fsDir1, fsDir2);
        assertEquals(1, temporaryFolder.getRoot().list(DirectoryFileFilter.DIRECTORY).length);
    }

    @Test
    public void directoryMapping() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(root, idx.getNodeState(), "/fooIndex").build();
        FSDirectoryFactory factory = new FSDirectoryFactory(temporaryFolder.getRoot());

        Directory dir1 = factory.newInstance(defn, idx, ":data", false);
        dir1.close();
        Directory dir2 = factory.newInstance(defn, idx, ":some-other-data", false);
        dir2.close();

        IndexRootDirectory idxDir = new IndexRootDirectory(temporaryFolder.getRoot());
        LocalIndexDir indexDir = idxDir.getLocalIndexes("/fooIndex").get(0);

        for (File dir : indexDir.dir.listFiles((FileFilter)DirectoryFileFilter.DIRECTORY)){
            assertNotNull(indexDir.indexMeta.getJcrNameFromFSName(dir.getName()));
        }
    }

}