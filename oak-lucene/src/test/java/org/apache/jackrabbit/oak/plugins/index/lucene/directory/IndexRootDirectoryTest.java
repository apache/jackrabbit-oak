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
import java.util.List;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class IndexRootDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private IndexRootDirectory dir;

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();

    @Before
    public void setUp() throws IOException {
        dir = new IndexRootDirectory(temporaryFolder.getRoot());
    }

    @Test
    public void getIndexDirOldFormat() throws Exception{
        File f1 = dir.getIndexDir(getDefn(), "/a/b");
        assertFalse(LocalIndexDir.isIndexDir(f1));

        builder.setProperty(IndexConstants.REINDEX_COUNT, 1);
        File f2 = dir.getIndexDir(getDefn(), "/a/b");
        File f3 = dir.getIndexDir(getDefn(), "/a/b");
        //Both should be same dir
        assertEquals(f2, f3);

        assertEquals(2, f2.getParentFile().list().length);
    }

    @Test
    public void newFormat() throws Exception{
        LuceneIndexEditorContext.configureUniqueId(builder);
        File f1 = dir.getIndexDir(getDefn(), "/a/b");
        File f2 = dir.getIndexDir(getDefn(), "/a/b");
        assertEquals(f1, f2);
    }

    @Test
    public void reindexCaseWithSamePath() throws Exception{
        LuceneIndexEditorContext.configureUniqueId(builder);
        File f1 = dir.getIndexDir(getDefn(), "/a/b");

        LuceneIndexEditorContext.configureUniqueId(resetBuilder());
        File f2 = dir.getIndexDir(getDefn(), "/a/b");

        assertNotEquals(f1, f2);
        List<LocalIndexDir> dirs = dir.getLocalIndexes("/a/b");

        //First one should be F2 as it got created later
        assertEquals(f2.getParentFile().getAbsolutePath(), dirs.get(0).getFSPath());

        assertTrue(dir.getLocalIndexes("/a/b/c").isEmpty());
    }

    @Test
    public void allLocalIndexes() throws Exception{
        LuceneIndexEditorContext.configureUniqueId(builder);
        File fa1 = dir.getIndexDir(getDefn(), "/a");
        LuceneIndexEditorContext.configureUniqueId(resetBuilder());
        File fa2 = dir.getIndexDir(getDefn(), "/a");

        LuceneIndexEditorContext.configureUniqueId(builder);
        File fb1 = dir.getIndexDir(getDefn(), "/b");
        LuceneIndexEditorContext.configureUniqueId(resetBuilder());
        File fb2 = dir.getIndexDir(getDefn(), "/b");

        List<LocalIndexDir> dirs = dir.getAllLocalIndexes();
        assertEquals(2, dirs.size());

        assertEquals(fb2.getParentFile().getAbsolutePath(), getDir("/b", dirs).getFSPath());
        assertEquals(fa2.getParentFile().getAbsolutePath(), getDir("/a", dirs).getFSPath());
    }

    @Test
    public void indexFolderName() throws Exception{
        assertEquals("abc", IndexRootDirectory.getIndexFolderBaseName("/abc"));
        assertEquals("abc12", IndexRootDirectory.getIndexFolderBaseName("/abc12"));
        assertEquals("xyabc12", IndexRootDirectory.getIndexFolderBaseName("/xy:abc12"));
        assertEquals("xyabc12", IndexRootDirectory.getIndexFolderBaseName("/xy:abc#^&12"));
        assertEquals("xyabc12", IndexRootDirectory.getIndexFolderBaseName("/oak:index/xy:abc12"));
        assertEquals("content_xyabc12", IndexRootDirectory.getIndexFolderBaseName("/content/oak:index/xy:abc12"));
        assertEquals("sales_xyabc12", IndexRootDirectory.getIndexFolderBaseName("/content/sales/oak:index/xy:abc12"));
        assertEquals("appsales_xyabc12", IndexRootDirectory.getIndexFolderBaseName
                ("/content/app:sales/oak:index/xy:abc12"));
    }

    @Test
    public void longFileName() throws Exception{
        String longName = Strings.repeat("x", IndexRootDirectory.MAX_NAME_LENGTH);
        assertEquals(longName, IndexRootDirectory.getIndexFolderBaseName(longName));

        String longName2 = Strings.repeat("x", IndexRootDirectory.MAX_NAME_LENGTH + 10);
        assertEquals(longName, IndexRootDirectory.getIndexFolderBaseName(longName2));
    }

    @Test
    public void gcIndexDirs() throws Exception{
        //Create an old format directory
        File fa0 = dir.getIndexDir(getDefn(), "/a");

        configureUniqueId();
        File fa1 = dir.getIndexDir(getDefn(), "/a");
        configureUniqueId();
        File fa2 = dir.getIndexDir(getDefn(), "/a");

        List<LocalIndexDir> dirs = dir.getLocalIndexes("/a");
        assertEquals(2, dirs.size());

        dir.gcEmptyDirs(fa2);
        //No index dir should be removed. Even empty ones
        assertEquals(2, dirs.size());

        configureUniqueId();
        File fa3 = dir.getIndexDir(getDefn(), "/a");

        assertEquals(3, dir.getLocalIndexes("/a").size());

        //Dir size should still be 3 as non empty dir cannot be be collected
        dir.gcEmptyDirs(fa3);
        assertEquals(3, dir.getLocalIndexes("/a").size());

        //Now get rid of 'default' for the first localDir dir i.e. fa1
        FileUtils.deleteQuietly(fa1);
        dir.gcEmptyDirs(fa1);

        assertEquals(2, dir.getLocalIndexes("/a").size());
        assertFalse(fa0.exists());

        FileUtils.deleteDirectory(fa2);
        FileUtils.deleteDirectory(fa3);

        //Note that we deleted both fa2 and fa3 but GC was done based on fa2 (fa2 < fa3)
        //So only dirs which are of same time or older than fa2 would be removed. So in this
        //case fa3 would be left
        dir.gcEmptyDirs(fa2);
        assertEquals(1, dir.getLocalIndexes("/a").size());
    }

    @Test
    public void gcIndexDirsOnStart() throws Exception{
        File fa0 = dir.getIndexDir(getDefn(), "/a");

        configureUniqueId();
        File fa1 = dir.getIndexDir(getDefn(), "/a");
        configureUniqueId();
        File fa2 = dir.getIndexDir(getDefn(), "/a");
        assertEquals(2, dir.getLocalIndexes("/a").size());

        //Now reinitialize
        dir = new IndexRootDirectory(temporaryFolder.getRoot());

        assertFalse(fa0.exists());
        assertFalse(fa1.exists());
        assertTrue(fa2.exists());
        assertEquals(1, dir.getLocalIndexes("/a").size());
    }

    private NodeBuilder resetBuilder() {
        builder = EMPTY_NODE.builder();
        return builder;
    }

    private void configureUniqueId(){
        LuceneIndexEditorContext.configureUniqueId(resetBuilder());
    }

    private IndexDefinition getDefn(){
        return new IndexDefinition(root, builder.getNodeState());
    }

    private static LocalIndexDir getDir(String jcrPath, List<LocalIndexDir> dirs){
        for (LocalIndexDir dir : dirs){
            if (dir.getJcrPath().equals(jcrPath)){
                return dir;
            }
        }
        return null;
    }

}