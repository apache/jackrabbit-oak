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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalIndexDirTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void basicStuff() throws Exception{
        IndexMeta m = new IndexMeta("/a/b", 100);
        File baseDir = createDirWithIndexMetaFile(m);

        LocalIndexDir dir = new LocalIndexDir(baseDir);
        assertEquals(dir.getJcrPath(), m.indexPath);
        assertEquals(dir.getFSPath(), baseDir.getAbsolutePath());
        assertTrue(dir.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void invalidDir() throws Exception{
        new LocalIndexDir(temporaryFolder.getRoot());
    }

    @Test
    public void nonEmptyDir() throws Exception{
        IndexMeta m = new IndexMeta("/a/b", 100);
        File baseDir = createDirWithIndexMetaFile(m);
        new File(baseDir, "foo").mkdir();
        LocalIndexDir dir = new LocalIndexDir(baseDir);
        assertFalse(dir.isEmpty());
    }

    @Test
    public void comparison() throws Exception{
        LocalIndexDir dir = new LocalIndexDir(createDirWithIndexMetaFile(new IndexMeta("/a/b", 100)));
        LocalIndexDir dir2 = new LocalIndexDir(createDirWithIndexMetaFile(new IndexMeta("/a/b/c", 200)));
        LocalIndexDir dir3 = new LocalIndexDir(createDirWithIndexMetaFile(new IndexMeta("/a", 300)));

        List<LocalIndexDir> dirs = Lists.newArrayList();
        dirs.add(dir2);
        dirs.add(dir);
        dirs.add(dir3);

        Collections.sort(dirs, Collections.<LocalIndexDir>reverseOrder());

        assertEquals("/a", dirs.get(0).getJcrPath());
        assertEquals("/a/b/c", dirs.get(1).getJcrPath());
        assertEquals("/a/b", dirs.get(2).getJcrPath());
    }

    private File createDirWithIndexMetaFile(IndexMeta m) throws IOException {
        File baseDir = temporaryFolder.getRoot();
        File indexMeta = new File(baseDir, IndexRootDirectory.INDEX_METADATA_FILE_NAME);
        m.writeTo(indexMeta);
        return baseDir;
    }

}