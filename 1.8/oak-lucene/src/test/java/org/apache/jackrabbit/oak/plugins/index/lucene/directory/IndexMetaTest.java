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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class IndexMetaTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void writeTo() throws Exception {
        IndexMeta m = new IndexMeta("/a/b", 100);
        File f = temporaryFolder.newFile();
        m.writeTo(f);
        IndexMeta m2 = new IndexMeta(f);

        assertEquals(m.indexPath, m2.indexPath);
        assertEquals(m.creationTime, m2.creationTime);
    }

    @Test
    public void mapping() throws Exception{
        IndexMeta m = new IndexMeta("/a/b", 100);
        File f = temporaryFolder.newFile();
        m.addDirectoryMapping(":hidden-data", "hiddendata");

        assertEquals(":hidden-data", m.getJcrNameFromFSName("hiddendata"));
        m.writeTo(f);
        IndexMeta m2 = new IndexMeta(f);
        assertEquals(":hidden-data", m2.getJcrNameFromFSName("hiddendata"));
        assertEquals("hiddendata", m2.getFSNameFromJCRName(":hidden-data"));
    }

}