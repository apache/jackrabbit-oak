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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class IndexerInfoTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void basics() throws Exception{
        IndexerInfo info = new IndexerInfo(temporaryFolder.getRoot(), "foo");
        info.save();

        IndexerInfo info2 = IndexerInfo.fromDirectory(temporaryFolder.getRoot());
        assertEquals(info.checkpoint, info2.checkpoint);
    }

    @Test
    public void indexDirs() throws Exception{
        IndexerInfo info = new IndexerInfo(temporaryFolder.getRoot(), "foo");
        info.save();

        List<String> indexPaths = Arrays.asList("/foo", "/bar");
        for (String indexPath : indexPaths){
            File indexDir = new File(temporaryFolder.getRoot(), indexPath.substring(1));
            File indexMeta = new File(indexDir, IndexerInfo.INDEX_METADATA_FILE_NAME);
            Properties p = new Properties();
            p.setProperty(IndexerInfo.PROP_INDEX_PATH, indexPath);
            indexDir.mkdir();
            PropUtils.writeTo(p, indexMeta, "index info");
        }

        Map<String, File> indexes = info.getIndexes();
        assertThat(indexes.keySet(), containsInAnyOrder("/foo", "/bar"));
    }

}