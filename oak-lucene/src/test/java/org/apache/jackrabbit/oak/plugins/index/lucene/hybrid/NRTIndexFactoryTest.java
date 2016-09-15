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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NRTIndexFactoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    private IndexCopier indexCopier;
    private NRTIndexFactory indexFactory;

    @Before
    public void setUp() throws IOException {
        indexCopier = new IndexCopier(sameThreadExecutor(), temporaryFolder.getRoot());
        indexFactory = new NRTIndexFactory(indexCopier);
    }

    @Test
    public void noIndexForAsync() throws Exception{
        IndexDefinition idxDefn = new IndexDefinition(root, builder.getNodeState());
        assertNull(indexFactory.createIndex(idxDefn));
    }

    @Test
    public void indexCreationNRT() throws Exception{
        IndexDefinition idxDefn = getIndexDefinition("/foo", IndexingMode.SYNC);

        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        assertNotNull(idx1);
        assertEquals(1, indexFactory.getIndexes("/foo").size());
    }

    @Test
    public void indexCreationSync() throws Exception{
        IndexDefinition idxDefn = getNRTIndexDefinition("/foo");

        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        assertNotNull(idx1);
        assertEquals(1, indexFactory.getIndexes("/foo").size());
    }

    @Test
    public void indexCreationAndCloser() throws Exception{
        IndexDefinition idxDefn = getNRTIndexDefinition("/foo");

        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        assertNotNull(idx1);
        assertEquals(1, indexFactory.getIndexes("/foo").size());

        NRTIndex idx2 = indexFactory.createIndex(idxDefn);
        assertEquals(2, indexFactory.getIndexes("/foo").size());
        assertFalse(idx1.isClosed());

        NRTIndex idx3 = indexFactory.createIndex(idxDefn);
        assertEquals(2, indexFactory.getIndexes("/foo").size());

        //With 2 generation open the first one should be closed
        assertTrue(idx1.isClosed());
    }

    @Test
    public void closeIndexOnClose() throws Exception{
        IndexDefinition idxDefn = getNRTIndexDefinition("/foo");

        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        NRTIndex idx2 = indexFactory.createIndex(idxDefn);
        assertEquals(2, indexFactory.getIndexes("/foo").size());

        indexFactory.close();
        assertEquals(0, indexFactory.getIndexes("/foo").size());
        assertTrue(idx1.isClosed());
        assertTrue(idx2.isClosed());
    }

    private IndexDefinition getNRTIndexDefinition(String indexPath) {
       return getIndexDefinition(indexPath, IndexingMode.NRT);
    }

    private IndexDefinition getIndexDefinition(String indexPath, IndexingMode indexingMode) {
        builder.setProperty(IndexConstants.INDEX_PATH, indexPath);
        TestUtil.enableIndexingMode(builder, indexingMode);

        return new IndexDefinition(root, builder.getNodeState());
    }
}