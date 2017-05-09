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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.junit.Test;

import static org.junit.Assert.*;

public class MultiplexersLuceneTest {
    private MountInfoProvider mip = Mounts.newBuilder()
            .mount("foo", "/libs", "/apps").build();
    private Mount fooMount = mip.getMountByName("foo");
    private Mount defaultMount = mip.getDefaultMount();

    @Test
    public void suggestDir() throws Exception{
        assertEquals(LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME, MultiplexersLucene.getSuggestDirName(defaultMount));
        assertTrue(MultiplexersLucene.isSuggestIndexDirName(MultiplexersLucene.getSuggestDirName(defaultMount)));
        assertFalse(MultiplexersLucene.isSuggestIndexDirName(MultiplexersLucene.getIndexDirName(defaultMount)));
    }

    @Test
    public void suggestDirWithMount() throws Exception{
        assertEquals(":oak:mount-foo-suggest-data", MultiplexersLucene.getSuggestDirName(fooMount));
        assertTrue(MultiplexersLucene.isSuggestIndexDirName(MultiplexersLucene.getSuggestDirName(fooMount)));
    }

    @Test
    public void indexDir() throws Exception{
        assertEquals(LuceneIndexConstants.INDEX_DATA_CHILD_NAME, MultiplexersLucene.getIndexDirName(defaultMount));
        assertTrue(MultiplexersLucene.isIndexDirName(MultiplexersLucene.getIndexDirName(defaultMount)));
        assertFalse(MultiplexersLucene.isIndexDirName(MultiplexersLucene.getSuggestDirName(defaultMount)));
    }

    @Test
    public void indexDirWithMount() throws Exception{
        assertEquals(":oak:mount-foo-index-data", MultiplexersLucene.getIndexDirName(fooMount));
        assertTrue(MultiplexersLucene.isIndexDirName(MultiplexersLucene.getIndexDirName(fooMount)));
    }

}