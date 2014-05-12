/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.blob;

import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStoreTest;
import org.junit.After;
import org.junit.Before;

import com.google.common.collect.Lists;

/**
 * Tests the RDBBlobStore implementation.
 */
public class RDBBlobStoreTest extends AbstractBlobStoreTest {

    private RDBBlobStore blobStore;

    private static final String URL = System.getProperty("rdb.jdbc-url", "jdbc:h2:mem:oakblobs");

    private static final String USERNAME = System.getProperty("rdb.jdbc-user", "sa");

    private static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");

    @Before
    @Override
    public void setUp() throws Exception {
        blobStore = new RDBBlobStore(RDBDataSourceFactory.forJdbcUrl(URL, USERNAME, PASSWD));
        blobStore.setBlockSize(128);
        blobStore.setBlockSizeMin(48);
        this.store = blobStore;
        empty(blobStore);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (blobStore != null) {
            empty(blobStore);
            blobStore.close();
        }
    }

    private static void empty(RDBBlobStore blobStore) throws Exception {
        Iterator<String> iter = blobStore.getAllChunkIds(0);
        List<String> ids = Lists.newArrayList();
        while (iter.hasNext()) {
            ids.add(iter.next());
        }
        blobStore.deleteChunks(ids, 0);
    }
}
