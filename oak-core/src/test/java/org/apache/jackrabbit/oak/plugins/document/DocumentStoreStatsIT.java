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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("Duplicates")
public class DocumentStoreStatsIT extends AbstractDocumentStoreTest {
    private DocumentStoreStatsCollector stats = mock(DocumentStoreStatsCollector.class);
    @Rule
    public TestName testName = new TestName();

    public DocumentStoreStatsIT(DocumentStoreFixture dsf) {
        super(dsf);
        configureStatsCollector(stats);
    }

    @Before
    public void checkSupportedStores() throws Exception{
        assumeFalse(ds instanceof MemoryDocumentStore);
    }

    @Test
    public void create() throws Exception{
        String id = testName.getMethodName();

        UpdateOp up = new UpdateOp(id, true);
        ds.create(Collection.NODES, singletonList(up));
        removeMe.add(id);

        verify(stats).doneCreate(anyLong(), eq(Collection.NODES), eq(singletonList(id)), eq(true));
    }

    @Test
    public void findCached_Uncached() throws Exception{
        String id = testName.getMethodName();

        UpdateOp up = new UpdateOp(id, true);
        ds.create(Collection.NODES, singletonList(up));
        removeMe.add(id);

        ds.find(Collection.NODES, id);
        verify(stats).doneFindCached(eq(Collection.NODES), eq(id));

        ds.invalidateCache();
        ds.find(Collection.NODES, id);
        verify(stats).doneFindUncached(anyLong(), eq(Collection.NODES), eq(id), eq(true), eq(false));
    }

    @Test
    public void query() throws Exception{
        // create ten documents
        String base = testName.getMethodName();
        for (int i = 0; i < 10; i++) {
            String id = base + i;
            UpdateOp up = new UpdateOp(id, true);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("document with " + id + " not created", success);
            removeMe.add(id);
        }

        ds.query(Collection.NODES, base, base + "A", 5);
        verify(stats).doneQuery(anyLong(), eq(Collection.NODES), eq(base), eq(base + "A"),
                eq(false),  //indexedProperty
                eq(5) , // resultSize
                anyLong(),   //lockTime
                eq(false) //isSlaveOk
        );
    }

    @Test
    public void update() throws Exception{
        String id = testName.getMethodName();

        UpdateOp up = new UpdateOp(id, true);
        ds.create(Collection.NODES, singletonList(up));
        removeMe.add(id);

        List<String> toupdate = new ArrayList<String>();
        toupdate.add(id + "-" + UUID.randomUUID());
        toupdate.add(id);

        UpdateOp up2 = new UpdateOp(id, false);
        up2.set("foo", "bar");
        ds.update(Collection.NODES, toupdate, up2);

        verify(stats).doneUpdate(anyLong(), eq(Collection.NODES), eq(2));
    }
    
    @Test
    public void findAndModify() throws Exception{
        String id = testName.getMethodName();

        UpdateOp up = new UpdateOp(id, true);
        ds.create(Collection.NODES, singletonList(up));
        removeMe.add(id);

        DocumentStoreStatsCollector coll = mock(DocumentStoreStatsCollector.class);
        configureStatsCollector(coll);


        up = new UpdateOp(id, true);
        up.max("_modified", 122L);
        ds.findAndUpdate(Collection.NODES, up);

        verify(coll).doneFindAndModify(anyLong(), eq(Collection.NODES), eq(id), eq(false), eq(true), anyInt());
    }

    private void configureStatsCollector(DocumentStoreStatsCollector stats) {
        if (ds instanceof MongoDocumentStore){
            ((MongoDocumentStore) ds).setStatsCollector(stats);
        }
        if (ds instanceof RDBDocumentStore){
            ((RDBDocumentStore) ds).setStatsCollector(stats);
        }
    }
}
