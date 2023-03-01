/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDBExceptionTest {

    private MongoDocumentStore store;

    private String exceptionMsg;

    private MongoTestClient client;

    @BeforeClass
    public static void checkMongoAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() {
        MongoUtils.dropCollections(MongoUtils.DB);
        client = new MongoTestClient(MongoUtils.URL);
        store = new MongoDocumentStore(client, client.getDatabase(MongoUtils.DB),
                new DocumentMK.Builder());
    }

    @After
    public void after() {
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Test
    public void idInExceptionMessage() {
        String id = Utils.getIdFromPath("/foo");
        UpdateOp insert = new UpdateOp(id, true);
        assertTrue(store.create(Collection.NODES, singletonList(insert)));

        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setModified(op, new Revision(System.currentTimeMillis(), 0, 1));
        exceptionMsg = "findAndUpdate failed";
        setExceptionMsg();
        try {
            store.findAndUpdate(Collection.NODES, op);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "createOrUpdate failed";
        setExceptionMsg();
        try {
            store.createOrUpdate(Collection.NODES, op);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "createOrUpdate (multiple) failed";
        setExceptionMsg();
        try {
            store.createOrUpdate(Collection.NODES, singletonList(op));
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "find failed";
        setExceptionMsg();
        try {
            store.find(Collection.NODES, id);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        Path foo = Path.fromString("/foo");
        String fromKey = Utils.getKeyLowerLimit(foo);
        String toKey = Utils.getKeyUpperLimit(foo);
        exceptionMsg = "query failed";
        setExceptionMsg();
        try {
            store.query(Collection.NODES, fromKey, toKey, 100);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(fromKey));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(toKey));
        }
    }

    @Test
    @Ignore
    public void add16MBDoc() throws Exception {
        //DocumentStore docStore = openDocumentStore();

        UpdateOp updateOp = new UpdateOp("/", false);

        // create a 1 MB property
        char[] chars = new char[1024 * 1024];

        Arrays.fill(chars, '0');
        String content = new String(chars);

        //create more than 16MB properties
        for (int i = 0; i < 17; i++) {
            updateOp.set("property"+ Integer.toString(i), content);
        }

        int size = updateOp.toString().length();

        store.createOrUpdate(Collection.NODES, updateOp);
        NodeDocument doc = store.find(Collection.NODES, "/");
        // assertNotNull(doc);
    }

    private void setExceptionMsg() {
        client.setExceptionBeforeUpdate(exceptionMsg);
        client.setExceptionBeforeQuery(exceptionMsg);
    }
}
