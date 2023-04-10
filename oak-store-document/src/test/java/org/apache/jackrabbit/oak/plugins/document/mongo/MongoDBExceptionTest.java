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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public void createOrUpdate16MBDoc() {

        UpdateOp updateOp = new UpdateOp("/foo", true);
        updateOp = create16MBProp(updateOp);
        exceptionMsg = "Document to upsert is larger than 16777216";
        try {
            store.createOrUpdate(Collection.NODES, updateOp);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
        }
    }

    @Test
    public void update16MBDoc() {

        String docName = "/foo";
        UpdateOp updateOp = new UpdateOp(docName, true);
        updateOp = create1MBProp(updateOp);
        store.createOrUpdate(Collection.NODES, updateOp);
        updateOp = create16MBProp(updateOp);
        exceptionMsg = "Resulting document after update is larger than 16777216";
        try {
            store.createOrUpdate(Collection.NODES, updateOp);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
            assertThat(e.getMessage(), containsString(docName));
        }
    }

    @Test
    public void multiCreateOrUpdate16MBDoc() {

        List<UpdateOp> updateOps = new ArrayList<>();

        UpdateOp op = new UpdateOp("/test", true);
        op = create1MBProp(op);

        store.createOrUpdate(Collection.NODES, op);

        UpdateOp op1 = new UpdateOp("/foo", true);
        op1 = create16MBProp(op1);

        // Updating both doc with 16MB
        op = create16MBProp(op);
        updateOps.add(op);
        updateOps.add(op1);
        exceptionMsg = "Resulting document after update is larger than 16777216";

        try {
            store.createOrUpdate(Collection.NODES, updateOps);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
        }
    }

    @Test
    public void create16MBDoc() {

        List<UpdateOp> updateOps = new ArrayList<>();

        UpdateOp op1 = new UpdateOp("/test", true);
        op1 = create1MBProp(op1);

        UpdateOp op2 = new UpdateOp("/foo", false);
        op2 = create1MBProp(op2);
        op2 = create16MBProp(op2);

        updateOps.add(op1);
        updateOps.add(op2);
        String id = op2.getId();
        exceptionMsg = "Payload document size is larger than maximum of 16777216.";
        try {
            store.create(Collection.NODES, updateOps);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
        }
    }

    @Test
    public void findAndUpdate16MBDoc() throws Exception {
        UpdateOp op = new UpdateOp("/foo", true);
        op = create1MBProp(op);
        store.createOrUpdate(Collection.NODES, op);
        op = create16MBProp(op);
        exceptionMsg = "Resulting document after update is larger than 16777216";
        try {
            store.findAndUpdate(Collection.NODES, op);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
       }
    }

    private void setExceptionMsg() {
        client.setExceptionBeforeUpdate(exceptionMsg);
        client.setExceptionBeforeQuery(exceptionMsg);
    }

    private UpdateOp create1MBProp(UpdateOp op) {
        // create a 1 MB property
        String content = create1MBContent();
        op.set("property0", content);
        return op;
    }

    private UpdateOp create16MBProp(UpdateOp op) {
        // create a 1 MB property
        String content = create1MBContent();
        op.set("property0", content);

        //create 16MB property
        for (int i = 1; i < 16; i++) {
            op.set("property"+ i, content);
        }
        return op;
    }

    private String create1MBContent() {
        char[] chars = new char[1024 * 1024];
        Arrays.fill(chars, '0');
        String content = new String(chars);
        return content;
    }
}
