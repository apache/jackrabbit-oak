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

import java.util.List;

import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.OakFongo;
import com.mongodb.WriteConcern;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDBExceptionTest {

    private MongoDocumentStore store;

    private String exceptionMsg;

    @Before
    public void before() throws Exception {
        OakFongo fongo = new OakFongo("fongo") {
            @Override
            protected void beforeExecuteBulkWriteOperation(boolean ordered,
                                                           Boolean bypassDocumentValidation,
                                                           List<?> writeRequests,
                                                           WriteConcern aWriteConcern) {
                maybeThrow();
            }

            @Override
            protected void beforeFindAndModify(DBObject query,
                                               DBObject fields,
                                               DBObject sort,
                                               boolean remove,
                                               DBObject update,
                                               boolean returnNew,
                                               boolean upsert) {
                maybeThrow();
            }

            @Override
            protected void beforeUpdate(DBObject q,
                                        DBObject o,
                                        boolean upsert,
                                        boolean multi,
                                        WriteConcern concern,
                                        DBEncoder encoder) {
                maybeThrow();
            }

            @Override
            protected void beforeFind(DBObject query, DBObject projection) {
                maybeThrow();
            }

            private void maybeThrow() {
                if (exceptionMsg != null) {
                    throw new MongoException(exceptionMsg);
                }
            }
        };
        store = new MongoDocumentStore(fongo.getDB("oak"),
                new DocumentMK.Builder());

    }

    @Test
    public void idInExceptionMessage() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        UpdateOp insert = new UpdateOp(id, true);
        assertTrue(store.create(Collection.NODES, singletonList(insert)));

        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setModified(op, new Revision(System.currentTimeMillis(), 0, 1));
        exceptionMsg = "findAndUpdate failed";
        try {
            store.findAndUpdate(Collection.NODES, op);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "createOrUpdate failed";
        try {
            store.createOrUpdate(Collection.NODES, op);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "createOrUpdate (multiple) failed";
        try {
            store.createOrUpdate(Collection.NODES, singletonList(op));
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertTrue(e.getMessage().contains(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        exceptionMsg = "find failed";
        try {
            store.find(Collection.NODES, id);
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString(exceptionMsg));
            assertTrue("Exception message does not contain id: '" + e.getMessage() + "'",
                    e.getMessage().contains(id));
        }

        String fromKey = Utils.getKeyLowerLimit("/foo");
        String toKey = Utils.getKeyUpperLimit("/foo");
        exceptionMsg = "query failed";
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
}
