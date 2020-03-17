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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class MongoMissingLastRevSeekerTest {

    private MongoConnection c;
    private String dbName;
    private DocumentMK.Builder builder;
    private MongoDocumentStore store;
    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        c = MongoUtils.getConnection();
        assumeTrue(c != null);
        dbName = c.getDB().getName();
        MongoUtils.dropCollections(c.getDB());
        builder = new DocumentMK.Builder().setMongoDB(c.getDB());
        store = (MongoDocumentStore) builder.getDocumentStore();
        ns = builder.getNodeStore();
    }

    @After
    public void after() throws Exception {
        if (ns != null) {
            ns.dispose();
        }
        if (c != null) {
            c.close();
        }
        MongoUtils.dropCollections(dbName);
    }

    @Test
    public void missingLastRevSeeker() throws Exception {
        assertTrue(builder.createMissingLastRevSeeker() instanceof MongoMissingLastRevSeeker);
    }

    @Test
    public void completeResult() throws Exception {
        final int NUM_DOCS = 200;
        // populate the store
        List<UpdateOp> ops = Lists.newArrayList();
        for (int i = 0; i < NUM_DOCS; i++) {
            UpdateOp op = new UpdateOp(getIdFromPath("/node-" + i), true);
            NodeDocument.setModified(op, new Revision(i * 5000, 0, 1));
            ops.add(op);
        }
        assertTrue(store.create(NODES, ops));

        Set<String> ids = Sets.newHashSet();
        boolean updated = false;
        MissingLastRevSeeker seeker = builder.createMissingLastRevSeeker();
        for (NodeDocument doc : seeker.getCandidates(0)) {
            if (!updated) {
                // as soon as we have the first document, update /node-0
                UpdateOp op = new UpdateOp(getIdFromPath("/node-0"), false);
                // and push out the _modified timestamp
                NodeDocument.setModified(op, new Revision(NUM_DOCS * 5000, 0, 1));
                // even after the update the document matches the query
                assertNotNull(store.findAndUpdate(NODES, op));
                updated = true;
            }
            if (doc.getPath().startsWith("/node-")) {
                ids.add(doc.getId());
            }
        }
        // seeker must return all documents
        assertEquals(NUM_DOCS, ids.size());
    }
}
