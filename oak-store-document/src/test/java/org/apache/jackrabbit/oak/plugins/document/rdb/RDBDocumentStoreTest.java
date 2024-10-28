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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;
import org.slf4j.event.Level;

import org.apache.jackrabbit.guava.common.collect.Lists;

public class RDBDocumentStoreTest extends AbstractDocumentStoreTest {

    public RDBDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testRDBQueryConditions() {
        if (ds instanceof RDBDocumentStore) {
            RDBDocumentStore rds = (RDBDocumentStore) ds;
            // create ten documents
            long now = System.currentTimeMillis();
            String base = this.getClass().getName() + ".testRDBQuery-";
            for (int i = 0; i < 10; i++) {
                String id = base + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set(NodeDocument.DELETED_ONCE, i % 2 == 1);
                up.set(NodeDocument.MODIFIED_IN_SECS, now++);
                boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
                assertTrue("document with " + id + " not created", success);
                removeMe.add(id);
            }

            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            // matches every second
            conditions.add(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 0));
            // matches first eight
            conditions.add(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, "<", now - 2));
            List<NodeDocument> result = rds.query(Collection.NODES, base, base + "A", RDBDocumentStore.EMPTY_KEY_PATTERN,
                    conditions, 10);
            assertEquals(4, result.size());
        }
    }

    @Test
    public void testRDBQueryKeyPatterns() {
        if (ds instanceof RDBDocumentStore) {
            int cnt = 10;
            RDBDocumentStore rds = (RDBDocumentStore) ds;
            // create ten documents
            String base = this.getClass().getName() + ".testRDBQuery-";
            for (int i = 0; i < cnt; i++) {
                // every second is a "regular" path
                String id = "1:" + (i % 2 == 1 ? "p" : "") + "/" + base + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_test", base);
                boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
                assertTrue("document with " + id + " not created", success);
                removeMe.add(id);
            }

            List<QueryCondition> conditions = new ArrayList<QueryCondition>();
            List<NodeDocument> result = rds.query(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE,
                    Arrays.asList("_:/%", "__:/%", "___:/%"), conditions, 10000);
            for (NodeDocument d : result) {
                if (base.equals(d.get("_test"))) {
                    assertTrue(d.getId().startsWith("1:p"));
                }
            }

            Iterable<NodeDocument> it = rds.queryAsIterable(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE,
                    Arrays.asList("_:/%", "__:/%", "___:/%"), conditions, Integer.MAX_VALUE, null);
            assertTrue(it instanceof Closeable);

            int c1 = 0, c2 = 0;
            for (NodeDocument d : it) {
                if (base.equals(d.get("_test"))) {
                    assertTrue(d.getId().startsWith("1:p"));
                    c1 += 1;
                }
            }
            // check that getting the iterator twice works
            for (NodeDocument d : it) {
                if (base.equals(d.get("_test"))) {
                    assertTrue(d.getId().startsWith("1:p"));
                    c2 += 1;
                }
            }
            assertEquals(cnt / 2, c1);
            assertEquals(cnt / 2, c2);

            Utils.closeIfCloseable(it);
        }
    }

    @Test
    public void testRDBStats() {
        if (ds instanceof RDBDocumentStore) {
            Map<String, String> info = ds.getStats();
            assertThat(info.keySet(), hasItem("nodes.ns"));
            assertThat(info.keySet(), hasItem("clusterNodes.ns"));
            assertThat(info.keySet(), hasItem("journal.ns"));
            assertThat(info.keySet(), hasItem("settings.ns"));
            assertThat(info.keySet(), hasItem("nodes.count"));
            assertThat(info.keySet(), hasItem("clusterNodes.count"));
            assertThat(info.keySet(), hasItem("journal.count"));
            assertThat(info.keySet(), hasItem("settings.count"));
            // info.forEach((k, v) -> System.err.println(k +": " + v));
        }
    }

    @Test
    public void testRDBJDBCPerfLog() {
        if (ds instanceof RDBDocumentStore) {
            LogCustomizer logCustomizerRead = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName() + ".perf")
                    .enable(Level.TRACE).matchesRegex("read: .*").create();
            logCustomizerRead.starting();
            LogCustomizer logCustomizerQuery = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName() + ".perf")
                    .enable(Level.TRACE).matchesRegex("quer.*").create();
            logCustomizerQuery.starting();

            try {
                String id1 = Utils.getIdFromPath("/testRDBJDBCPerfLog");
                String id2 = Utils.getIdFromPath("/testRDBJDBCPerfLog/foo");
                UpdateOp up1 = new UpdateOp(id1, true);
                up1.set(NodeDocument.MODIFIED_IN_SECS, 12345L);
                super.removeMe.add(id1);
                super.ds.create(Collection.NODES, Collections.singletonList(up1));
                super.ds.invalidateCache();
                super.ds.find(Collection.NODES, id1);
                int count = logCustomizerRead.getLogs().size();
                assertTrue(count > 0);
                super.ds.find(Collection.NODES, id1);
                assertEquals("no new log entry expected but got: " + logCustomizerRead.getLogs(), count,
                        logCustomizerRead.getLogs().size());
                count = logCustomizerRead.getLogs().size();

                // add a child node
                UpdateOp up2 = new UpdateOp(id2, true);
                up1.set(NodeDocument.MODIFIED_IN_SECS, 12346L);
                super.removeMe.add(id2);
                super.ds.create(Collection.NODES, Collections.singletonList(up2));

                // query
                Path p = Path.fromString("/testRDBJDBCPerfLog");
                List<NodeDocument> results = super.ds.query(Collection.NODES, Utils.getKeyLowerLimit(p), Utils.getKeyUpperLimit(p), 10);
                assertEquals(1, results.size());
                assertEquals(2, logCustomizerQuery.getLogs().size());
            } finally {
                logCustomizerRead.finished();
                logCustomizerQuery.finished();
            }
        }
    }

    // stolen from MongoMissingLastRevSeekerTest
    @Test
    public void completeResult() throws Exception {
        if (ds instanceof RDBDocumentStore) {
            final int NUM_DOCS = 200;
            // populate the store
            List<UpdateOp> ops = new ArrayList<>();
            for (int i = 0; i < NUM_DOCS; i++) {
                UpdateOp op = new UpdateOp(getIdFromPath("/lastRevnode-" + i), true);
                NodeDocument.setModified(op, new Revision(i * 5000, 0, 1));
                ops.add(op);
                removeMe.add(op.getId());
            }
            assertTrue(ds.create(NODES, ops));

            Set<String> ids = new HashSet<>();
            boolean updated = false;
            MissingLastRevSeeker seeker = new RDBMissingLastRevSeeker((RDBDocumentStore) ds, Clock.SIMPLE);
            for (NodeDocument doc : seeker.getCandidates(0)) {
                if (!updated) {
                    // as soon as we have the first document, update
                    // /lastRevnode-0
                    UpdateOp op = new UpdateOp(getIdFromPath("/lastRevnode-0"), false);
                    // and push out the _modified timestamp
                    NodeDocument.setModified(op, new Revision(NUM_DOCS * 5000, 0, 1));
                    // even after the update the document matches the query
                    assertNotNull(ds.findAndUpdate(NODES, op));
                    updated = true;
                }
                if (doc.getPath().toString().startsWith("/lastRevnode-")) {
                    ids.add(doc.getId());
                }
            }
            // seeker must return all documents
            assertEquals(NUM_DOCS, ids.size());
        }
    }

    @Test
    public void testAppendStringColumnLimit() {
        if (ds instanceof RDBDocumentStore) {
            String id = this.getClass().getName() + ".testAppendStringColumnLimit";
            UpdateOp up = new UpdateOp(id, true);
            assertTrue(ds.create(Collection.NODES, Collections.singletonList(up)));
            removeMe.add(id);
            int count = 1;
            long duration = 1000;
            long end = System.currentTimeMillis() + duration;
            while (System.currentTimeMillis() < end) {
                UpdateOp op = new UpdateOp(id, false);
                String value = generateString(512, true);
                op.set("foo-" + count++, value);
                assertNotNull(ds.findAndUpdate(NODES, op));
            }
        }
    }
}
