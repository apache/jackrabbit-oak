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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.SplitOperations.forDocument;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VersionGCSweepTest extends AbstractTwoNodeTest {

    public VersionGCSweepTest(DocumentStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void oldSweepRevision() throws Exception {
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        // run background update and read, but no sweep
        manyChanges(ds1, "/foo");
        ds1.runBackgroundUpdateOperations();
        ds1.runBackgroundReadOperations();
        int ds1Splits = getNumSplitDocuments(store1, "/");
        assertTrue(ds1Splits > 0);

        manyChanges(ds2, "/bar");
        ds2.runBackgroundOperations();
        int ds2Splits = getNumSplitDocuments(store2, "/") - ds1Splits;
        assertTrue(ds2Splits > 0);

        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(10));
        VersionGarbageCollector gc = ds2.getVersionGarbageCollector();
        gc.gc(5, TimeUnit.MINUTES);

        // gc must collect split docs from ds2
        // but not from ds1, because its sweep revision
        // is older than the gc maxRevisionAge
        assertEquals(ds1Splits, getNumSplitDocuments(store2, "/"));
        Iterator<NodeDocument> prevs = getRootDocument(store2).getAllPreviousDocs();
        while (prevs.hasNext()) {
            for (Revision r : prevs.next().getAllChanges()) {
                assertEquals(ds1.getClusterId(), r.getClusterId());
            }
        }
    }

    private void manyChanges(DocumentNodeStore ns, String path)
            throws Exception {
        for (int i = 0; i < 100; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder nb = builder;
            for (String name : PathUtils.elements(path)) {
                nb = nb.child(name);
            }
            nb.setProperty("p", i);
            // also set property on root node. this will use
            // the root document as the commit root
            builder.setProperty(String.valueOf(path.hashCode()), i);
            TestUtils.merge(ns, builder);
            if (i % 10 == 9) {
                splitRoot(ns);
            }
        }
    }

    private void splitRoot(DocumentNodeStore ns) throws Exception {
        DocumentStore store = ns.getDocumentStore();
        NodeDocument doc = getRootDocument(store);
        List<UpdateOp> ops = forDocument(doc, ns, ns.getHeadRevision(),
                TestUtils.NO_BINARY, 2);
        assertFalse(ops.isEmpty());
        store.createOrUpdate(NODES, ops);
    }

    private static int getNumSplitDocuments(DocumentStore store, String path)
            throws Exception {
        NodeDocument doc = store.find(NODES, getIdFromPath(path));
        assertNotNull(doc);
        return Iterators.size(doc.getAllPreviousDocs());
    }
}
