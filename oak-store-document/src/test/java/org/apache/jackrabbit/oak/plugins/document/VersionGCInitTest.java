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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.enableDetailGC;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class VersionGCInitTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    @Before
    public void before() {
        ns = builderProvider.newBuilder().getNodeStore();
    }

    @Test
    public void lazyInitialize() throws Exception {
        DocumentStore store = ns.getDocumentStore();
        Document vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNull(vgc);

        ns.getVersionGarbageCollector().gc(1, DAYS);

        vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(vgc);
        assertEquals(0L, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP));
        assertNull(vgc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
    }

    @Test
    public void lazyInitializeWithDetailedGC() throws Exception {
        DocumentStore store = ns.getDocumentStore();
        Document vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNull(vgc);

        enableDetailGC(ns.getVersionGarbageCollector());
        long offset = SECONDS.toMillis(42);
        String id = getIdFromPath("/node");
        Revision r = new Revision(offset, 0, 1);
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setModified(op, r);
        store.createOrUpdate(NODES, op);
        VersionGCStats stats = ns.getVersionGarbageCollector().gc(1, DAYS);

        vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(vgc);
        assertEquals(stats.oldestModifiedDocTimeStamp, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP));
        assertEquals(stats.oldestModifiedDocId, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
        assertEquals(MIN_ID_VALUE, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
    }

    @Test
    public void lazyInitializeWithDetailedGCWithNoData() throws Exception {
        DocumentStore store = ns.getDocumentStore();
        Document vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNull(vgc);

        enableDetailGC(ns.getVersionGarbageCollector());
        VersionGCStats stats = ns.getVersionGarbageCollector().gc(1, DAYS);

        vgc = store.find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(vgc);
        assertEquals(stats.oldestModifiedDocTimeStamp, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP));
        assertEquals(stats.oldestModifiedDocId, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
        assertEquals(MIN_ID_VALUE, vgc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
    }
}
