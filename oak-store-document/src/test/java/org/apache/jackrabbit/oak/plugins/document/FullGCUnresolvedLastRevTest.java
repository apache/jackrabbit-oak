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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.FullGCMode.ALL_ORPHANS;
import static org.apache.jackrabbit.oak.plugins.document.FullGCMode.EMPTYPROPS;
import static org.apache.jackrabbit.oak.plugins.document.FullGCMode.GAP_ORPHANS;
import static org.apache.jackrabbit.oak.plugins.document.FullGCMode.GAP_ORPHANS_EMPTYPROPS;
import static org.apache.jackrabbit.oak.plugins.document.util.UtilsTest.createFeature;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * OAK-12557: FullGC must neither delete live documents as orphans nor remove
 * live properties when it runs on a cluster node, which cannot yet resolve the
 * most recent change of a parent that was just split by another cluster node.
 */
@RunWith(Parameterized.class)
public class FullGCUnresolvedLastRevTest extends UnresolvedLastRevTestBase {

    private static final String NODE_BY_READER = DATA + "/_reader";
    private static final String NODE_BY_READER_ID = Utils.getIdFromPath(NODE_BY_READER);
    private static final String PROPERTY = "byWriter";

    /** Modes that do not remove orphaned documents unless there is a gap in the ancestors. */
    private static final Set<FullGCMode> NON_ORPHAN_MODES = EnumSet.of(EMPTYPROPS, GAP_ORPHANS, GAP_ORPHANS_EMPTYPROPS);

    /** Modes that do not remove empty properties. */
    private static final Set<FullGCMode> NON_EMPTYPROPS_MODES = EnumSet.of(GAP_ORPHANS, ALL_ORPHANS);

    @Parameterized.Parameters(name = "{1}")
    public static List<Object[]> modes() {
        return IntStream.rangeClosed(1, 10)
                .mapToObj(i -> new Object[] {i, FullGCMode.getMode(i)})
                .collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public int modeValue;

    @Parameterized.Parameter(1)
    public FullGCMode mode;

    private FullGCMode originalMode;

    @Before
    public void saveMode() {
        originalMode = VersionGarbageCollector.getFullGcMode();
    }

    @After
    public void restoreMode() throws Exception {
        FieldUtils.writeStaticField(VersionGarbageCollector.class, "fullGcMode", originalMode, true);
    }

    @Test
    public void liveSegmentIsNotDeleted() throws Exception {
        prepareAndRunFullGC(null);

        assertNotNull(store.find(NODES, SEGMENT_ID));
        readerCatchesUp();
        assertTrue(readerState(SEGMENT).exists());
    }

    @Test
    public void livePropertyIsNotRemoved() throws Exception {
        prepareAndRunFullGC(null);

        assertFalse(store.find(NODES, NODE_BY_READER_ID).getLocalMap(PROPERTY).isEmpty());
        readerCatchesUp();
        assertTrue(readerState(NODE_BY_READER).hasProperty(PROPERTY));
    }

    /**
     * Reproduces the incident: with the legacy behavior FullGC deletes the
     * live segment document as an orphan.
     */
    @Test
    public void legacyDeletesLiveSegment() throws Exception {
        prepareAndRunFullGC(createFeature(true));

        boolean deleted = store.find(NODES, SEGMENT_ID) == null;
        assertEquals("live segment deleted by " + mode, !NON_ORPHAN_MODES.contains(mode), deleted);
    }

    /**
     * With the legacy behavior FullGC also removes a property written by the
     * async indexer on a node created by another cluster node.
     */
    @Test
    public void legacyRemovesLiveProperty() throws Exception {
        prepareAndRunFullGC(createFeature(true));

        NodeDocument doc = store.find(NODES, NODE_BY_READER_ID);
        assertNotNull(doc);
        boolean removed = doc.getLocalMap(PROPERTY).isEmpty();
        assertEquals("live property removed by " + mode, !NON_EMPTYPROPS_MODES.contains(mode), removed);
    }

    private void prepareAndRunFullGC(Feature legacyFeature) throws Exception {
        createNodeStores(legacyFeature);
        commit(reader, b -> child(b, NODE_BY_READER).setProperty("byReader", true));
        prepare(reader, b -> child(b, NODE_BY_READER).setProperty(PROPERTY, "live"));
        // make sure the segment is old enough for FullGC
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(2));
        commitAndSplitUnseenByReader();

        // FullGC evaluates :data for the first time after the split
        reader.getNodeCache().invalidateAll();
        reader.getNodeChildrenCache().invalidateAll();
        VersionGarbageCollector gc = new VersionGarbageCollector(reader,
                new VersionGCSupport(readerStore), true, false, true, modeValue,
                0, 1000, 10000, TimeUnit.HOURS.toMillis(1), 0);
        gc.setFullGCPaths(Set.of("/oak:index"), Set.of());
        gc.gc(1, TimeUnit.HOURS);

        NodeState data = readerState(DATA);
        assertTrue(data.exists());
    }
}
