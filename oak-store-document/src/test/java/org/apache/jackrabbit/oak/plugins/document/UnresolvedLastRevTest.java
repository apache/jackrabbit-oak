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

import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.SYS_PROP_LEGACY_UNRESOLVED_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.FT_NAME_LEGACY_UNRESOLVED_LAST_REV;
import static org.apache.jackrabbit.oak.plugins.document.util.UtilsTest.createFeature;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * OAK-12557: child nodes must remain visible to other cluster nodes when the
 * most recent change of the parent cannot be resolved yet and older changes
 * were moved to a previous document.
 */
public class UnresolvedLastRevTest extends UnresolvedLastRevTestBase {

    private static final String SEGMENT_NAME = PathUtils.getName(SEGMENT);

    /**
     * Same shape as the production incident: {@code :data} was created by a
     * different cluster node than the one running the async indexer.
     */
    @Test
    public void childVisibleWhenParentCreatedByOtherClusterNode() throws Exception {
        createNodeStores(null);
        prepare(reader);
        commitAndSplitUnseenByReader();

        assertTrue(readerState(SEGMENT).exists());
        assertThat(readerState(DATA).getChildNodeNames(), hasItem(SEGMENT_NAME));
    }

    /**
     * {@code :data} was created by the async indexer. The split keeps the
     * creation revision of the writer, but child nodes added after it must
     * be visible as well.
     */
    @Test
    public void childVisibleWhenParentCreatedBySameClusterNode() throws Exception {
        createNodeStores(null);
        prepare(writer);
        commitAndSplitUnseenByReader();

        assertTrue(readerState(SEGMENT).exists());
        assertThat(readerState(DATA).getChildNodeNames(), hasItem(SEGMENT_NAME));
    }

    /**
     * The value of {@code dirListing} visible to the reader is in the
     * previous document created by the split. The older value written by the
     * reader itself is still in the main document and must not be returned.
     */
    @Test
    public void propertyValueFromPreviousDocument() throws Exception {
        createNodeStores(null);
        prepare(reader);
        String expected = readerState(DATA).getString(DIR_LISTING);
        commitAndSplitUnseenByReader();

        assertEquals(expected, readerState(DATA).getString(DIR_LISTING));

        readerCatchesUp();
        assertEquals(writer.getRoot().getChildNode("oak:index").getChildNode("lucene")
                .getChildNode(":data").getString(DIR_LISTING), readerState(DATA).getString(DIR_LISTING));
    }

    @Test
    public void legacyReturnsStalePropertyValue() throws Exception {
        createNodeStores(createFeature(true));
        prepare(reader);
        commitAndSplitUnseenByReader();

        // the value written by the reader when it created :data
        assertEquals("[]", readerState(DATA).getString(DIR_LISTING));
    }

    @Test
    public void lastRevisionFallsBackToReadRevision() throws Exception {
        createNodeStores(null);
        prepare(reader);
        Revision unseen = commitAndSplitUnseenByReader();

        DocumentNodeState parent = (DocumentNodeState) readerState(PathUtils.getParentPath(DATA));
        DocumentNodeState data = (DocumentNodeState) readerState(DATA);
        int writerId = writer.getClusterId();
        // the data node was read with the last revision of its parent
        assertEquals(parent.getLastRevision().getRevision(writerId),
                data.getLastRevision().getRevision(writerId));

        // once the reader is up-to-date, the last revision is precise again
        readerCatchesUp();
        data = (DocumentNodeState) readerState(DATA);
        assertEquals(unseen, data.getLastRevision().getRevision(writerId));
        assertTrue(readerState(SEGMENT).exists());
    }

    /**
     * Reproduces the bug with the legacy behavior: the last revision of
     * {@code :data} has no entry for the writer, the segment written by it is
     * invisible to the reader.
     */
    @Test
    public void legacyHidesChildWhenParentCreatedByOtherClusterNode() throws Exception {
        createNodeStores(createFeature(true));
        prepare(reader);
        commitAndSplitUnseenByReader();

        DocumentNodeState data = (DocumentNodeState) readerState(DATA);
        assertNull(data.getLastRevision().getRevision(writer.getClusterId()));
        assertFalse(readerState(SEGMENT).exists());
        assertThat(data.getChildNodeNames(), not(hasItem(SEGMENT_NAME)));

        readerCatchesUp();
        assertTrue(readerState(SEGMENT).exists());
    }

    @Test
    public void featureToggleCanBeChangedAtRuntime() throws Exception {
        Whiteboard whiteboard = new DefaultWhiteboard();
        try (Feature feature = Feature.newFeature(FT_NAME_LEGACY_UNRESOLVED_LAST_REV, whiteboard)) {
            createNodeStores(feature);
            prepare(reader);
            commitAndSplitUnseenByReader();
            FeatureToggle toggle = getToggle(whiteboard);

            assertFalse(feature.isEnabled());
            assertTrue(readerState(SEGMENT).exists());

            toggle.setEnabled(true);
            assertFalse(readerState(SEGMENT).exists());

            toggle.setEnabled(false);
            assertTrue(readerState(SEGMENT).exists());
        }
    }

    @Test
    public void systemPropertyRestoresLegacyBehavior() throws Exception {
        System.setProperty(SYS_PROP_LEGACY_UNRESOLVED_LAST_REV, "true");
        try {
            createNodeStores(null);
        } finally {
            System.clearProperty(SYS_PROP_LEGACY_UNRESOLVED_LAST_REV);
        }
        prepare(reader);
        commitAndSplitUnseenByReader();

        assertFalse(readerState(SEGMENT).exists());
    }

    @Test
    public void builderFeature() {
        Feature feature = createFeature(true);
        DocumentMK.Builder builder = builderProvider.newBuilder();
        assertNull(builder.getLegacyUnresolvedLastRevFeature());
        assertEquals(builder, builder.setLegacyUnresolvedLastRevFeature(feature));
        assertEquals(feature, builder.getLegacyUnresolvedLastRevFeature());
    }

    @Test
    public void unresolvedChangeWithoutSplitKeepsChildVisible() throws Exception {
        createNodeStores(createFeature(true));
        prepare(reader);
        readerStore.freezeCommitRoot();
        writerCommit(b -> {});

        // without a split, older changes of the writer are still local and
        // the child is visible with both the legacy and the new behavior
        assertTrue(readerState(SEGMENT).exists());
        NodeState data = readerState(DATA);
        assertThat(data.getChildNodeNames(), hasItem(SEGMENT_NAME));
    }

    private static FeatureToggle getToggle(Whiteboard whiteboard) {
        List<FeatureToggle> toggles = WhiteboardUtils.getServices(whiteboard, FeatureToggle.class,
                t -> FT_NAME_LEGACY_UNRESOLVED_LAST_REV.equals(t.getName()));
        assertEquals(1, toggles.size());
        return toggles.get(0);
    }
}
