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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.apache.commons.codec.binary.Hex;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_MEMORY_CACHE_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_PREV_NO_PROP_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isFullGCEnabled;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isEmbeddedVerificationEnabled;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isThrottlingEnabled;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest {

    private static final long TIME_DIFF_WARN_THRESHOLD_MILLIS = 2000L;
    private static final String LONG_PATH = "/foo/barbar/qwerty/asdfgh/zxcvbnm/adfsuyhdgjebuuuuuuupcccccccccsdb123ceeeeeeeeeeideaallthe_rifbdjhhbgksdfdght_acbsajbvcfjdnswersfb_dvhffbjrhbfhjdbfjsideacentrefgduyfwerebhjvbrhuv_fbhefhsbjasbka/adfsuyhdgjebuuuuuuupcccccccccsdb123ceeeeeeeeeeideaallthe_rifbdjhhbgksdfdght_acbsajbvcfjdnswersfb_dvhffbjrhbfhjdbfjsideacentrefgduyfwerebhjvbrhuv_fbhefhsbjasbka/adfsuyhdgjebuuuuuuupcccccccccsdb123ceeeeeeeeeeideaallthe_rifbdjhhbgksdfdght_acbsajbvcfjdnswersfb_dvhffbjrhbfhjdbfjsideacentrefgduyfwerebhjvbrhuv_fbhefhsbjasbka";

    @Test
    public void getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        assertEquals("2:p/" + r.toString() + "/0",
                Utils.getPreviousIdFor(Path.ROOT, r, 0));
        assertEquals("3:p/test/" + r.toString() + "/1",
                Utils.getPreviousIdFor(Path.fromString("/test"), r, 1));
        assertEquals("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/" + r.toString() + "/3",
                Utils.getPreviousIdFor(Path.fromString("/a/b/c/d/e/f/g/h/i/j/k/l/m"), r, 3));
    }

    @Test
    public void previousDoc() throws Exception{
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        assertTrue(Utils.isPreviousDocId(Utils.getPreviousIdFor(Path.ROOT, r, 0)));
        assertTrue(Utils.isPreviousDocId(Utils.getPreviousIdFor(Path.fromString("/a/b/c/d/e/f/g/h/i/j/k/l/m"), r, 3)));
        assertFalse(Utils.isPreviousDocId(Utils.getIdFromPath("/a/b")));
        assertFalse(Utils.isPreviousDocId("foo"));
        assertFalse(Utils.isPreviousDocId("0:"));
    }

    @Test
    public void leafPreviousDoc() throws Exception {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        assertTrue(Utils.isLeafPreviousDocId(Utils.getPreviousIdFor(Path.ROOT, r, 0)));
        assertTrue(Utils.isLeafPreviousDocId(Utils.getPreviousIdFor(Path.fromString("/a/b/c/d/e/f/g/h/i/j/k/l/m"), r, 0)));
        assertFalse(Utils.isLeafPreviousDocId(Utils.getPreviousIdFor(Path.fromString("/a/b/c/d/e/f/g/h/i/j/k/l/m"), r, 3)));
        assertFalse(Utils.isLeafPreviousDocId(Utils.getIdFromPath("/a/b")));
        assertFalse(Utils.isLeafPreviousDocId("foo"));
        assertFalse(Utils.isLeafPreviousDocId("0:"));
        assertFalse(Utils.isLeafPreviousDocId(":/0"));
    }

    @Test
    public void getParentIdFromLowerLimit() throws Exception{
        assertEquals("1:/foo",Utils.getParentIdFromLowerLimit(Utils.getKeyLowerLimit(Path.fromString("/foo"))));
        assertEquals("1:/foo",Utils.getParentIdFromLowerLimit("2:/foo/bar"));
    }

    @Test
    public void getParentId() throws Exception{
        Path longPath = Path.fromString(PathUtils.concat("/" + "p".repeat(Utils.PATH_LONG + 1), "foo"));
        assertTrue(Utils.isLongPath(longPath));

        assertNull(Utils.getParentId(Utils.getIdFromPath(longPath)));

        assertNull(Utils.getParentId(Utils.getIdFromPath(Path.ROOT)));
        assertEquals("1:/foo", Utils.getParentId("2:/foo/bar"));
    }

    @Test
    public void getParentIdForLongPathWithNodeNameLimit() {
        Path longPath = Path.fromString(LONG_PATH);
        assertTrue(Utils.isNodeNameLong(longPath, Utils.NODE_NAME_LIMIT));
    }

    @Test
    public void getParentIdForLongPathWithoutNodeNameLimit() {
        Path longPath = Path.fromString(LONG_PATH);
        assertFalse(Utils.isNodeNameLong(longPath, Integer.MAX_VALUE));
        assertNull(Utils.getParentId(Utils.getIdFromPath(longPath)));
    }

    @Test
    public void throttlingEnabledDefaultValue() {
        boolean throttlingEnabled = isThrottlingEnabled(newDocumentNodeStoreBuilder());
        assertFalse("Throttling is disabled by default", throttlingEnabled);
    }

    @Test
    public void throttlingExplicitlyDisabled() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setThrottlingEnabled(false);
        Feature docStoreThrottlingFeature = mock(Feature.class);
        when(docStoreThrottlingFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreThrottlingFeature(docStoreThrottlingFeature);
        boolean throttlingEnabled = isThrottlingEnabled(builder);
        assertFalse("Throttling is disabled explicitly", throttlingEnabled);
    }

    @Test
    public void throttlingEnabledViaConfiguration() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setThrottlingEnabled(true);
        Feature docStoreThrottlingFeature = mock(Feature.class);
        when(docStoreThrottlingFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreThrottlingFeature(docStoreThrottlingFeature);
        boolean throttlingEnabled = isThrottlingEnabled(builder);
        assertTrue("Throttling is enabled via configuration", throttlingEnabled);
    }

    @Test
    public void throttlingEnabledViaFeatureToggle() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setThrottlingEnabled(false);
        Feature docStoreThrottlingFeature = mock(Feature.class);
        when(docStoreThrottlingFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreThrottlingFeature(docStoreThrottlingFeature);
        boolean throttlingEnabled = isThrottlingEnabled(builder);
        assertTrue("Throttling is enabled via Feature Toggle", throttlingEnabled);
    }

    @Test
    public void fullGCEnabledDefaultValue() {
        boolean fullGCEnabled = isFullGCEnabled(newDocumentNodeStoreBuilder());
        assertFalse("Full GC is disabled by default", fullGCEnabled);
    }

    @Test
    public void fullGCExplicitlyDisabled() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setFullGCEnabled(false);
        Feature docStoreFullGCFeature = mock(Feature.class);
        when(docStoreFullGCFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreFullGCFeature(docStoreFullGCFeature);
        boolean fullGCEnabled = isFullGCEnabled(builder);
        assertFalse("Full GC is disabled explicitly", fullGCEnabled);
    }

    @Test
    public void fullGCEnabledViaConfiguration() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setFullGCEnabled(true);
        Feature docStoreFullGCFeature = mock(Feature.class);
        when(docStoreFullGCFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreFullGCFeature(docStoreFullGCFeature);
        boolean fullGCEnabled = isFullGCEnabled(builder);
        assertTrue("Full GC is enabled via configuration", fullGCEnabled);
    }

    @Test
    public void fullGCEnabledViaFeatureToggle() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setFullGCEnabled(false);
        Feature docStoreFullGCFeature = mock(Feature.class);
        when(docStoreFullGCFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreFullGCFeature(docStoreFullGCFeature);
        boolean fullGCEnabled = isFullGCEnabled(builder);
        assertTrue("Full GC is enabled via Feature Toggle", fullGCEnabled);
    }

    @Test
    public void fullGCDisabledForRDB() {
        DocumentNodeStoreBuilder<?> builder = newRDBDocumentNodeStoreBuilder();
        builder.setFullGCEnabled(true);
        Feature docStoreFullGCFeature = mock(Feature.class);
        when(docStoreFullGCFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreFullGCFeature(docStoreFullGCFeature);
        boolean fullGCEnabled = isFullGCEnabled(builder);
        assertFalse("Full GC is disabled for RDB Document Store", fullGCEnabled);
    }

    @Test
    public void fullGCModeDefaultValue() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        int fullGCModeDefaultValue = builder.getFullGCMode();
        final int fullGcModeNone = 0;
        assertEquals("Full GC mode has NONE value by default", fullGCModeDefaultValue, fullGcModeNone);
    }

    @Test
    public void fullGCModeSetViaConfiguration() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        final int fullGcModeGapOrphans = 2;
        builder.setFullGCMode(fullGcModeGapOrphans);
        int fullGCModeValue = builder.getFullGCMode();
        assertEquals("Full GC mode set correctly via configuration", fullGCModeValue, fullGcModeGapOrphans);
    }

    @Test
    public void fullGCModeHasDefaultValueForRDB() {
        DocumentNodeStoreBuilder<?> builder = newRDBDocumentNodeStoreBuilder();
        builder.setFullGCMode(3);
        int fullGCModeValue = builder.getFullGCMode();
        assertEquals("Full GC mode has default value 0 for RDB Document Store", fullGCModeValue, 0);
    }

    @Test
    public void embeddedVerificationEnabledDefaultValue() {
        boolean embeddedVerificationEnabled = isEmbeddedVerificationEnabled(newDocumentNodeStoreBuilder());
        assertTrue("Embedded Verification is enabled by default", embeddedVerificationEnabled);
    }

    @Test
    public void embeddedVerificationExplicitlyDisabled() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setEmbeddedVerificationEnabled(false);
        Feature docStoreEmbeddedVerificationFeature = mock(Feature.class);
        when(docStoreEmbeddedVerificationFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreEmbeddedVerificationFeature(docStoreEmbeddedVerificationFeature);
        boolean embeddedVerificationEnabled = isEmbeddedVerificationEnabled(builder);
        assertFalse("Embedded Verification is disabled explicitly", embeddedVerificationEnabled);
    }

    @Test
    public void embeddedVerificationEnabledViaConfiguration() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setEmbeddedVerificationEnabled(true);
        Feature docStoreEmbeddedVerificationFeature = mock(Feature.class);
        when(docStoreEmbeddedVerificationFeature.isEnabled()).thenReturn(false);
        builder.setDocStoreEmbeddedVerificationFeature(docStoreEmbeddedVerificationFeature);
        boolean embeddedVerificationEnabled = isEmbeddedVerificationEnabled(builder);
        assertTrue("Embedded Verification is enabled via configuration", embeddedVerificationEnabled);
    }

    @Test
    public void embeddedVerificationEnabledViaFeatureToggle() {
        DocumentNodeStoreBuilder<?> builder = newDocumentNodeStoreBuilder();
        builder.setEmbeddedVerificationEnabled(false);
        Feature docStoreEmbeddedVerificationFeature = mock(Feature.class);
        when(docStoreEmbeddedVerificationFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreEmbeddedVerificationFeature(docStoreEmbeddedVerificationFeature);
        boolean embeddedVerificationEnabled = isEmbeddedVerificationEnabled(builder);
        assertTrue("Embedded Verification is enabled via Feature Toggle", embeddedVerificationEnabled);
    }

    @Test
    public void embeddedVerificationDisabledForRDB() {
        DocumentNodeStoreBuilder<?> builder = newRDBDocumentNodeStoreBuilder();
        builder.setEmbeddedVerificationEnabled(true);
        Feature docStoreEmbeddedVerificationFeature = mock(Feature.class);
        when(docStoreEmbeddedVerificationFeature.isEnabled()).thenReturn(true);
        builder.setDocStoreEmbeddedVerificationFeature(docStoreEmbeddedVerificationFeature);
        boolean embeddedVerificationEnabled = isEmbeddedVerificationEnabled(builder);
        assertFalse("Embedded Verification is disabled for RDB Document Store", embeddedVerificationEnabled);
    }

    @Test
    public void prevNoPropDisabledByDefault() {
        assertPrevNoPropDisabled(newDocumentNodeStoreBuilder());
    }

    @Test
    public void prevNoPropDisabled() {
        assertPrevNoPropDisabled(newDocumentNodeStoreBuilder()
                .setPrevNoPropCacheFeature(createFeature(false)));
    }

    private void assertPrevNoPropDisabled(DocumentNodeStoreBuilder<?> builder) {
        assertNotNull(builder);
        @Nullable
        Feature feature = builder.getPrevNoPropCacheFeature();
        if (feature != null) {
            assertFalse(feature.isEnabled());
        }
        assertEquals(0, builder.getPrevNoPropCacheSize());
        assertNull(builder.buildPrevNoPropCache());
    }

    @Test
    public void prevNoPropEnabled() {
        DocumentNodeStoreBuilder<?> b =
                newDocumentNodeStoreBuilder().setPrevNoPropCacheFeature(createFeature(true));
        assertNotNull(b);
        assertTrue(b.getPrevNoPropCacheFeature().isEnabled());
        assertEquals(DEFAULT_MEMORY_CACHE_SIZE * DEFAULT_PREV_NO_PROP_CACHE_PERCENTAGE / 100,
                b.getPrevNoPropCacheSize());
        assertNotNull(b.buildPrevNoPropCache());
    }

    public static Feature createFeature(boolean enabled) {
        Feature f = mock(Feature.class);
        when(f.isEnabled()).thenReturn(enabled);
        return f;
    }

    @Test
    public void getDepthFromId() throws Exception{
        assertEquals(1, Utils.getDepthFromId("1:/x"));
        assertEquals(2, Utils.getDepthFromId("2:/x"));
        assertEquals(10, Utils.getDepthFromId("10:/x"));
    }

    @Ignore("Performance test")
    @Test
    public void performance_getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        Path path = Path.fromString("/some/test/path/foo");
        // warm up
        for (int i = 0; i < 1 * 1000 * 1000; i++) {
            Utils.getPreviousIdFor(path, r, 0);
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i < 10 * 1000 * 1000; i++) {
            Utils.getPreviousIdFor(path, r, 0);
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }

    @Ignore("Performance test")
    @Test
    public void performance_revisionToString() {
        for (int i = 0; i < 4; i++) {
            performance_revisionToStringOne();
        }
    }
    
    private static void performance_revisionToStringOne() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        int dummy = 0;
        long time = System.currentTimeMillis();
        for (int i = 0; i < 30 * 1000 * 1000; i++) {
            dummy += r.toString().length();
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time: " + time + " dummy " + dummy);
    }

    @Test
    public void max() {
        Revision a = new Revision(42, 0, 1);
        Revision b = new Revision(43, 0, 1);
        assertSame(b, Utils.max(a, b));

        Revision a1 = new Revision(42, 1, 1);
        assertSame(a1, Utils.max(a, a1));

        assertSame(a, Utils.max(a, null));
        assertSame(a, Utils.max(null, a));
        assertNull(Utils.max(null, null));
    }

    @Test
    public void min() {
        Revision a = new Revision(42, 1, 1);
        Revision b = new Revision(43, 0, 1);
        assertSame(a, Utils.min(a, b));

        Revision a1 = new Revision(42, 0, 1);
        assertSame(a1, Utils.min(a, a1));

        assertSame(a, Utils.min(a, null));
        assertSame(a, Utils.min(null, a));
        assertNull(Utils.max(null, null));
    }

    @Test
    public void getAllDocuments() throws CommitFailedException {
        DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
        try {
            NodeBuilder builder = store.getRoot().builder();
            for (int i = 0; i < 1000; i++) {
                builder.child("test-" + i);
            }
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            assertEquals(1001 /* root + 1000 children */, Iterables.size(
                    Utils.getAllDocuments(store.getDocumentStore())));
        } finally {
            store.dispose();
        }
    }

    @Test
    public void getMaxExternalRevisionTime() {
        int localClusterId = 1;
        List<Revision> revs = ImmutableList.of();
        long revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(Long.MIN_VALUE, revTime);

        revs = ImmutableList.of(Revision.fromString("r1-0-1"));
        revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(Long.MIN_VALUE, revTime);

        revs = ImmutableList.of(
                Revision.fromString("r1-0-1"),
                Revision.fromString("r2-0-2"));
        revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(2, revTime);

        revs = ImmutableList.of(
                Revision.fromString("r3-0-1"),
                Revision.fromString("r2-0-2"));
        revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(2, revTime);

        revs = ImmutableList.of(
                Revision.fromString("r1-0-1"),
                Revision.fromString("r2-0-2"),
                Revision.fromString("r2-0-3"));
        revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(2, revTime);

        revs = ImmutableList.of(
                Revision.fromString("r1-0-1"),
                Revision.fromString("r3-0-2"),
                Revision.fromString("r2-0-3"));
        revTime = Utils.getMaxExternalTimestamp(revs, localClusterId);
        assertEquals(3, revTime);
    }

    @Test
    public void getMinTimestampForDiff() {
        RevisionVector from = new RevisionVector(new Revision(17, 0, 1));
        RevisionVector to = new RevisionVector(new Revision(19, 0, 1));
        assertEquals(17, Utils.getMinTimestampForDiff(from, to, new RevisionVector()));
        assertEquals(17, Utils.getMinTimestampForDiff(to, from, new RevisionVector()));

        RevisionVector minRevs = new RevisionVector(
                new Revision(7, 0, 1),
                new Revision(4, 0, 2));
        assertEquals(17, Utils.getMinTimestampForDiff(from, to, minRevs));
        assertEquals(17, Utils.getMinTimestampForDiff(to, from, minRevs));

        to = to.update(new Revision(15, 0, 2));
        // must return min revision of clusterId 2
        assertEquals(4, Utils.getMinTimestampForDiff(from, to, minRevs));
        assertEquals(4, Utils.getMinTimestampForDiff(to, from, minRevs));

    }

    @Test(expected = IllegalArgumentException.class)
    public void getDepthFromIdIllegalArgumentException1() {
        Utils.getDepthFromId("a:/foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDepthFromIdIllegalArgumentException2() {
        Utils.getDepthFromId("42");
    }

    @Test
    public void alignWithExternalRevisions() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        // past
        Revision lastRev1 = new Revision(c.getTime() - 1000, 0, 1);
        // future
        Revision lastRev2 = new Revision(c.getTime() + 1000, 0, 2);

        // create a root document
        NodeDocument doc = new NodeDocument(new MemoryDocumentStore(), c.getTime());
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), true);
        NodeDocument.setLastRev(op, lastRev1);
        NodeDocument.setLastRev(op, lastRev2);
        UpdateUtils.applyChanges(doc, op);

        // must not wait even if revision is in the future
        Utils.alignWithExternalRevisions(doc, c, 2, TIME_DIFF_WARN_THRESHOLD_MILLIS);
        assertThat(c.getTime(), is(lessThan(lastRev2.getTimestamp())));

        // must wait until after lastRev2 timestamp
        Utils.alignWithExternalRevisions(doc, c, 1, TIME_DIFF_WARN_THRESHOLD_MILLIS);
        assertThat(c.getTime(), is(greaterThan(lastRev2.getTimestamp())));
    }

    @Test
    public void warnOnClockDifferences() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        // local
        Revision lastRev1 = new Revision(c.getTime(), 0, 1);
        // other in the future
        Revision lastRev2 = new Revision(c.getTime() + 5000, 0, 2);

        // create a root document
        NodeDocument doc = new NodeDocument(new MemoryDocumentStore(), c.getTime());
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), true);
        NodeDocument.setLastRev(op, lastRev1);
        NodeDocument.setLastRev(op, lastRev2);
        UpdateUtils.applyChanges(doc, op);

        LogCustomizer customizer = LogCustomizer.forLogger(Utils.class).enable(Level.WARN).create();
        customizer.starting();
        try {
            // must wait until after lastRev2 timestamp
            Utils.alignWithExternalRevisions(doc, c, 1, TIME_DIFF_WARN_THRESHOLD_MILLIS);
            assertThat(c.getTime(), is(greaterThan(lastRev2.getTimestamp())));

            assertFalse(customizer.getLogs().isEmpty());
            assertThat(customizer.getLogs().iterator().next(), containsString("Detected clock differences"));
        } finally {
            customizer.finished();
        }
    }

    @Test
    public void noWarnOnMinorClockDifferences() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        // local
        Revision lastRev1 = new Revision(c.getTime(), 0, 1);
        // other slightly in the future
        Revision lastRev2 = new Revision(c.getTime() + 100, 0, 2);

        // create a root document
        NodeDocument doc = new NodeDocument(new MemoryDocumentStore(), c.getTime());
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), true);
        NodeDocument.setLastRev(op, lastRev1);
        NodeDocument.setLastRev(op, lastRev2);
        UpdateUtils.applyChanges(doc, op);

        LogCustomizer customizer = LogCustomizer.forLogger(Utils.class).enable(Level.WARN).create();
        customizer.starting();
        try {
            // must wait until after lastRev2 timestamp
            Utils.alignWithExternalRevisions(doc, c, 1, TIME_DIFF_WARN_THRESHOLD_MILLIS);
            assertThat(c.getTime(), is(greaterThan(lastRev2.getTimestamp())));

            assertThat(customizer.getLogs(), empty());
        } finally {
            customizer.finished();
        }
    }

    @Test
    public void noWarnWithSingleClusterId() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        // local
        Revision lastRev1 = new Revision(c.getTime(), 0, 1);

        // create a root document
        NodeDocument doc = new NodeDocument(new MemoryDocumentStore(), c.getTime());
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), true);
        NodeDocument.setLastRev(op, lastRev1);
        UpdateUtils.applyChanges(doc, op);

        LogCustomizer customizer = LogCustomizer.forLogger(Utils.class).enable(Level.WARN).create();
        customizer.starting();
        try {
            Utils.alignWithExternalRevisions(doc, c, 1, TIME_DIFF_WARN_THRESHOLD_MILLIS);

            assertThat(customizer.getLogs(), empty());
        } finally {
            customizer.finished();
        }
    }

    @Test
    public void isIdFromLongPath() {
        Path path = Path.fromString("/test");
        while (!Utils.isLongPath(path)) {
            path = new Path(path, path.getName());
        }
        String idFromLongPath = Utils.getIdFromPath(path);
        assertTrue(Utils.isIdFromLongPath(idFromLongPath));
        assertFalse(Utils.isIdFromLongPath("foo"));
        assertFalse(Utils.isIdFromLongPath(NodeDocument.MIN_ID_VALUE));
        assertFalse(Utils.isIdFromLongPath(NodeDocument.MAX_ID_VALUE));
        assertFalse(Utils.isIdFromLongPath(":"));
    }

    @Test
    public void idDepth() {
        assertEquals(0, Utils.getIdDepth(Path.ROOT));
        assertEquals(0, Utils.getIdDepth(Path.fromString("a")));
        assertEquals(1, Utils.getIdDepth(Path.fromString("/a")));
        assertEquals(2, Utils.getIdDepth(Path.fromString("/a/b")));
        assertEquals(3, Utils.getIdDepth(Path.fromString("/a/b/c")));
        assertEquals(2, Utils.getIdDepth(Path.fromString("a/b/c")));
    }

    @Test
    public void encodeHexString() {
        Random r = new Random(42);
        for (int i = 0; i < 1000; i++) {
            int len = r.nextInt(100);
            byte[] data = new byte[len];
            r.nextBytes(data);
            // compare against commons codec implementation
            assertEquals(Hex.encodeHexString(data),
                    Utils.encodeHexString(data, new StringBuilder()).toString());
        }
    }

    @Test
    public void isLocalChange() {
        RevisionVector empty = new RevisionVector();
        Revision r11 = Revision.fromString("r1-0-1");
        Revision r21 = Revision.fromString("r2-0-1");
        Revision r12 = Revision.fromString("r1-0-2");
        Revision r22 = Revision.fromString("r2-0-2");

        assertFalse(Utils.isLocalChange(empty, empty, 1));
        assertTrue(Utils.isLocalChange(empty, new RevisionVector(r11), 1));
        assertFalse(Utils.isLocalChange(empty, new RevisionVector(r11), 0));
        assertFalse(Utils.isLocalChange(new RevisionVector(r11), new RevisionVector(r11), 1));
        assertTrue(Utils.isLocalChange(new RevisionVector(r11), new RevisionVector(r21), 1));
        assertFalse(Utils.isLocalChange(new RevisionVector(r11), new RevisionVector(r11, r12), 1));
        assertFalse(Utils.isLocalChange(new RevisionVector(r11, r12), new RevisionVector(r11, r12), 1));
        assertFalse(Utils.isLocalChange(new RevisionVector(r11, r12), new RevisionVector(r11, r22), 1));
        assertFalse(Utils.isLocalChange(new RevisionVector(r11, r12), new RevisionVector(r21, r22), 1));
        assertTrue(Utils.isLocalChange(new RevisionVector(r11, r12), new RevisionVector(r21, r12), 1));
    }

    @Test
    public void abortingIterableIsCloseable() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        Iterable<String> iterable = CloseableIterable.wrap(
                Collections.emptyList(), () -> closed.set(true));
        Utils.closeIfCloseable(Utils.abortingIterable(iterable, s -> true));
        assertTrue(closed.get());
    }

    @Test
    public void checkRevisionAge() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo info = mock(ClusterNodeInfo.class);
        when(info.getId()).thenReturn(2);

        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());

        // store is empty -> fine
        Utils.checkRevisionAge(store, info, clock);

        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), true);
        NodeDocument.setLastRev(op, new Revision(clock.getTime(), 0, 1));
        assertTrue(store.create(Collection.NODES, Collections.singletonList(op)));

        // root document does not have a lastRev entry for clusterId 2
        Utils.checkRevisionAge(store, info, clock);

        long lastRevTime = clock.getTime();
        op = new UpdateOp(Utils.getIdFromPath("/"), false);
        NodeDocument.setLastRev(op, new Revision(lastRevTime, 0, 2));
        assertNotNull(store.findAndUpdate(Collection.NODES, op));

        // lastRev entry for clusterId 2 is older than current time
        Utils.checkRevisionAge(store, info, clock);

        // rewind time
        clock = new Clock.Virtual();
        clock.waitUntil(lastRevTime - 1000);
        try {
            // now the check must fail
            Utils.checkRevisionAge(store, info, clock);
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            assertThat(e.getMessage(), containsString("newer than current time"));
        }
    }

    @Test
    public void getStartRevisionsEmpty() {
        RevisionVector rv = Utils.getStartRevisions(Collections.emptyList());
        assertEquals(0, rv.getDimensions());
    }

    @Test
    public void getStartRevisionsSingleNode() {
        int clusterId = 1;
        long now = System.currentTimeMillis();
        ClusterNodeInfoDocument info = mockedClusterNodeInfo(clusterId, now);
        RevisionVector rv = Utils.getStartRevisions(Collections.singleton(info));
        assertEquals(1, rv.getDimensions());
        Revision r = rv.getRevision(clusterId);
        assertNotNull(r);
        assertEquals(now, r.getTimestamp());
    }

    @Test
    public void getStartRevisionsMultipleNodes() {
        int clusterId1 = 1;
        int clusterId2 = 2;
        long startTime1 = System.currentTimeMillis();
        long startTime2 = startTime1 + 1000;
        ClusterNodeInfoDocument info1 = mockedClusterNodeInfo(clusterId1, startTime1);
        ClusterNodeInfoDocument info2 = mockedClusterNodeInfo(clusterId2, startTime2);
        RevisionVector rv = Utils.getStartRevisions(Arrays.asList(info1, info2));
        assertEquals(2, rv.getDimensions());
        Revision r1 = rv.getRevision(clusterId1);
        assertNotNull(r1);
        Revision r2 = rv.getRevision(clusterId2);
        assertNotNull(r2);
        assertEquals(startTime1, r1.getTimestamp());
        assertEquals(startTime2, r2.getTimestamp());
    }

    @Test
    public void sum() {
        assertEquals(0, Utils.sum());
        assertEquals(42, Utils.sum(7, 15, 20));
        assertEquals(-12, Utils.sum(-7, 15, -20));
        assertEquals(42, Utils.sum(Long.MAX_VALUE, 43, Long.MIN_VALUE));

        assertEquals(Long.MAX_VALUE, Utils.sum(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE - 1, Utils.sum(Long.MAX_VALUE, -1));
        assertEquals(Long.MAX_VALUE, Utils.sum(Long.MAX_VALUE, 1));

        assertEquals(Long.MIN_VALUE, Utils.sum(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE + 1, Utils.sum(Long.MIN_VALUE, 1));
        assertEquals(Long.MIN_VALUE, Utils.sum(Long.MIN_VALUE, -1));
    }

    private static ClusterNodeInfoDocument mockedClusterNodeInfo(int clusterId,
                                                                 long startTime) {
        ClusterNodeInfoDocument info = Mockito.mock(ClusterNodeInfoDocument.class);
        Mockito.when(info.getClusterId()).thenReturn(clusterId);
        Mockito.when(info.getStartTime()).thenReturn(startTime);
        return info;
    }
}
