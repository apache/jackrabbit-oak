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

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest {

    @Test
    public void getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        assertEquals("2:p/" + r.toString() + "/0",
                Utils.getPreviousIdFor("/", r, 0));
        assertEquals("3:p/test/" + r.toString() + "/1",
                Utils.getPreviousIdFor("/test", r, 1));
        assertEquals("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/" + r.toString() + "/3",
                Utils.getPreviousIdFor("/a/b/c/d/e/f/g/h/i/j/k/l/m", r, 3));
    }

    @Test
    public void getParentIdFromLowerLimit() throws Exception{
        assertEquals("1:/foo",Utils.getParentIdFromLowerLimit(Utils.getKeyLowerLimit("/foo")));
        assertEquals("1:/foo",Utils.getParentIdFromLowerLimit("2:/foo/bar"));
    }

    @Test
    public void getParentId() throws Exception{
        String longPath = PathUtils.concat("/"+Strings.repeat("p", Utils.PATH_LONG + 1), "foo");
        assertTrue(Utils.isLongPath(longPath));

        assertNull(Utils.getParentId(Utils.getIdFromPath(longPath)));

        assertNull(Utils.getParentId(Utils.getIdFromPath("/")));
        assertEquals("1:/foo",Utils.getParentId("2:/foo/bar"));
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
        String path = "/some/test/path/foo";
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
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        // warm up
        for (int i = 0; i < 1 * 1000 * 1000; i++) {
            r.toString();
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i < 30 * 1000 * 1000; i++) {
            r.toString();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
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
}
