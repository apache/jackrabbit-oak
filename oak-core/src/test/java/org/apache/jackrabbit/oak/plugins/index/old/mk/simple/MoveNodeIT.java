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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import junit.framework.Assert;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * Test moving nodes. These tests currently only work against the
 * {@link SimpleKernelImpl} implementation as they assume a specific
 * ordering of child nodes that isn't guaranteed by the
 * {@link MicroKernel} contract.
 */
public class MoveNodeIT {

    private final MicroKernel mk = new SimpleKernelImpl("mem:SimpleKernelTest");

    private String head;
    private String journalRevision;

    @Before
    public void setUp() throws Exception {
        head = mk.getHeadRevision();
        commit("/", "+ \"test\": {\"a\":{}, \"b\":{}, \"c\":{}}");
        commit("/", "+ \"test2\": {}");
        getJournal();
    }

    // TODO fix test since it incorrectly expects a specific order of child nodes
    @Test
    public void addProperty() {
        // add a property /test/c
        commit("/", "+ \"test/c\": 123");
        Assert.assertEquals("{c:123,a,b,c}", getNode("/test"));
        assertJournal("+\"/test/c\":123");
    }

    @Test
    public void addPropertyTwice() {
        commit("/", "+ \"test/c\": 123");

        // duplicate add property can fail
        // TODO document that both is fine
        try {
            commit("/", "+ \"test/c\": 123");
            Assert.fail();
        } catch (MicroKernelException e) {
            // expected
        }
        Assert.assertEquals("{c:123,a,b,c}", getNode("/test"));
    }

    // TODO fix test since it incorrectly expects a specific order of child nodes
    @Test
    public void order() {
        Assert.assertEquals("{a,b,c}", getNode("/test"));
    }

    // TODO fix test since it incorrectly expects a specific order of child nodes
    @Test
    public void rename() {
        // rename /test/b
        commit("/", "> \"test/b\": \"test/b1\"");
        Assert.assertEquals("{a,b1,c}", getNode("/test"));
        assertJournal(">\"/test/b\":\"/test/b1\"");

        // and back
        commit("/", "> \"test/b1\": \"test/b\"");
        Assert.assertEquals("{a,b,c}", getNode("/test"));
        assertJournal(">\"/test/b1\":\"/test/b\"");
    }

    @Test
    public void reorderBefore() {
        // order c before b
        commit("/", "> \"test/c\": {\"before\": \"test/b\"}");
        Assert.assertEquals("{a,c,b}", getNode("/test"));
        assertJournal(">\"/test/c\":{\"before\":\"/test/b\"}");

        // and now b before a
        commit("/", "> \"test/b\": {\"before\": \"test/a\"}");
        Assert.assertEquals("{b,a,c}", getNode("/test"));
        assertJournal(">\"/test/b\":{\"before\":\"/test/a\"}");
    }

    @Test
    public void reorderAfter() {
        // order a after b
        commit("/", "> \"test/a\": {\"after\": \"test/b\"}");
        Assert.assertEquals("{b,a,c}", getNode("/test"));
        assertJournal(">\"/test/a\":{\"after\":\"/test/b\"}");

        // and now a after c
        commit("/", "> \"test/a\": {\"after\": \"test/c\"}");
        Assert.assertEquals("{b,c,a}", getNode("/test"));
        assertJournal(">\"/test/a\":{\"after\":\"/test/c\"}");

        // and now a after a (a no-op)
        commit("/", "> \"test/a\": {\"after\": \"test/a\"}");
        Assert.assertEquals("{b,c,a}", getNode("/test"));
        assertJournal(">\"/test/a\":{\"after\":\"/test/a\"}");
    }

    @Test
    public void moveFirst() {
        // move /test/a to /test2/a (rename is not supported in this way)
        commit("/", "> \"test/a\": {\"first\": \"test2\"}");
        Assert.assertEquals("{b,c}", getNode("/test"));
        Assert.assertEquals("{a}", getNode("/test2"));
        assertJournal(">\"/test/a\":{\"first\":\"/test2\"}");

        // move /test/c to /test2
        commit("/", "> \"test/c\": {\"first\": \"test2\"}");
        Assert.assertEquals("{b}", getNode("/test"));
        Assert.assertEquals("{c,a}", getNode("/test2"));
        assertJournal(">\"/test/c\":{\"first\":\"/test2\"}");
    }

    @Test
    public void moveCombinedWithSet() {
        // move /test/b to /test_b
        commit("/", "> \"test/b\": \"test_b\"");
        Assert.assertEquals("{a,c}", getNode("/test"));
        Assert.assertEquals("{}", getNode("/test_b"));
        assertJournal(">\"/test/b\":\"/test_b\"");

        // move /test/a to /test_a, combined with adding a property
        commit("/", "> \"test/a\": \"test_a\" ^ \"test_a/x\": 1");
        Assert.assertEquals("{c}", getNode("/test"));
        Assert.assertEquals("{x:1}", getNode("/test_a"));
        assertJournal(
                ">\"/test/a\":\"/test_a\"\n"+
                "+\"/test_a/x\":1");
    }

    @Test
    public void moveBefore() {
        // move /test/b to /test2/b, before any other nodes in /test2
        commit("/", "> \"test/b\": {\"first\": \"test2\"}");
        Assert.assertEquals("{a,c}", getNode("/test"));
        Assert.assertEquals("{b}", getNode("/test2"));
        assertJournal(">\"/test/b\":{\"first\":\"/test2\"}");

        // move /test/c to /test2, before b
        commit("/", "> \"test/c\": {\"before\": \"test2/b\"}");
        Assert.assertEquals("{a}", getNode("/test"));
        Assert.assertEquals("{c,b}", getNode("/test2"));
        assertJournal(">\"/test/c\":{\"before\":\"/test2/b\"}");
    }

    @Test
    public void moveAfter() {
        // move /test/c to /test2
        commit("/", "> \"test/c\": \"test2/c\"");
        Assert.assertEquals("{a,b}", getNode("/test"));
        Assert.assertEquals("{c}", getNode("/test2"));
        assertJournal(">\"/test/c\":\"/test2/c\"");

        // move /test/a to /test2, after c
        commit("/", "> \"test/a\": {\"after\": \"test2/c\"}");
        Assert.assertEquals("{b}", getNode("/test"));
        Assert.assertEquals("{c,a}", getNode("/test2"));
        assertJournal(">\"/test/a\":{\"after\":\"/test2/c\"}");

        // move /test/b to /test2, after c
        commit("/", "> \"test/b\": {\"after\": \"test2/c\"}");
        Assert.assertEquals("{}", getNode("/test"));
        Assert.assertEquals("{c,b,a}", getNode("/test2"));
        assertJournal(">\"/test/b\":{\"after\":\"/test2/c\"}");
    }

    @Test
    public void moveLast() {
        // move /test/a to /test2, as last
        commit("/", "> \"test/b\": {\"last\": \"test2\"}");
        Assert.assertEquals("{a,c}", getNode("/test"));
        Assert.assertEquals("{b}", getNode("/test2"));
        assertJournal(">\"/test/b\":{\"last\":\"/test2\"}");

        // move /test/c to /test2, as last
        commit("/", "> \"test/c\": {\"last\": \"test2\"}");
        Assert.assertEquals("{a}", getNode("/test"));
        Assert.assertEquals("{b,c}", getNode("/test2"));
        assertJournal(">\"/test/c\":{\"last\":\"/test2\"}");
    }

    // TODO fix test since it incorrectly expects a specific order of child nodes
    @Test
    public void copy() {
        // copy /test to /test2/copy
        commit("/", "* \"test\": \"/test2/copy\"");
        Assert.assertEquals("{a,b,c}", getNode("/test"));
        Assert.assertEquals("{copy:{a,b,c}}", getNode("/test2"));

        // if (isSimpleKernel(mk)) {
            assertJournal("*\"/test\":\"/test2/copy\"");
        // } else {
        //     assertJournal("+\"/test2/copy\":{\"a\":{},\"b\":{},\"c\":{}}");
        // }
    }

    // TODO fix test since it incorrectly expects a specific order of child nodes
    @Test
    public void move() {
        // move /test/b to /test2
        commit("/", "> \"test/b\": \"/test2/b\"");
        Assert.assertEquals("{a,c}", getNode("/test"));
        Assert.assertEquals("{b}", getNode("/test2"));
        assertJournal(">\"/test/b\":\"/test2/b\"");

        // move /test/a to /test2
        commit("/", "> \"test/a\": \"test2/b1\"");
        Assert.assertEquals("{c}", getNode("/test"));
        Assert.assertEquals("{b,b1}", getNode("/test2"));
        assertJournal(">\"/test/a\":\"/test2/b1\"");

        // move /test/c to /test2
        commit("/", "> \"test/c\": \"test2/c\"");
        Assert.assertEquals("{}", getNode("/test"));
        Assert.assertEquals("{b,b1,c}", getNode("/test2"));
        assertJournal(">\"/test/c\":\"/test2/c\"");
    }

    private void commit(String root, String diff) {
        head = mk.commit(root, diff, head, null);
    }

    private String getNode(String node) {
        String s = mk.getNodes(node, mk.getHeadRevision(), 1, 0, -1, null);
        s = s.replaceAll("\"", "").replaceAll(":childNodeCount:.", "");
        s = s.replaceAll("\\{\\,", "\\{").replaceAll("\\,\\}", "\\}");
        s = s.replaceAll("\\:\\{\\}", "");
        s = s.replaceAll(",,", ",");
        return s;
    }

    private void assertJournal(String expectedJournal) {
        Assert.assertEquals(expectedJournal, getJournal());
    }

    private String getJournal() {
        if (journalRevision == null) {
            String revs = mk.getRevisionHistory(0, 1, null);
            JsopTokenizer t = new JsopTokenizer(revs);
            t.read('[');
            do {
                t.read('{');
                Assert.assertEquals("id", t.readString());
                t.read(':');
                journalRevision = t.readString();
                t.read(',');
                Assert.assertEquals("ts", t.readString());
                t.read(':');
                t.read(JsopReader.NUMBER);
                t.read(',');
                Assert.assertEquals("msg", t.readString());
                t.read(':');
                t.read();
                t.read('}');
            } while (t.matches(','));
        }
        String head = mk.getHeadRevision();
        String journal = mk.getJournal(journalRevision, head, null);
        JsopTokenizer t = new JsopTokenizer(journal);
        StringBuilder buff = new StringBuilder();
        t.read('[');
        boolean isNew = false;
        do {
            t.read('{');
            Assert.assertEquals("id", t.readString());
            t.read(':');
            t.readString();
            t.read(',');
            Assert.assertEquals("ts", t.readString());
            t.read(':');
            t.read(JsopReader.NUMBER);
            t.read(',');
            Assert.assertEquals("msg", t.readString());
            t.read(':');
            t.read();
            t.read(',');
            Assert.assertEquals("changes", t.readString());
            t.read(':');
            String changes = t.readString().trim();
            if (isNew) {
                if (buff.length() > 0) {
                    buff.append('\n');
                }
                buff.append(changes);
            }
            // the first revision isn't new, all others are
            isNew = true;
            t.read('}');
        } while (t.matches(','));
        journalRevision = head;
        return buff.toString();
    }

}