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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.Node.Children;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.DB;

/**
 * A set of simple tests.
 */
public class SimpleTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;

    @Test
    public void test() {
        DocumentMK mk = new DocumentMK.Builder().open();
        mk.dispose();
    }

    @Test
    public void pathToId() {
        assertEquals("0:/", Utils.getIdFromPath("/"));
        assertEquals("/", Utils.getPathFromId("0:/"));
        assertEquals("1:/test", Utils.getIdFromPath("/test"));
        assertEquals("/test", Utils.getPathFromId("1:/test"));
        assertEquals("10:/1/2/3/3/4/6/7/8/9/a", Utils.getIdFromPath("/1/2/3/3/4/6/7/8/9/a"));
        assertEquals("/1/2/3/3/4/6/7/8/9/a", Utils.getPathFromId("10:/1/2/3/3/4/6/7/8/9/a"));
    }
    
    @Test
    public void pathDepth() {
        assertEquals(0, Utils.pathDepth(""));
        assertEquals(0, Utils.pathDepth("/"));
        assertEquals(1, Utils.pathDepth("1/"));
        assertEquals(2, Utils.pathDepth("/a/"));
        assertEquals(2, Utils.pathDepth("/a/b"));
        assertEquals(3, Utils.pathDepth("/a/b/c"));
    }
    
    @Test
    public void addNodeGetNode() {
        DocumentMK mk = new DocumentMK.Builder().open();
        Revision rev = Revision.fromString(mk.getHeadRevision());
        Node n = new Node("/test", rev);
        n.setProperty("name", "Hello");
        UpdateOp op = n.asOperation(true);
        // mark as commit root
        NodeDocument.setRevision(op, rev, "c");
        DocumentStore s = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        assertTrue(s.create(Collection.NODES, Lists.newArrayList(op)));
        Node n2 = ns.getNode("/test", rev);
        assertEquals("Hello", n2.getProperty("name"));
        mk.dispose();
    }
    
    @Test
    public void nodeIdentifier() {
        DocumentMK mk = createMK(true);

        String rev0 = mk.getHeadRevision();
        String rev1 = mk.commit("/", "+\"test\":{}", null, null);
        String rev2 = mk.commit("/test", "+\"a\":{}", null, null);
        String rev3 = mk.commit("/test", "+\"b\":{}", null, null);
        String rev4 = mk.commit("/test", "^\"a/x\":1", null, null);
        
        String r0 = mk.getNodes("/", rev0, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/@r0-0-1\",\":childNodeCount\":0}", r0);
        String r1 = mk.getNodes("/", rev1, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/@r1-0-1\",\"test\":{},\":childNodeCount\":1}", r1);
        String r2 = mk.getNodes("/", rev2, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/@r2-0-1\",\"test\":{},\":childNodeCount\":1}", r2);
        String r3;
        r3 = mk.getNodes("/", rev3, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/@r3-0-1\",\"test\":{},\":childNodeCount\":1}", r3);
        r3 = mk.getNodes("/test", rev3, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/test@r3-0-1\",\"a\":{},\"b\":{},\":childNodeCount\":2}", r3);
        String r4;
        r4 = mk.getNodes("/", rev4, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/@r4-0-1\",\"test\":{},\":childNodeCount\":1}", r4);
        r4 = mk.getNodes("/test", rev4, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/test@r4-0-1\",\"a\":{},\"b\":{},\":childNodeCount\":2}", r4);
        r4 = mk.getNodes("/test/a", rev4, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/test/a@r4-0-1\",\"x\":1,\":childNodeCount\":0}", r4);
        r4 = mk.getNodes("/test/b", rev4, 0, 0, Integer.MAX_VALUE, ":id");
        assertEquals("{\":id\":\"/test/b@r3-0-1\",\":childNodeCount\":0}", r4);
        
        mk.dispose();        
    }
    
    @Test
    public void conflict() {
        DocumentMK mk = createMK();
        mk.commit("/", "+\"a\": {}", null, null);
        try {
            mk.commit("/", "+\"b\": {}  +\"a\": {}", null, null);
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
        // the previous commit should be rolled back now,
        // so this should work
        mk.commit("/", "+\"b\": {}", null, null);
        mk.dispose();
    }
    
    @Test
    public void diff() {
        DocumentMK mk = createMK();
        
        String rev0 = mk.getHeadRevision();
        String rev1 = mk.commit("/", "+\"t1\":{}", null, null);
        String rev2 = mk.commit("/", "+\"t2\":{}", null, null);
        String rev3 = mk.commit("/", "+\"t3\":{}", null, null);
        String rev4 = mk.commit("/", "^\"t3/x\":1", null, null);
        
        String r0 = mk.getNodes("/", rev0, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\":childNodeCount\":0}", r0);
        String r1 = mk.getNodes("/", rev1, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"t1\":{},\":childNodeCount\":1}", r1);
        String r2 = mk.getNodes("/", rev2, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"t1\":{},\"t2\":{},\":childNodeCount\":2}", r2);
        String r3 = mk.getNodes("/", rev3, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"t1\":{},\"t2\":{},\"t3\":{},\":childNodeCount\":3}", r3);
        
        String diff01 = mk.diff(rev0, rev1, "/", 0).trim();
        assertEquals("+\"/t1\":{}", diff01);
        String diff12 = mk.diff(rev1, rev2, "/", 0).trim();
        assertEquals("+\"/t2\":{}", diff12);
        String diff23 = mk.diff(rev2, rev3, "/", 0).trim();
        assertEquals("+\"/t3\":{}", diff23);
        String diff13 = mk.diff(rev1, rev3, "/", 0).trim();
        assertEquals("+\"/t2\":{}\n+\"/t3\":{}", diff13);
        String diff34 = mk.diff(rev3, rev4, "/", 0).trim();
        assertEquals("^\"/t3\":{}", diff34);
        mk.dispose();
    }

    @Test
    public void reAddDeleted() {
        DocumentMK mk = createMK();
        String rev0 = mk.getHeadRevision();
        String rev1 = mk.commit("/", "+\"test\":{\"name\": \"Hello\"} ^ \"x\": 1", null, null);
        String rev2 = mk.commit("/", "-\"test\" ^ \"x\": 2", null, null);
        String rev3 = mk.commit("/", "+\"test\":{\"name\": \"Hallo\"} ^ \"x\": 3", null, null);
        String test0 = mk.getNodes("/test", rev0, 0, 0, Integer.MAX_VALUE, null);
        assertNull(null, test0);
        String test1 = mk.getNodes("/test", rev1, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test1);
        String test2 = mk.getNodes("/test", rev2, 0, 0, Integer.MAX_VALUE, null);
        assertNull(null, test2);
        String test3 = mk.getNodes("/test", rev3, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hallo\",\":childNodeCount\":0}", test3);
        mk.dispose();
    }

    @Test
    public void reAddDeleted2() {
        DocumentMK mk = createMK();
        String rev = mk.commit("/", "+\"test\":{\"x\":\"1\",\"child\": {}}", null, null);
        rev = mk.commit("/", "-\"test\"", rev, null);
        rev = mk.commit("/", "+\"test\":{}  +\"test2\": {}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\":childNodeCount\":0}", test);
        String test2 = mk.getNodes("/test2", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\":childNodeCount\":0}", test2);
        mk.dispose();
    }
    
    @Test
    public void move() {
        DocumentMK mk = createMK();
        String rev = mk.commit("/", "+\"test\":{\"x\":\"1\",\"child\": {}}", null, null);
        rev = mk.commit("/", ">\"test\": \"/test2\"", rev, null);
        String test = mk.getNodes("/test2", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"x\":\"1\",\"child\":{},\":childNodeCount\":1}", test);
        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertNull(test);
        mk.dispose();
    }
    
    @Test
    public void copy() {
        DocumentMK mk = createMK();
        String rev = mk.commit("/", "+\"test\":{\"x\":\"1\",\"child\": {}}", null, null);
        rev = mk.commit("/", "*\"test\": \"/test2\"", rev, null);
        String test = mk.getNodes("/test2", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"x\":\"1\",\"child\":{},\":childNodeCount\":1}", test);
        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"x\":\"1\",\"child\":{},\":childNodeCount\":1}", test);
        mk.dispose();
    }

    @Test
    public void escapePropertyName() {
        DocumentMK mk = createMK();
        String rev = mk.commit(
                "/", "+\"test\":{\"name.first\": \"Hello\", \"_id\": \"a\", \"$x\": \"1\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"$x\":\"1\",\"_id\":\"a\",\"name.first\":\"Hello\",\":childNodeCount\":0}", test);
        mk.dispose();
    }
    
    @Test
    public void commit() {
        DocumentMK mk = createMK();
        DocumentNodeStore ns = mk.getNodeStore();
        
        String rev = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
        
        String r0 = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
        String r1 = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
        test = mk.getNodes("/test", r0, 0, 0, Integer.MAX_VALUE, null);
        Node n = ns.getNode("/", Revision.fromString(r0));
        assertNotNull(n);
        Children c = ns.getChildren(n, null, Integer.MAX_VALUE);
        assertEquals("[/test]", c.toString());
        n = ns.getNode("/test", Revision.fromString(r1));
        assertNotNull(n);
        c = ns.getChildren(n, null, Integer.MAX_VALUE);
        assertEquals("[/test/a, /test/b]", c.toString());

        rev = mk.commit("", "^\"/test\":1", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        // System.out.println(test);
        mk.dispose();
    }

    @Test
    public void delete() {
        DocumentMK mk = createMK();
        DocumentNodeStore ns = mk.getNodeStore();

        mk.commit("/", "+\"testDel\":{\"name\": \"Hello\"}", null, null);
        mk.commit("/testDel", "+\"a\":{\"name\": \"World\"}", null, null);
        mk.commit("/testDel", "+\"b\":{\"name\": \"!\"}", null, null);
        String r1 = mk.commit("/testDel", "+\"c\":{\"name\": \"!\"}", null, null);

        Node n = ns.getNode("/testDel", Revision.fromString(r1));
        assertNotNull(n);
        Children c = ns.getChildren(n, null, Integer.MAX_VALUE);
        assertEquals(3, c.children.size());

        String r2 = mk.commit("/testDel", "-\"c\"", null, null);
        n = ns.getNode("/testDel", Revision.fromString(r2));
        assertNotNull(n);
        c = ns.getChildren(n, null, Integer.MAX_VALUE);
        assertEquals(2, c.children.size());

        String r3 = mk.commit("/", "-\"testDel\"", null, null);
        n = ns.getNode("/testDel", Revision.fromString(r3));
        assertNull(n);
    }
    
    @Test
    public void escapeUnescape() {
        DocumentMK mk = createMK();
        String rev;
        String nodes; 
        Random r = new Random(1);
        for (int i = 0; i < 20; i++) {
            int len = 1 + r.nextInt(5);
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < len; j++) {
                buff.append((char) (32 + r.nextInt(128)));
            }
            String s = buff.toString();
            String x2 = Utils.escapePropertyName(s);
            String s2 = Utils.unescapePropertyName(x2);
            if (!s.equals(s2)) {
                assertEquals(s, s2);
            }
            if (s.indexOf('/') >= 0) {
                continue;
            }
            JsopBuilder jsop = new JsopBuilder();
            jsop.tag('+').key(s).object().key(s).value("x").endObject();
            rev = mk.commit("/", jsop.toString(), 
                    null, null);
            nodes = mk.getNodes("/" + s, rev, 0, 0, 100, null);
            jsop = new JsopBuilder();
            jsop.object().key(s).value("x").
                    key(":childNodeCount").value(0).endObject();
            String n = jsop.toString();
            assertEquals(n, nodes);
            nodes = mk.getNodes("/", rev, 0, 0, 100, null);
            jsop = new JsopBuilder();
            jsop.object().key(s).object().endObject().
            key(":childNodeCount").value(1).endObject();
            n = jsop.toString();
            assertEquals(n, nodes);
            jsop = new JsopBuilder();
            jsop.tag('-').value(s);
            rev = mk.commit("/", jsop.toString(), rev, null);
            
        }
    }
    
    @Test
    public void nodeAndPropertyNames() {
        DocumentMK mk = createMK();
        String rev;
        String nodes; 
        for (String s : new String[] { "_", "$", "__", "_id", "$x", ".", ".\\", "x\\", "\\x", "first.name" }) {
            String x2 = Utils.escapePropertyName(s);
            String s2 = Utils.unescapePropertyName(x2);
            if (!s.equals(s2)) {
                assertEquals(s, s2);
            }
            JsopBuilder jsop = new JsopBuilder();
            jsop.tag('+').key(s).object().key(s).value("x").endObject();
            rev = mk.commit("/", jsop.toString(), 
                    null, null);
            nodes = mk.getNodes("/" + s, rev, 0, 0, 10, null);
            jsop = new JsopBuilder();
            jsop.object().key(s).value("x").
                    key(":childNodeCount").value(0).endObject();
            String n = jsop.toString();
            assertEquals(n, nodes);
            nodes = mk.getNodes("/", rev, 0, 0, 10, null);
            jsop = new JsopBuilder();
            jsop.object().key(s).object().endObject().
            key(":childNodeCount").value(1).endObject();
            n = jsop.toString();
            assertEquals(n, nodes);
            jsop = new JsopBuilder();
            jsop.tag('-').value(s);
            rev = mk.commit("/", jsop.toString(), rev, null);
        }
    }

    @Test
    public void addAndMove() {
        DocumentMK mk = createMK();

        String head = mk.getHeadRevision();
        head = mk.commit("",
                "+\"/root\":{}\n" +
                        "+\"/root/a\":{}\n"+
                        "+\"/root/a/b\":{}\n",
                head, "");

        head = mk.commit("",
                ">\"/root/a\":\"/root/c\"\n",
                head, "");

        assertFalse(mk.nodeExists("/root/a", head));
        assertTrue(mk.nodeExists("/root/c/b", head));
    }

    @Test
    public void commitRoot() {
        DocumentMK mk = createMK();
        try {
            DocumentStore store = mk.getDocumentStore();
            Revision head = Revision.fromString(mk.getHeadRevision());
            head = Revision.fromString(mk.commit("", "+\"/test\":{\"foo\":{}}", head.toString(), null));

            // root node must not have the revision
            NodeDocument rootDoc = store.find(Collection.NODES, "0:/");

            //As we update the childStatus flag the commit root would shift
            //one layer above
            // assertNotNull(rootDoc);
            // assertFalse(rootDoc.containsRevision(head));

            // test node must have head in revisions
            NodeDocument node = store.find(Collection.NODES, "1:/test");
            //assertNotNull(node);
            //assertTrue(node.containsRevision(head));

            // foo must not have head in revisions and must refer to test
            // as commit root (depth = 1)
            NodeDocument foo = store.find(Collection.NODES, "2:/test/foo");
            assertNotNull(foo);
            assertFalse(foo.containsRevision(head));
            assertEquals("/", foo.getCommitRootPath(head));

            head = Revision.fromString(mk.commit("", "+\"/bar\":{}+\"/test/foo/bar\":{}", head.toString(), null));

            // root node is root of commit
            rootDoc = store.find(Collection.NODES, "0:/");
            assertNotNull(rootDoc);
            assertTrue(rootDoc.containsRevision(head));

            // /bar refers to root nodes a commit root
            NodeDocument bar = store.find(Collection.NODES, "1:/bar");
            assertNotNull(bar);
            assertEquals("/", bar.getCommitRootPath(head));

            // /test/foo/bar refers to root nodes a commit root
            bar = store.find(Collection.NODES, "3:/test/foo/bar");
            assertNotNull(bar);
            assertEquals("/", bar.getCommitRootPath(head));

        } finally {
            mk.dispose();
        }
    }

    private static DocumentMK createMK() {
        return createMK(false);
    }

    private static DocumentMK createMK(boolean useSimpleRevision) {
        DocumentMK.Builder builder = new DocumentMK.Builder();

        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            builder.setMongoDB(db);
        }

        builder.setUseSimpleRevision(useSimpleRevision);

        return builder.open();
    }

}
