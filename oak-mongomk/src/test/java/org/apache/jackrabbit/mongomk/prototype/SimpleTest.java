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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.DB;

/**
 * A set of simple tests.
 */
public class SimpleTest {
    
    private static final boolean MONGO_DB = false;
//    private static final boolean MONGO_DB = true;

    @Test
    public void test() {
        MongoMK mk = new MongoMK();
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
    public void revision() {
        for (int i = 0; i < 100; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            Revision r2 = Revision.fromString(r.toString());
            assertEquals(r.toString(), r2.toString());
            assertEquals(r.hashCode(), r2.hashCode());
            assertTrue(r.equals(r2));
        }
    }
    
    @Test
    public void addNodeGetNode() {
        MongoMK mk = new MongoMK();
        Revision rev = mk.newRevision();
        Node n = new Node("/test", rev);
        n.setProperty("name", "Hello");
        UpdateOp op = n.asOperation(true);
        DocumentStore s = mk.getDocumentStore();
        assertTrue(s.create(Collection.NODES, Lists.newArrayList(op)));
        Node n2 = mk.getNode("/test", rev);
        assertEquals("Hello", n2.getProperty("name"));
        mk.dispose();
    }
    
    @Test
    public void diff() {
        MongoMK mk = createMK();
        String rev0 = mk.getHeadRevision();
        // TODO
//        String rev1 = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
//        String rev2 = mk.commit("/", "-\"test\"", null, null);
//        String rev3 = mk.commit("/", "+\"test\":{\"name\": \"Hallo\"}", null, null);
//        String test0 = mk.getNodes("/test", rev0, 0, 0, Integer.MAX_VALUE, null);
//        assertNull(null, test0);
//        String test1 = mk.getNodes("/test", rev1, 0, 0, Integer.MAX_VALUE, null);
//        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test1);
//        String test2 = mk.getNodes("/test", rev2, 0, 0, Integer.MAX_VALUE, null);
//        assertNull(null, test2);
//        String test3 = mk.getNodes("/test", rev3, 0, 0, Integer.MAX_VALUE, null);
//        assertEquals("{\"name\":\"Hallo\",\":childNodeCount\":0}", test3);
        mk.dispose();
    }

    @Test
    public void reAddDeleted() {
        MongoMK mk = createMK();
        String rev0 = mk.getHeadRevision();
        String rev1 = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        String rev2 = mk.commit("/", "-\"test\"", null, null);
        String rev3 = mk.commit("/", "+\"test\":{\"name\": \"Hallo\"}", null, null);
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
        MongoMK mk = createMK();
        String rev = mk.commit("/", "+\"test\":{\"x\":\"1\",\"child\": {}}", null, null);
        rev = mk.commit("/", "-\"test\"", rev, null);
        rev = mk.commit("/", "+\"test\":{}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\":childNodeCount\":0}", test);
        mk.dispose();
    }

    @Test
    public void escapePropertyName() {
        MongoMK mk = createMK();
        String rev = mk.commit(
                "/", "+\"test\":{\"name.first\": \"Hello\", \"_id\": \"a\", \"$x\": \"1\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"$x\":\"1\",\"_id\":\"a\",\"name.first\":\"Hello\",\":childNodeCount\":0}", test);
        mk.dispose();
    }
    
    @Test
    public void commit() {
        MongoMK mk = createMK();
        
        String rev = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
        
        rev = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
        rev = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        Children c;
        c = mk.readChildren("/", "1",
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/: [/test]", c.toString());
        c = mk.readChildren("/test", "2",
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/test: [/test/a, /test/b]", c.toString());

        rev = mk.commit("", "^\"/test\":1", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        // System.out.println(test);
        mk.dispose();
    }
    
    @Test
    public void cache() {
        MongoMK mk = createMK();
        
        // BAD
        String rev = mk.commit("/", "+\"testRoot\":{} +\"index\":{}", null, null);
        
        // GOOD
//        String rev = mk.commit("/", "+\"testRoot\":{} ", null, null);
        
        String test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
        // System.out.println("  " + test);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
        rev = mk.commit("/testRoot", "+\"a\":{}", null, null);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        rev = mk.commit("/testRoot/a", "+\"b\":{}", null, null);
//        rev = mk.commit("/testRoot/a/b", "+\"c\":{} +\"d\":{}", null, null);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
//        rev = mk.commit("/index", "+\"a\":{}", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, ":id");
        // System.out.println("  " + test);
//        test = mk.getNodes("/testRoot", rev, 0, 0, Integer.MAX_VALUE, ":id");
//        System.out.println("  " + test);
        
//        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
//        
//        rev = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
//        rev = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
//        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
//        Children c;
//        c = mk.readChildren("/", "1",
//                Revision.fromString(rev), Integer.MAX_VALUE);
//        assertEquals("/: [/test]", c.toString());
//        c = mk.readChildren("/test", "2",
//                Revision.fromString(rev), Integer.MAX_VALUE);
//        assertEquals("/test: [/test/a, /test/b]", c.toString());
//
//        rev = mk.commit("", "^\"/test\":1", null, null);
//        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
//        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        // System.out.println(test);
        mk.dispose();
    }


    @Test
    public void testDeletion() {
        MongoMK mk = createMK();

        String rev = mk.commit("/", "+\"testDel\":{\"name\": \"Hello\"}", null,
                null);
        rev = mk.commit("/testDel", "+\"a\":{\"name\": \"World\"}", null, null);
        rev = mk.commit("/testDel", "+\"b\":{\"name\": \"!\"}", null, null);
        rev = mk.commit("/testDel", "+\"c\":{\"name\": \"!\"}", null, null);

        Children c = mk.readChildren("/testDel", "1", Revision.fromString(rev),
                Integer.MAX_VALUE);
        assertEquals(3, c.children.size());

        rev = mk.commit("/testDel", "-\"c\"", null, null);
        c = mk.readChildren("/testDel", "2", Revision.fromString(rev),
                Integer.MAX_VALUE);
        assertEquals(2, c.children.size());

        rev = mk.commit("/", "-\"testDel\"", null, null);
        Node n = mk.getNode("/testDel", Revision.fromString(rev));
        assertNull(n);
    }

    @Test
    public void testAddAndMove() {
        MongoMK mk = createMK();

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

    private static MongoMK createMK() {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            return new MongoMK(db, 0);
        }
        return new MongoMK();
    }

}
