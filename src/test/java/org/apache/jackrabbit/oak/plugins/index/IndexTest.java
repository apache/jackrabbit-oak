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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.plugins.index.BTree;
import org.apache.jackrabbit.oak.plugins.index.Cursor;
import org.apache.jackrabbit.oak.plugins.index.Indexer;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndex;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the indexing mechanism.
 */
public class IndexTest {

    private MicroKernel mk;
    private Indexer indexer;

    @Before
    public void before() {
        mk = new MicroKernelImpl();
        indexer = new Indexer(mk);
        indexer.init();
    }

    @Test
    public void createIndexAfterAddingData() {
        PropertyIndex indexOld = indexer.createPropertyIndex("x", false);
        mk.commit("/", "+ \"test\": { \"test2\": { \"id\": 1 }, \"id\": 1 }", mk.getHeadRevision(), "");
        mk.commit("/", "+ \"test3\": { \"test2\": { \"id\": 2 }, \"id\": 2 }", mk.getHeadRevision(), "");
        indexOld.getPath("x", mk.getHeadRevision());
        PropertyIndex index = indexer.createPropertyIndex("id", false);
        Iterator<String> it = index.getPaths("1", mk.getHeadRevision());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test", it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test/test2", it.next());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void nonUnique() {
        PropertyIndex index = indexer.createPropertyIndex("id", false);
        mk.commit("/", "+ \"test\": { \"test2\": { \"id\": 1 }, \"id\": 1 }", mk.getHeadRevision(), "");
        mk.commit("/", "+ \"test3\": { \"test2\": { \"id\": 2 }, \"id\": 2 }", mk.getHeadRevision(), "");
        Iterator<String> it = index.getPaths("1", mk.getHeadRevision());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test", it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test/test2", it.next());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void nestedAddNode() {
        PropertyIndex index = indexer.createPropertyIndex("id", true);

        mk.commit("/", "+ \"test\": { \"test2\": { \"id\": 2 }, \"id\": 1 }", mk.getHeadRevision(), "");
        Assert.assertEquals("/test", index.getPath("1", mk.getHeadRevision()));
        Assert.assertEquals("/test/test2", index.getPath("2", mk.getHeadRevision()));
    }

    @Test
    public void move() {
        PropertyIndex index = indexer.createPropertyIndex("id", true);

        mk.commit("/", "+ \"test\": { \"test2\": { \"id\": 2 }, \"id\": 1 }", mk.getHeadRevision(), "");
        Assert.assertEquals("/test", index.getPath("1", mk.getHeadRevision()));
        Assert.assertEquals("/test/test2", index.getPath("2", mk.getHeadRevision()));

        mk.commit("/", "> \"test\": \"moved\"", mk.getHeadRevision(), "");
        Assert.assertEquals("/moved", index.getPath("1", mk.getHeadRevision()));
        Assert.assertEquals("/moved/test2", index.getPath("2", mk.getHeadRevision()));
    }

    @Test
    public void copy() {
        PropertyIndex index = indexer.createPropertyIndex("id", false);

        mk.commit("/", "+ \"test\": { \"test2\": { \"id\": 2 }, \"id\": 1 }", mk.getHeadRevision(), "");
        Assert.assertEquals("/test", index.getPath("1", mk.getHeadRevision()));
        Assert.assertEquals("/test/test2", index.getPath("2", mk.getHeadRevision()));

        mk.commit("/", "* \"test\": \"copied\"", mk.getHeadRevision(), "");
        Iterator<String> it = index.getPaths("1", mk.getHeadRevision());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/copied", it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test", it.next());
        Assert.assertFalse(it.hasNext());
        it = index.getPaths("2", mk.getHeadRevision());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/copied/test2", it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals("/test/test2", it.next());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void ascending() {
        BTree tree = new BTree(indexer, "test", true);
        tree.setMinSize(2);
        print(mk, tree);
        int len = 30;
        for (int i = 0; i < len; i++) {
            log("#insert " + i);
            tree.add("" + i, "p" + i);
            // print(mk, tree);
        }
        // indexer.commitChanges();
        for (int i = 0; i < len; i++) {
            // log("#test " + i);
            Cursor c = tree.findFirst("" + i);
            if (c.hasNext()) {
                Assert.assertEquals("" + i, c.next());
            }
        }
        print(mk, tree);
        for (int i = 0; i < len; i++) {
            Assert.assertTrue("not found when removing " + i, tree.remove("" + i, null));
            // print(mk, tree);
        }
        // indexer.commitChanges();
        print(mk, tree);
        for (int i = 0; i < len; i++) {
            Cursor c = tree.findFirst("" + i);
            Assert.assertFalse(c.hasNext());
        }
    }

    @Test
    public void duplicateKeyUnique() {
        duplicateKey(true);
    }

    @Test
    public void duplicateKeyNonUnique() {
        duplicateKey(false);
    }

    private void duplicateKey(boolean unique) {
        BTree tree = new BTree(indexer, "test", unique);
        tree.setMinSize(2);

        // add
        tree.add("1", "p1");
        if (unique) {
            try {
                tree.add("1", "p2");
                Assert.fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
        } else {
            tree.add("1", "p2");
        }

        // search
        Cursor c = tree.findFirst("1");
        Assert.assertEquals("1", c.next());
        Assert.assertEquals("p1", c.getValue());
        if (unique) {
            Assert.assertFalse(c.hasNext());
        } else {
            Assert.assertEquals("1", c.next());
            Assert.assertEquals("p2", c.getValue());
        }
        try {
            c.remove();
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
        Assert.assertFalse(c.hasNext());
        Assert.assertEquals(null, c.next());

        // remove
        tree.remove("1", "p1");

    }

    @Test
    public void random() {
        BTree tree = new BTree(indexer, "test", true);
        tree.setMinSize(2);
        Random r = new Random(1);
        TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
        for (int i = 0; i < 100; i++) {
            log("op #" + i);
            // print(mk, tree);
            int x = r.nextInt(10);
            boolean exists = treeMap.containsKey(x);
            Cursor c = tree.findFirst("" + x);
            boolean gotExists = c.hasNext();
            String x2 = null;
            if (gotExists) {
                x2 = c.next();
                if (!x2.equals("" + x)) {
                    gotExists = false;
                }
            }
            Assert.assertEquals("find " + x, exists, gotExists);
            if (exists) {
                Assert.assertEquals("" + x, x2);
                Assert.assertEquals(treeMap.get(x), c.getValue());
            }
            boolean add = r.nextBoolean();
            if (add) {
                if (!exists) {
                    log("#insert " + x + " = p" + i);
                    treeMap.put(x, "p" + i);
                    tree.add("" + x, "p" + i);
                }
            } else {
                if (exists) {
                    log("#remove " + x);
                    treeMap.remove(x);
                    tree.remove("" + x, null);
                }
            }
        }
    }

    static void log(String s) {
        // System.out.println(s);
    }

    static void print(MicroKernel mk, BTree tree) {
        String head = mk.getHeadRevision();
        String t = mk.getNodes(Indexer.INDEX_CONFIG_PATH, head, 100, 0, -1, null);
        log(t);
        Cursor c = tree.findFirst("0");
        StringBuilder buff = new StringBuilder();
        while (c.hasNext()) {
            buff.append(c.next()).append(' ');
        }
        log(buff.toString());
    }

}
