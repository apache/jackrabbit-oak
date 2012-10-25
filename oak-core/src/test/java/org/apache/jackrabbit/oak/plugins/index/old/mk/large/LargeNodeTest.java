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
package org.apache.jackrabbit.oak.plugins.index.old.mk.large;

import junit.framework.Assert;

import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test moving nodes.
 */
@RunWith(Parameterized.class)
public class LargeNodeTest extends MultiMkTestBase {

    private String head;

    public LargeNodeTest(String url) {
        super(url);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        head = mk.getHeadRevision();
        commit("/", "+ \"t\": {\"a\":{}, \"b\":{}, \"c\":{}}");
    }

    @Override
    @After
    public void tearDown() throws InterruptedException {
        if (isSimpleKernel(mk)) {
            head = mk.commit("/:root/head/config", "^ \"maxMemoryChildren\": " + Integer.MAX_VALUE, head, "");
            head = mk.commit("/:root/head/config", "^ \"maxMemoryChildren\": null", head, "");
        }
        super.tearDown();
    }

    @Test
    public void getNodes() {
        head = mk.commit("/", "+\"x0\" : {\"x\": 0, \"x1\":{\"x\":1, \"x2\": {\"x\": -3}}}", head, null);
        String s = mk.getNodes("/x0", head, 1, 0, -1, null);
        Assert.assertEquals("{\"x\":0,\":childNodeCount\":1,\"x1\":{\"x\":1,\":childNodeCount\":1,\"x2\":{}}}", s);
        s = mk.getNodes("/x0", head, 1, 0, -1, null);
        Assert.assertEquals("{\"x\":0,\":childNodeCount\":1,\"x1\":{\"x\":1,\":childNodeCount\":1,\"x2\":{}}}", s);
        s = mk.getNodes("/x0", head, 0, 0, -1, null);
        Assert.assertEquals("{\"x\":0,\":childNodeCount\":1,\"x1\":{}}", s);
        s = mk.getNodes("/x0", head, 0, 0, 0, null);
        Assert.assertEquals("{\"x\":0,\":childNodeCount\":1}", s);
        head = mk.commit("/", "-\"x0\"", head, null);
    }

    @Test
    public void largeNodeListAndGetNodes() {
        if (!isSimpleKernel(mk)) {
            return;
        }
        int max = 90;
        head = mk.commit("/:root/head/config", "^ \"maxMemoryChildren\":" + max, head, "");
        Assert.assertEquals("{\"maxMemoryChildren\":"+max+",\":childNodeCount\":0}", mk.getNodes("/:root/head/config", head, 1, 0, -1, null));
        head = mk.commit("/", "+ \"test\": {}", head, "");
        for (int i = 0; i < 100; i++) {
            head = mk.commit("/", "+ \"test/" + i + "\": {\"x\":" + i + "}\n", head, "");
        }
        Assert.assertTrue(mk.nodeExists("/test", head));
        mk.getNodes("/test", head, 1, 0, -1, null);
    }

    @Test
    public void veryLargeNodeList() {
        if (isSimpleKernel(mk)) {
            int max = 2000;
            head = mk.commit("/:root/head/config", "^ \"maxMemoryChildren\":" + max, head, "");
            Assert.assertEquals("{\"maxMemoryChildren\":"+max+",\":childNodeCount\":0}", mk.getNodes("/:root/head/config", head, 1, 0, -1, null));
        }
        head = mk.commit("/", "+ \"test\": {}", head, "");

        // added 1000000 nodes 33.93 seconds (1000000 ops; 29471 op/s)
        // int count = 1000000;
        int count = 5000;
        StopWatch timer = new StopWatch();
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i % 100 == 0 && timer.log()) {
                log("added " + i + " nodes " + timer.operationsPerSecond(i));
            }
            buff.append("+ \"test/" + i + "\": {\"x\":" + i + "}\n");
            if (i % 1000 == 0) {
                head = mk.commit("/", buff.toString(), head, "");
                buff.setLength(0);
            }
        }
        log("added " + count + " nodes " + timer.operationsPerSecond(count));
        if (buff.length() > 0) {
            head = mk.commit("/", buff.toString(), head, "");
        }
        Assert.assertEquals("{\":childNodeCount\":"+count+"}", mk.getNodes("/test", head, 1, 0, 0, null));
    }

    private void log(String s) {
        // System.out.println(url + " " + s);
    }

    @Test
    public void largeNodeList() {
        if (!isSimpleKernel(mk)) {
            return;
        }

        head = mk.commit("/:root/head/config", "^ \"maxMemoryChildren\": 10", head, "");
        Assert.assertEquals("{\"maxMemoryChildren\":10,\":childNodeCount\":0}", mk.getNodes("/:root/head/config", head, 1, 0, -1, null));
        for (int i = 0; i < 100; i++) {
            head = mk.commit("/", "+ \"t" + i + "\": {\"x\":" + i + "}", head, "");
        }
        boolean largeChildNodeListsAreSorted = true;
        if (largeChildNodeListsAreSorted) {
            Assert.assertEquals("{\":childNodeCount\":101,\"t\":{\":childNodeCount\":3,\"a\":{},\"b\":{},\"c\":{}}," +
                    "\"t0\":{\"x\":0,\":childNodeCount\":0}," +
                    "\"t1\":{\"x\":1,\":childNodeCount\":0}," +
                    "\"t10\":{\"x\":10,\":childNodeCount\":0}," +
                    "\"t11\":{\"x\":11,\":childNodeCount\":0}," +
                    "\"t12\":{\"x\":12,\":childNodeCount\":0}," +
                    "\"t13\":{\"x\":13,\":childNodeCount\":0}," +
                    "\"t14\":{\"x\":14,\":childNodeCount\":0}," +
                    "\"t15\":{\"x\":15,\":childNodeCount\":0}," +
                    "\"t16\":{\"x\":16,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 0, -1, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t0\":{\"x\":0,\":childNodeCount\":0}," +
                    "\"t1\":{\"x\":1,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 1, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t17\":{\"x\":17,\":childNodeCount\":0}," +
                    "\"t18\":{\"x\":18,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 10, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t26\":{\"x\":26,\":childNodeCount\":0}," +
                    "\"t27\":{\"x\":27,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 20, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t44\":{\"x\":44,\":childNodeCount\":0}," +
                    "\"t45\":{\"x\":45,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 40, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t9\":{\"x\":9,\":childNodeCount\":0}," +
                    "\"t90\":{\"x\":90,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 90, 2, null));
        } else {
            Assert.assertEquals("{\":childNodeCount\":101,\"t\":{\":childNodeCount\":3,\"a\":{},\"b\":{},\"c\":{}}," +
                    "\"t0\":{\"x\":0,\":childNodeCount\":0}," +
                    "\"t1\":{\"x\":1,\":childNodeCount\":0}," +
                    "\"t2\":{\"x\":2,\":childNodeCount\":0}," +
                    "\"t3\":{\"x\":3,\":childNodeCount\":0}," +
                    "\"t4\":{\"x\":4,\":childNodeCount\":0}," +
                    "\"t5\":{\"x\":5,\":childNodeCount\":0}," +
                    "\"t6\":{\"x\":6,\":childNodeCount\":0}," +
                    "\"t7\":{\"x\":7,\":childNodeCount\":0}," +
                    "\"t8\":{\"x\":8,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 0, -1, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t0\":{\"x\":0,\":childNodeCount\":0}," +
                    "\"t1\":{\"x\":1,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 1, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t9\":{\"x\":9,\":childNodeCount\":0}," +
                    "\"t10\":{\"x\":10,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 10, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t19\":{\"x\":19,\":childNodeCount\":0}," +
                    "\"t20\":{\"x\":20,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 20, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t39\":{\"x\":39,\":childNodeCount\":0}," +
                    "\"t40\":{\"x\":40,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 40, 2, null));
            Assert.assertEquals("{\":childNodeCount\":101," +
                    "\"t89\":{\"x\":89,\":childNodeCount\":0}," +
                    "\"t90\":{\"x\":90,\":childNodeCount\":0}}",
                    mk.getNodes("/", head, 1, 90, 2, null));
        }
    }

    @Test
    public void offsetLimit() {
        if (!isSimpleKernel(mk)) {
            // TODO fix test since it incorrectly expects a specific order of child nodes
            return;
        }

        Assert.assertEquals("{a,b,c}", getNode("/t", 0, 0, -1));
        Assert.assertEquals("{b,c}", getNode("/t", 0, 1, -1));
        Assert.assertEquals("{c}", getNode("/t", 0, 2, -1));
        Assert.assertEquals("{a}", getNode("/t", 0, 0, 1));
        Assert.assertEquals("{a,b}", getNode("/t", 0, 0, 2));
        Assert.assertEquals("{b}", getNode("/t", 0, 1, 1));
    }

    private void commit(String root, String diff) {
        head = mk.commit(root, diff, head, null);
    }

    private String getNode(String node, int depth, long offset, int count) {
        String s = mk.getNodes(node, mk.getHeadRevision(), depth, offset, count, null);
        s = s.replaceAll("\"", "").replaceAll(":childNodeCount:.", "");
        s = s.replaceAll("\\{\\,", "\\{").replaceAll("\\,\\}", "\\}");
        s = s.replaceAll("\\:\\{\\}", "");
        return s;
    }

}