/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Random;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.index.old.mk.MicroKernelFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the virtual repository wrapper.
 */
@RunWith(Parameterized.class)
public class VirtualRepositoryWrapperTest extends MultiMkTestBase {

    private String head;
    private MicroKernel mkRep1;
    private MicroKernel mkRep2;
    private MicroKernel mkVirtual;

    public VirtualRepositoryWrapperTest(String url) {
        super(url);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        String url1 = url + "/rep1", url2 = url + "/rep2";
        mkRep1 = MicroKernelFactory.getInstance(url1 + ";clean");
        mkRep2 = MicroKernelFactory.getInstance(url2 + ";clean");
        String init = "+ \":mount\": { " +
            "\"r1\": { \"url\": \"" + url1 + "\", \"paths\": \"/data/a\" }" +
            "," +
            "\"r2\": { \"url\": \"" + url2 + "\", \"paths\": \"/data/b\" }" +
            "}";
        mkRep1.commit("/", init, mkRep1.getHeadRevision(), "");
        mkRep2.commit("/", init, mkRep2.getHeadRevision(), "");
        mkVirtual = MicroKernelFactory.getInstance("virtual:" + url1);
    }

    @Override
    @After
    public void tearDown() throws InterruptedException {
        try {
            mkRep1.commit("/", "- \":mount\"", mkRep1.getHeadRevision(), "");
            mkRep2.commit("/", "- \":mount\"", mkRep2.getHeadRevision(), "");
            if (mkVirtual != null) {
                MicroKernelFactory.disposeInstance(mkVirtual);
            }
            MicroKernelFactory.disposeInstance(mkRep1);
            MicroKernelFactory.disposeInstance(mkRep2);
            super.tearDown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void commit() {
        if (!isSimpleKernel(mk)) {
            return;
        }

        // add node
        head = mkVirtual.commit("/", "+ \"data\": {} ", head, "");
        head = mkVirtual.commit("/", "+ \"data/a\": { \"data\": \"Hello\" }", head, "");
        head = mkVirtual.commit("/", "+ \"data/b\": { \"data\": \"World\" }", head, "");

        // get nodes
        String m1 = mkRep1.getNodes("/data", mkRep1.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\":childNodeCount\":1,\"a\":{\"data\":\"Hello\",\":childNodeCount\":0}}", m1);
        String m2 = mkRep2.getNodes("/data", mkRep2.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\":childNodeCount\":1,\"b\":{\"data\":\"World\",\":childNodeCount\":0}}", m2);
        String m = mkVirtual.getNodes("/data/a", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Hello\",\":childNodeCount\":0}", m);
        m = mkVirtual.getNodes("/data/b", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"World\",\":childNodeCount\":0}", m);

        // get nodes on unknown nodes
        m = mkVirtual.getNodes("/notMapped", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertNull(m);
        m = mkVirtual.getNodes("/data/a/notExist", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertNull(m);

        // set property
        head = mkVirtual.commit("/", "^ \"data/a/data\": \"Hallo\"", head, "");
        head = mkVirtual.commit("/", "^ \"data/b/data\": \"Welt\"", head, "");
        m = mkVirtual.getNodes("/data/a", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Hallo\",\":childNodeCount\":0}", m);
        m = mkVirtual.getNodes("/data/b", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Welt\",\":childNodeCount\":0}", m);

        // add property
        head = mkVirtual.commit("/", "+ \"data/a/lang\": \"de\"", head, "");
        m = mkVirtual.getNodes("/data/a", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Hallo\",\"lang\":\"de\",\":childNodeCount\":0}", m);
        head = mkVirtual.commit("/", "^ \"data/a/lang\": null", head, "");
        m = mkVirtual.getNodes("/data/a", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Hallo\",\":childNodeCount\":0}", m);

        // move
        head = mkVirtual.commit("/", "+ \"data/a/sub\": {}", head, "");
        head = mkVirtual.commit("/", "> \"data/a/sub\": \"data/a/sub2\"", head, "");
        m = mkVirtual.getNodes("/data/a", mkVirtual.getHeadRevision(), 1, 0, -1, null);
        assertEquals("{\"data\":\"Hallo\",\":childNodeCount\":1,\"sub2\":{\":childNodeCount\":0}}", m);
        head = mkVirtual.commit("/", "- \"data/a/sub2\"", head, "");

        // remove node
        head = mkVirtual.commit("/", "- \"data/b\"", head, "");
        assertTrue(mkVirtual.nodeExists("/data/a", head));
        assertFalse(mkVirtual.nodeExists("/data/b", head));

    }

    @Test
    public void binary() {
        int len = 1000;
        byte[] data = new byte[len];
        new Random().nextBytes(data);
        String id = mkVirtual.write(new ByteArrayInputStream(data));
        byte[] test = new byte[len];
        mkVirtual.read(id, 0, test, 0, len);
        assertTrue(Arrays.equals(data, test));
        assertEquals(len, mkVirtual.getLength(id));
    }

}
