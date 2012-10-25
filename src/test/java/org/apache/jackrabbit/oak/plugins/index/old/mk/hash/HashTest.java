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
package org.apache.jackrabbit.oak.plugins.index.old.mk.hash;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests the indexing mechanism.
 */
@RunWith(Parameterized.class)
public class HashTest extends MultiMkTestBase {

    private String head;

    public HashTest(String url) {
        super(url);
    }

    @Override
    @After
    public void tearDown() throws InterruptedException {
        if (isSimpleKernel(mk)) {
            head = mk.commit("/:root/head/config", "^ \"hash\": false", head, "");
            head = mk.commit("/:root/head/config", "^ \"hash\": null", head, "");
        }
        super.tearDown();
    }

    @Test
    public void getHash() {
        head = mk.getHeadRevision();
        if (isSimpleKernel(mk)) {
            head = mk.commit("/:root/head/config", "^ \"hash\": true", head, "");
        } else {
            int todo;
            return;
        }

        head = mk.commit("/", "+ \"test1\": { \"id\": 1 }", mk.getHeadRevision(), "");
        head = mk.commit("/", "+ \"test2\": { \"id\": 1 }", mk.getHeadRevision(), "");
        NodeImpl r = NodeImpl.parse(mk.getNodes("/", head, 1, 0, -1, null));
        assertTrue(r.getHash() != null);
        NodeImpl t1 = NodeImpl.parse(mk.getNodes("/test1", head, 1, 0, -1, null));
        NodeImpl t2 = NodeImpl.parse(mk.getNodes("/test2", head, 1, 0, -1, null));
        assertTrue(Arrays.equals(t1.getHash(), t2.getHash()));
        assertFalse(Arrays.equals(t1.getHash(), r.getHash()));
    }

}
