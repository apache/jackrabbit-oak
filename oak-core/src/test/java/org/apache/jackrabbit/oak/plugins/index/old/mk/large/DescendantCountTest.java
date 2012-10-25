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
package org.apache.jackrabbit.oak.plugins.index.old.mk.large;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast.Jsop;
import org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast.JsopObject;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the combined child node count (number of descendants).
 */
@RunWith(Parameterized.class)
public class DescendantCountTest extends MultiMkTestBase {

    public DescendantCountTest(String url) {
        super(url);
    }

    @Override
    @After
    public void tearDown() throws InterruptedException {
        if (isSimpleKernel(mk)) {
            mk.commit("/:root/head/config", "^ \"descendantCount\": false", mk.getHeadRevision(), "");
            mk.commit("/:root/head/config", "^ \"descendantCount\": null", mk.getHeadRevision(), "");
        }
        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        if (!isSimpleKernel(mk)) {
            return;
        }

        String head = mk.getHeadRevision();
        head = mk.commit("/:root/head/config", "^ \"descendantCount\": true", head, "");

        NodeCreator c = new NodeCreator(mk);
        for (int i = 1; i < 20; i++) {
            c.setTestRoot("test" + i);
            c.setWidth(2);
            c.setTotalCount(i);
            c.setData(null);
            c.create();
            head = mk.getHeadRevision();
            String json = JsopBuilder.prettyPrint(mk.getNodes("/test" + i, head, Integer.MAX_VALUE, 0, -1, null));
            NodeImpl n = NodeImpl.parse(json);
            long count = count(n);
            JsopObject o = (JsopObject) Jsop.parse(json);
            Object d = o.get(NodeImpl.DESCENDANT_COUNT);
            if (d != null) {
                long descendants = Long.parseLong(d.toString());
                if (count != descendants) {
                    assertEquals(json, count, descendants + 1);
                }
                d = o.get(NodeImpl.DESCENDANT_INLINE_COUNT);
                long inline = Long.parseLong(d.toString());
                if (inline <= 0) {
                    // at least 1
                    assertEquals(json, 1, inline);
                }
            } else if (i > 10) {
                fail();
            }
        }
    }

    long count(NodeImpl n) {
        int count = 1;
        for (long pos = 0;; pos++) {
            String childName = n.getChildNodeName(pos);
            if (childName == null) {
                break;
            }
            NodeImpl c = n.getNode(childName);
            count += count(c);
        }
        return count;
    }

}
