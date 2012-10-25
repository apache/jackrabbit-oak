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

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;

import org.apache.jackrabbit.oak.plugins.index.old.mk.MultiMkTestBase;
import org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast.Jsop;
import org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast.JsopArray;
import org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast.JsopObject;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test moving nodes.
 */
@RunWith(Parameterized.class)
public class ManyRevisionsTest extends MultiMkTestBase {

    public ManyRevisionsTest(String url) {
        super(url);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void readRevisions() {
        String head = mk.getHeadRevision();
        head = mk.commit("/", "+\"test\" : {\"id\": -1}", head, "\"-1\"");
        String first = head;
        ArrayList<String> revs = new ArrayList<String>();
        for (int i = 0; i < 1000; i++) {
            String rev = mk.commit("/", "^ \"test/id\": " + i, head, "\"" + i + "\"");
            revs.add(rev);
            head = rev;
        }
        int i = 0;
        String last = first;
        for (String rev : revs) {
            String n = mk.getNodes("/test", rev, 1, 0, -1, null);
            NodeImpl node = NodeImpl.parse(n);
            assertEquals(i, Integer.parseInt(node.getProperty("id")));
            String journal = mk.getJournal(last, rev, null);
            JsopArray array = (JsopArray) Jsop.parse(journal);
            assertEquals(last + ".." + rev + ": " + journal,
                    2, array.size());
            JsopObject obj = (JsopObject) array.get(0);
            assertEquals("\"" + (i - 1) + "\"", obj.get("msg"));
            obj = (JsopObject) array.get(1);
            assertEquals("\"" + i + "\"", obj.get("msg"));
            last = rev;
            i++;
        }
    }

    @Test
    public void smallWrites() {
        String head = mk.getHeadRevision();
        log(url);
        StopWatch watch = new StopWatch();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            head = mk.commit("/", "^ \"x\": " + (i % 10), head, "");
            if (i % 100 == 0 && watch.log()) {
                log(watch.operationsPerSecond(i));
            }
        }
        log(watch.operationsPerSecond(count));
    }

    private void log(String s) {
        // System.out.println(s);
    }

}
