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
package org.apache.jackrabbit.mk.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.apache.jackrabbit.mk.MultiMkTestBase;
import org.apache.jackrabbit.mk.util.Concurrent;
import org.apache.jackrabbit.mk.util.Concurrent.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test concurrent access to nodes, the journal, and revision.
 */
@RunWith(Parameterized.class)
public class ConcurrentTest extends MultiMkTestBase {

    public ConcurrentTest(String url) {
        super(url);
    }

    @Test
    public void test() throws Exception {
        final AtomicInteger id = new AtomicInteger();
        Concurrent.run(url, new Task() {
            public void call() throws Exception {
                long start = System.currentTimeMillis();
                String rev = mk.getHeadRevision();
                int i = id.getAndIncrement();
                String newRev = mk.commit("/", "+\"" + i + "\":{\"x\": " + i + "}", rev, "");
                Assert.assertTrue(!newRev.equals(rev));
                mk.getJournal(rev, newRev);
                mk.getRevisions(start, 100);
                mk.getNodes("/" + i, newRev);
                mk.getNodes("/" + i, newRev, 0, 0, 0);
                Assert.assertFalse(mk.nodeExists("/" + i, rev));
                Assert.assertTrue(mk.nodeExists("/" + i, newRev));
                rev = newRev;
                newRev = mk.commit("/", "-\"" + i + "\"", rev, "");
                Assert.assertTrue(mk.nodeExists("/" + i, rev));
                Assert.assertFalse(mk.nodeExists("/" + i, newRev));
            }
        });
    }

}
