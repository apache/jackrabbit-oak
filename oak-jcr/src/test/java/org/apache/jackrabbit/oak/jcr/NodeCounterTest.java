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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

import javax.jcr.Session;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class NodeCounterTest extends AbstractRepositoryTest {

    public NodeCounterTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testEstimated() throws Exception {
        Session session = getAdminSession();
        JackrabbitNode rn = (JackrabbitNode) session.getRootNode();
        JackrabbitNode jn = (JackrabbitNode) rn.addNode("estimatedCountTest");
        session.save();
        long beforeNodes = jn.getEstimatedDescendantNodeCount();
        long nowNodes = beforeNodes;
        long start = System.currentTimeMillis();
        long maxSeconds = 60;
        int i = 0;

        // run until timeout of estimated count has increased
        while ((System.currentTimeMillis() - start) < TimeUnit.SECONDS.toMillis(maxSeconds)
            && nowNodes <= beforeNodes) {
            i += 1;
            jn.addNode("bar" + i);
            // save after 100 nodes
            if (i % 100 == 0) {
                session.save();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
            nowNodes = jn.getEstimatedDescendantNodeCount();
        }

        assertTrue("estimated node count did not increase within " + maxSeconds +
                " seconds, " + i + " nodes created, started with " + nowNodes + " estimation",
                nowNodes > beforeNodes);
    }
}
