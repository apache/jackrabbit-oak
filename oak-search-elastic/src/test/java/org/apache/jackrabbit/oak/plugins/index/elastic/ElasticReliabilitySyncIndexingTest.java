/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticReliabilitySyncIndexingTest extends ElasticReliabilityTest {

    @Override
    public boolean useAsyncIndexing() {
        return false;
    }

    @Test
    public void connectionCutOnQuery() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit(Map.of("sync-mode", "rt"));

        String query = "select [jcr:path] from [nt:base] where propa is not null";

        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/a", "/test/b"));
        });

        // simulate an upstream connection cut
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0L);

        assertEventually(() -> {
            // elastic is down, query should not use it
            assertThat(explain(query), not(containsString("elasticsearch:" + indexName)));

            // result set should be correct anyway since traversal is enabled
            assertQuery(query, List.of("/test/a", "/test/b"));
        });

        // re-establish connection
        cutConnectionUpstream.remove();

        assertEventually(() -> {
            // result set should be the same as before but this time elastic should be used
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/a", "/test/b"));
        });
    }

    @Test
    public void connectionCutOnIndex() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb"));

        // simulate an upstream connection cut
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 8192L);

        Tree testBefore = root.getTree("/").addChild("test");
        try {
            for (int i = 0; i < 100; i++) {
                Tree child = testBefore.addChild("child" + i);
                child.setProperty("propa", "a" + i);
                root.commit();
            }
        } catch (CommitFailedException cfe) {
            // ignore commit failures, we expect some due to the connection cut
            // Remove the toxic to allow further indexing
            cutConnectionUpstream.remove();
        }

        // Add a new child after the commit failure
        Tree testAfter = root.getTree("/").addChild("test").addChild("child-added-after-failure");
        testAfter.setProperty("propb", "b");
        root.commit();

        // Make sure that the child added after the commit failure is indexed correctly
        String query = "select [jcr:path] from [nt:base] where propb is not null";
        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/child-added-after-failure"));
        });
    }
}
