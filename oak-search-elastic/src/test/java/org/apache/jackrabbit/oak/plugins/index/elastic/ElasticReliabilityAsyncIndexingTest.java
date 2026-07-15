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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticRetryPolicy;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticReliabilityAsyncIndexingTest extends ElasticReliabilityTest {

    @Override
    public boolean useAsyncIndexing() {
        return true;
    }

    @Override
    public long getIndexCorruptIntervalInMillis() {
        return TimeUnit.DAYS.toMillis(7);
    }

    @Override
    public ElasticRetryPolicy getElasticRetryPolicy() {
        return ElasticRetryPolicy.NO_RETRY;
    }

    @Test
    public void connectionCutOnIndex() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb"));
        root.commit();

        // Do not wait for the lane to run, it would take too long, force it to run immediately
        asyncIndexUpdate.run();
        assertFalse(asyncIndexUpdate.isFailing());

        // The query should use the index that was just created
        String query = "select [jcr:path] from [nt:base] where propa is not null";
        assertThat(explain(query), containsString("elasticsearch:" + indexName));

        // Create initial content and check that it is indexed correctly
        Tree testTree = root.getTree("/").addChild("test");
        for (int i = 0; i < 10; i++) {
            Tree child = testTree.addChild("child" + i);
            child.setProperty("propa", "a" + i);
            root.commit();
        }
        asyncIndexUpdate.run();
        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, IntStream.range(0, 10).mapToObj(i -> "/test/child" + i).collect(Collectors.toList()));
        });

        // Set a toxic to cut the connection to Elastic after further 4KB of data sent upstream
        LimitData cutConnectionUpstream = proxy.toxics()
                .limitData("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 4096L);

        // Add more children to the test tree
        for (int i = 10; i < 50; i++) {
            Tree child = testTree.addChild("child" + i);
            child.setProperty("propa", "a" + i);
            root.commit();
        }
        // This time the async index update should fail due to the connection cut
        asyncIndexUpdate.run();
        assertTrue(asyncIndexUpdate.isFailing());

        // Remove the toxic to allow querying and further indexing
        cutConnectionUpstream.remove();
        // The query should still return the initial 10 children and eventually some of the new children , but none of the children that were added after the toxic was applied
        assertEventually(() -> {
            List<String> resultsAfter = executeQuery(query, SQL2);
            assertThat(
                    "Some but not all of the new entries should be reflected on the index",
                    10 < resultsAfter.size() && resultsAfter.size() < 50
            );
        });

        // Run a new async index cycle after the toxic is removed
        asyncIndexUpdate.run();
        assertFalse(asyncIndexUpdate.isFailing());

        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, IntStream.range(0, 50).mapToObj(i -> "/test/child" + i).collect(Collectors.toList()));
        });
    }
}
