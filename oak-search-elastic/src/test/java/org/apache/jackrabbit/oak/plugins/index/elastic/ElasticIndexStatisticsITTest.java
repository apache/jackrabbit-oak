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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for {@link ElasticIndexStatistics} against a real Elasticsearch cluster.
 * Covers only the default code path (feature toggle FT_OAK-12381 enabled) where {@code ElasticIndexStatistics}
 * does not require any cluster-level privileges in the Elasticsearch API key.
 */
public class ElasticIndexStatisticsITTest extends ElasticAbstractQueryTest {

    @Test
    public void storeStatsReflectRealIndex() throws Exception {
        long before = System.currentTimeMillis();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "bar");
        test.addChild("b").setProperty("foo", "bar");
        root.commit();

        IndexDefinitionBuilder builder = createIndex("foo");
        Tree index = setIndex("fooIndex", builder);
        root.commit();

        // wait until the index is refreshed and both documents are searchable, so that the
        // statistics retrieved below are not read before Elasticsearch made them available.
        assertEventually(() -> {
            assertTrue(exists(index));
            assertEquals(2, countDocuments(index));
        });

        ElasticIndexDefinition indexDefinition = getElasticIndexDefinition(index);
        ElasticIndexStatistics statistics = new ElasticIndexStatistics(esConnection, indexDefinition);

        assertEquals(2, statistics.numDocs());
        assertEquals(2, statistics.getDocCountFor("foo"));
        assertTrue("expected at least 2 lucene documents", statistics.luceneNumDocs() >= 2);
        assertEquals(0, statistics.luceneNumDeletedDocs());
        assertTrue("expected a positive store size", statistics.storeSize() > 0);
        assertTrue("expected a positive primary store size not exceeding the total store size",
                statistics.primaryStoreSize() > 0 && statistics.primaryStoreSize() <= statistics.storeSize());
        assertTrue("expected a creation date between test start and now",
                statistics.creationDate() >= before && statistics.creationDate() <= System.currentTimeMillis());
    }
}
