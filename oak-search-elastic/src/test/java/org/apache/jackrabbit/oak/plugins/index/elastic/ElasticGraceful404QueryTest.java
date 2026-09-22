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
import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OAK-12248: graceful handling of ES 404 (alias not found) during query.
 */
public class ElasticGraceful404QueryTest extends ElasticAbstractQueryTest {

    private static final String QUERY = "SELECT * FROM [nt:base] WHERE [ghost] IS NOT NULL OPTION(TRAVERSAL FAIL)";

    @After
    public void resetToggle() {
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(false);
    }

    private Tree provisionIndex() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("ghost", "value");
        test.addChild("b").setProperty("ghost", "value");
        root.commit();

        IndexDefinitionBuilder builder = createIndex("ghost");
        builder.indexRule("nt:base").property("ghost").propertyIndex();
        Tree index = setIndex("ghostIndex", builder);
        root.commit();

        assertEventually(() -> {
            assertTrue(exists(index));
            assertEquals(2, countDocuments(index));
        });
        return index;
    }

    private void deleteAlias(Tree index) throws Exception {
        esConnection.getClient().indices().delete(i -> i
                .index(getElasticIndexDefinition(index).getIndexAlias() + "*"));
    }

    @Test
    public void queryOnMissingAlias_withToggleOff_failsWithTraversalError() throws Exception {
        Tree index = provisionIndex();
        deleteAlias(index);

        List<String> results = executeQuery(QUERY, SQL2);

        assertEquals(1, results.size());
        assertTrue("Expected traversal-fail error when toggle is off and alias is missing",
                results.get(0).contains("Traversal"));
    }

    @Test
    public void queryOnMissingAlias_withToggleOn_returnsEmpty() throws Exception {
        Tree index = provisionIndex();
        deleteAlias(index);
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);

        List<String> results = executeQuery(QUERY, SQL2);

        assertEquals("Expected empty result set when alias is missing and toggle is on",
                0, results.size());
    }
}
