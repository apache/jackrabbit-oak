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

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class ElasticIndexInfoProviderTest extends ElasticAbstractQueryTest {

    @After
    public void resetToggle() {
        ElasticFeatureToggles.FT_OAK_12381_DISABLE.set(false);
    }

    @Test
    public void isValidWithIndexOnlyPermissions() throws Exception {
        Tree index = createSearchableIndex(0);

        assertTrue("A healthy index should be valid without cluster-level privileges",
                createInfoProvider().isValid(index.getPath()));
    }

    @Test
    public void isInvalidWithUnassignedReplica() throws Exception {
        assumeTrue("Requires the single-node test container", elasticRule.useDocker());
        Tree index = createSearchableIndex(1);

        assertFalse("An index with an unassigned replica should not be valid",
                createInfoProvider().isValid(index.getPath()));
    }

    @Test
    public void legacyValidationRequiresClusterPermissions() throws Exception {
        assumeTrue("Requires the index-only API key from the test container", elasticRule.useDocker());
        Tree index = createSearchableIndex(0);
        ElasticFeatureToggles.FT_OAK_12381_DISABLE.set(true);

        ElasticsearchException exception = assertThrows(ElasticsearchException.class,
                () -> createInfoProvider().isValid(index.getPath()));
        assertEquals(403, exception.status());
    }

    private Tree createSearchableIndex(int replicas) throws Exception {
        root.getTree("/").addChild("test").setProperty("foo", "bar");
        root.commit();

        Tree index = setIndex("fooIndex", createIndex("foo"));
        index.setProperty(ElasticIndexDefinition.NUMBER_OF_REPLICAS, replicas);
        root.commit();

        assertEventually(() -> {
            assertTrue(exists(index));
            assertEquals(1, countDocuments(index));
        });

        return index;
    }

    private ElasticIndexInfoProvider createInfoProvider() {
        return new ElasticIndexInfoProvider(
                nodeStore, indexTracker, new AsyncIndexInfoServiceImpl(nodeStore));
    }
}
