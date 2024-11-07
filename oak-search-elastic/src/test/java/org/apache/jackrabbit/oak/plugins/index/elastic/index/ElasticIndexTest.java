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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticIndexTest extends ElasticAbstractQueryTest {

    @Test
    public void indexStoresMappingVersion() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").propertyIndex();
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        assertEventually(() -> assertEquals(ElasticIndexDefinition.MAPPING_VERSION.toString(),
                getElasticIndexDefinition(index).getMappingVersion()));
    }

    @Test
    public void indexTrackerHandlesIndexDefinitionChanges() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a");
        builder.indexRule("nt:base").property("a").propertyIndex();
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        ElasticIndexDefinition elasticIndexDefinitionT0;
        ElasticIndexNode indexNodeT0 = indexTracker.acquireIndexNode(index.getPath());
        try {
            elasticIndexDefinitionT0 = indexNodeT0.getDefinition();
            assertNotNull(elasticIndexDefinitionT0);
        } finally {
            indexNodeT0.release();
        }

        Tree test = root.getTree("/").addChild("test");
        test.addChild("t").setProperty("a", "foo");
        root.commit();

        ElasticIndexDefinition elasticIndexDefinitionT1;
        ElasticIndexNode indexNodeT1 = indexTracker.acquireIndexNode(index.getPath());
        try {
            elasticIndexDefinitionT1 = indexNodeT1.getDefinition();
            assertNotNull(elasticIndexDefinitionT1);
        } finally {
            indexNodeT1.release();
        }

        // no changes in the index node and definition when the index content gets updated
        assertEquals(indexNodeT0, indexNodeT1);
        assertEquals(elasticIndexDefinitionT0, elasticIndexDefinitionT1);

        Tree b = index.getChild("indexRules").getChild("nt:base").getChild("properties").addChild("b");
        b.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        b.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        root.commit();

        ElasticIndexDefinition elasticIndexDefinitionT2;
        ElasticIndexNode indexNodeT2 = indexTracker.acquireIndexNode(index.getPath());
        try {
            elasticIndexDefinitionT2 = indexNodeT2.getDefinition();
            assertNotNull(elasticIndexDefinitionT2);
        } finally {
            indexNodeT2.release();
        }

        // index node and definition are different after the index definition change
        assertNotEquals(indexNodeT1, indexNodeT2);
        assertNotEquals(elasticIndexDefinitionT1, elasticIndexDefinitionT2);
        assertNotNull(elasticIndexDefinitionT2.getPropertiesByName().get("b"));
    }
}
