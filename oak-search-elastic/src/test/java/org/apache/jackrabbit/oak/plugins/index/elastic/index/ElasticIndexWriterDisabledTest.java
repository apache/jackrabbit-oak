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
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ElasticIndexWriterDisabledTest extends ElasticAbstractQueryTest {

    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty(ElasticIndexEditorProvider.OAK_INDEX_ELASTIC_WRITER_DISABLE_KEY, "true");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void elasticDoNotIndexDocuments() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a");
        builder.indexRule("nt:base")
                .property("a")
                .propertyIndex().analyzed();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        for (int i = 0; i < 100; i++) {
            Tree c = content.addChild("c_" + i);
            c.setProperty("a", "foo");
            c.setProperty("b", "bar");
        }
        root.commit(Collections.singletonMap("sync-mode", "rt"));
        asyncIndexUpdate.run();

        List<String> results = executeQuery("select [jcr:path] from [nt:base] where contains(a, 'foo')", "JCR-SQL2");
        assertEquals(0, results.size());
        assertFalse(((IndexStatsMBean) asyncIndexUpdate.getIndexStats()).isFailing());
    }

    @Test
    public void elasticDoNotIndexOnReindex() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a");
        builder.indexRule("nt:base")
                .property("a")
                .propertyIndex().analyzed();

        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, builder);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        for (int i = 0; i < 100; i++) {
            Tree c = content.addChild("c_" + i);
            c.setProperty("a", "foo");
            c.setProperty("b", "bar");
        }
        root.commit(Collections.singletonMap("sync-mode", "rt"));
        asyncIndexUpdate.run();

        List<String> results = executeQuery("select [jcr:path] from [nt:base] where contains(a, 'foo')", "JCR-SQL2");
        assertEquals(0, results.size());
        assertFalse(((IndexStatsMBean) asyncIndexUpdate.getIndexStats()).isFailing());

        Tree indexNode = root.getTree("/").getChild(INDEX_DEFINITIONS_NAME).getChild(indexName);
        Tree b = indexNode.getChild("indexRules").getChild("nt:base").getChild("properties").addChild("b");
        b.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        b.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        // Now we reindex and see everything works fine
        indexNode.setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();

        // adding an extra content node (handled by reindex and potentially incremental indexing)
        Tree c = content.addChild("c_100");
        c.setProperty("a", "foo");
        c.setProperty("b", "bar");
        root.commit(Collections.singletonMap("sync-mode", "rt"));
        asyncIndexUpdate.run();

        results = executeQuery("select [jcr:path] from [nt:base] where contains(a, 'foo')", "JCR-SQL2");
        assertEquals(0, results.size());
        assertFalse(((IndexStatsMBean) asyncIndexUpdate.getIndexStats()).isFailing());
    }

}
