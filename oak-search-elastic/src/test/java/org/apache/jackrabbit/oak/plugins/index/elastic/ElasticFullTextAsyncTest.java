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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ElasticFullTextAsyncTest extends ElasticAbstractQueryTest {

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    @Test
    public void fullTextQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa").analyzed();

        String indexId = UUID.randomUUID().toString();
        setIndex(indexId, builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "Simple test");
        test.addChild("c").setProperty("propa", "Hello everyone. This is an elastic test");
        test.addChild("d").setProperty("propa", "howdy! hello again");
        root.commit();

        String query = "//*[jcr:contains(@propa, 'Hello')] ";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString("elasticsearch:" + indexId));
            assertQuery(query, XPATH, Arrays.asList("/test/a", "/test/c", "/test/d"));
        });
    }

    @Test
    public void noStoredIndexDefinition() throws Exception {
        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa").analyzed();

        String indexId = UUID.randomUUID().toString();
        setIndex(indexId, builder);
        root.commit();

        assertEventually(() -> {
            NodeState node = NodeStateUtils.getNode(nodeStore.getRoot(), "/" + INDEX_DEFINITIONS_NAME + "/" + indexId);
            PropertyState ps = node.getProperty(IndexConstants.REINDEX_COUNT);
            assertTrue(ps != null && ps.getValue(Type.LONG) == 1 && !node.hasChildNode(INDEX_DEFINITION_NODE));
        });
    }

    @Test
    public void nodeScopeIndexedQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a", "b").async("async");
        builder.indexRule("nt:base").property("a").analyzed().nodeScopeIndex();
        builder.indexRule("nt:base").property("b").analyzed().nodeScopeIndex();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("a", "hello");
        test.addChild("b").setProperty("a", "world");
        test.addChild("c").setProperty("a", "hello world");
        Tree d = test.addChild("d");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(., 'Hello')] ", XPATH, Arrays.asList("/test/a", "/test/c", "/test/d"));
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Arrays.asList("/test/c", "/test/d"));
        });
    }

    /*
    In this test only nodeScope property is set over index. (OAK-9166)
     */
    @Test
    public void onlyNodeScopeIndexedQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex(false, "nt:base", "a", "b").async("async");
        builder.indexRule("nt:base").property("a").nodeScopeIndex();
        builder.indexRule("nt:base").property("b").nodeScopeIndex();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("a", "hello");
        test.addChild("nodeb").setProperty("a", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(., 'Hello')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodec", "/test/noded"));
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Arrays.asList("/test/nodec", "/test/noded"));
        });
    }

    @Test
    public void propertyIndexWithNodeScopeIndexedQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a", "b").async("async");
        builder.indexRule("nt:base").property("a").nodeScopeIndex();
        builder.indexRule("nt:base").property("b").nodeScopeIndex();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("a", "hello");
        test.addChild("nodeb").setProperty("a", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(., 'Hello')] ", XPATH, Arrays.asList("/test/nodea", "/test/nodec", "/test/noded"));
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Arrays.asList("/test/nodec", "/test/noded"));
        });
    }

    /*
        In ES we don't add a property data to :fulltext if both nodescope and analyzed is set on index. Instead we use a
        multimatch query with cross_fields
        In this test only we set nodeScope on a property and on b property just analyzed property is set over index. (OAK-9166)
        contains query of type contain(., 'string') should not return b.
     */
    @Test
    public void onlyAnalyzedPropertyShouldNotBeReturnedForNodeScopeIndexedQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex(false, "nt:base", "a", "b").async("async");
        builder.indexRule("nt:base").property("a").nodeScopeIndex();
        builder.indexRule("nt:base").property("b").analyzed();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("b", "hello");
        test.addChild("nodeb").setProperty("b", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(., 'Hello')] ", XPATH, Arrays.asList("/test/nodec", "/test/noded"));
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Collections.singletonList("/test/nodec"));
        });
    }

    @Test
    public void fullTextMultiTermQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("analyzed_field");
        builder.async("async");
        builder.indexRule("nt:base").property("analyzed_field").analyzed();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "test123");
        test.addChild("b").setProperty("analyzed_field", "test456");
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(@analyzed_field, 'test123')] ", XPATH, Collections.singletonList("/test/a"))
        );
    }

    @Test
    public void fulltextWithModifiedNodeScopeIndex() throws Exception {
        IndexDefinitionBuilder builder = createIndex("analyzed_field");
        builder.async("async");
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed();

        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("analyzed_field", "sun.jpg");
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(@analyzed_field, 'SUN.JPG')] ", XPATH, Collections.singletonList("/test/a")));

        // add nodeScopeIndex at a later stage
        index.getChild("indexRules").getChild("nt:base").getChild("properties")
                .getChild("analyzed_field").setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(., 'jpg')] ", XPATH, Collections.singletonList("/test/a")));
    }

}
