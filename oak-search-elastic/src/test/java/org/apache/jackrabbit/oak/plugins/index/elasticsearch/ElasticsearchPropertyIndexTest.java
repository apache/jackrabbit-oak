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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticsearchPropertyIndexTest extends AbstractQueryTest {
    @Rule
    public ElasticsearchManagementRule esMgmt = new ElasticsearchManagementRule();

    @Override
    protected ContentRepository createRepository() {
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(esMgmt,
                new ExtractedTextCache(10* FileUtils.ONE_MB, 100));
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(esMgmt);

        // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
        oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());

        NodeStore nodeStore = new MemoryNodeStore(builder.getNodeState());

        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Test
    public void indexSelection() throws Exception {
        setIndex("test1", createIndex(of("propa", "propb")));
        setIndex("test2", createIndex(of("propc")));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
        assertThat(explain(propaQuery), containsString("elasticsearch:test1"));
        assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"), containsString("elasticsearch:test2"));

        assertQuery(propaQuery, asList("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", asList("/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", asList("/test/d"));
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception{
        //make index
        IndexDefinitionBuilder builder = createIndex(Collections.EMPTY_SET);
        builder.includedPaths("/test")
                .evaluatePathRestrictions()
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME).propertyIndex();
        setIndex("test1", builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("foo");
        test.addChild("camelCase");
        test.addChild("sc").addChild("bar");
        root.commit();

        String queryPrefix = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/test') AND ";
        //test
        String propabQuery = queryPrefix + "LOCALNAME() = 'foo'";
        String explanation = explain(propabQuery);
        Assert.assertThat(explanation, containsString("elasticsearch:test1(/oak:index/test1) "));
        Assert.assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\","));
        assertQuery(propabQuery, Arrays.asList("/test/foo"));
        assertQuery(queryPrefix + "LOCALNAME() = 'bar'", Arrays.asList("/test/sc/bar"));
        assertQuery(queryPrefix + "LOCALNAME() LIKE 'foo'", Arrays.asList("/test/foo"));
        assertQuery(queryPrefix + "LOCALNAME() LIKE 'camel%'", Arrays.asList("/test/camelCase"));

        assertQuery(queryPrefix + "NAME() = 'bar'", Arrays.asList("/test/sc/bar"));
        assertQuery(queryPrefix + "NAME() LIKE 'foo'", Arrays.asList("/test/foo"));
        assertQuery(queryPrefix + "NAME() LIKE 'camel%'", Arrays.asList("/test/camelCase"));
    }

    @Test
    public void emptyIndex() throws Exception{
        setIndex("test1", createIndex(of("propa", "propb")));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        Assert.assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"), containsString("elasticsearch:test1"));
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        setIndex("test1", createIndex(of("propa", "propb")));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa is not null", Arrays.asList("/test/a", "/test/b"));
    }

    private static IndexDefinitionBuilder createIndex(Set<String> propNames) {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder().noAsync();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        propNames.forEach(propName -> indexRule.property(propName).propertyIndex());

        return builder;
    }

    private void setIndex(String idxName, IndexDefinitionBuilder builder) {
        builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }
}