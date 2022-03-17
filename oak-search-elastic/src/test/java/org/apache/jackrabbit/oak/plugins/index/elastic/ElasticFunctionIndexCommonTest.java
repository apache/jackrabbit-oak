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

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.FunctionIndexCommonTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditor;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ElasticFunctionIndexCommonTest extends FunctionIndexCommonTest {
    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(
            ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    @Rule
    public TestName name = new TestName();

    public ElasticFunctionIndexCommonTest() {
        indexOptions = new ElasticIndexOptions();
    }

    @Before
    public void filter() {
        // assumeTrue(name.getMethodName().equals("lengthName"));
    }

    @Test
    public void sameOrderableRelPropWithAndWithoutFunc_checkOrdering() throws Exception {

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        nonFunc.setProperty("name", "jcr:content/n/foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        root.commit();

        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree a = test.addChild("jcr:content");
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree b = a.addChild("n");

            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("foo", "bar" + i);
            i++;
        }

        root.commit();
            postCommitHook();

        // Check ordering works for func and non func properties
        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        // Now we change the value of foo on already indexed nodes and see if changes
        // get indexed properly.

        i = 5;
        for (String node : asList("a", "c", "b", "e", "d")) {

            Tree test = root.getTree("/").getChild(node).getChild("jcr:content").getChild("n");

            test.setProperty("foo", "bar" + i);
            i--;
        }
        root.commit();
        postCommitHook();

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/d", "/e", "/b", "/c", "/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                getIndexProvider() + "upper(/oak:index/upper)", asList("/a", "/c", "/b", "/e", "/d"));

    }

    private void assertOrderedPlanAndQuery(String query, String planExpectation, List<String> paths) {
        List<String> result = assertPlanAndQuery(query, planExpectation, paths);
        assertEquals("Ordering doesn't match", paths, result);
    }

    private List<String> assertPlanAndQuery(String query, String planExpectation, List<String> paths) {
        assertThat(explain(query), containsString(planExpectation));
        return assertQuery(query, paths);
    }

    @Override
    protected String getIndexProvider() {
        return "elasticsearch:";
    }

    @Override
    protected void postCommitHook() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected ContentRepository createRepository() {
        ElasticTestRepositoryBuilder builder = new ElasticTestRepositoryBuilder(elasticRule);
        builder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        repositoryOptionsUtil = builder.build();

        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        // setTraversalEnabled(false);
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    protected Tree createIndex(Tree index, String name, Set<String> propNames) {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(
                PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        // def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    @Override
    protected String getLoggerName() {
        return ElasticIndexEditor.class.getName();
    }
}
