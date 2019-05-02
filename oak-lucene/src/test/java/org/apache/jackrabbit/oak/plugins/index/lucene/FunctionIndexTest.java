/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class FunctionIndexTest extends AbstractQueryTest {

    private LuceneIndexEditorProvider editorProvider;

    private NodeStore nodeStore;
    
    @Override
    protected ContentRepository createRepository() {
        editorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        nodeStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }
    
    @Test
    public void noIndexTest() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        for (int idx = 0; idx < 3; idx++) {
            Tree low = test.addChild("" + (char) ('a' + idx));
            low.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree up = test.addChild("" + (char) ('A' + idx));
            up.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();
        
        String query = "select [jcr:path] from [nt:base] where lower(localname()) = 'b'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, Lists.newArrayList("/test/b", "/test/B"));
        
        String queryXPath = "/jcr:root/test//*[fn:lower-case(fn:local-name()) = 'b']";
        assertThat(explainXpath(queryXPath), containsString("traverse"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/b", "/test/B"));

        queryXPath = "/jcr:root/test//*[fn:lower-case(fn:local-name()) > 'b']";
        assertThat(explainXpath(queryXPath), containsString("traverse"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/c", "/test/C"));

        query = "select [jcr:path] from [nt:base] where lower(localname()) = 'B'";
        assertThat(explain(query), containsString("traverse"));
        assertQuery(query, Lists.<String>newArrayList());
    }
    
    @Test
    public void lowerCaseLocalName() throws Exception {
        Tree luceneIndex = createIndex("lowerLocalName", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths", 
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lowerLocalName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "lower(localname())");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 0; idx < 3; idx++) {
            Tree low = test.addChild("" + (char) ('a' + idx));
            low.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree up = test.addChild("" + (char) ('A' + idx));
            up.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:base] where lower(localname()) = 'b'";
        assertThat(explain(query), containsString("lucene:lowerLocalName"));
        assertQuery(query, Lists.newArrayList("/test/b", "/test/B"));
        
        String queryXPath = "/jcr:root//*[fn:lower-case(fn:local-name()) = 'b']";
        assertThat(explainXpath(queryXPath), containsString("lucene:lowerLocalName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/b", "/test/B"));

        queryXPath = "/jcr:root//*[fn:lower-case(fn:local-name()) > 'b']";
        assertThat(explainXpath(queryXPath), containsString("lucene:lowerLocalName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/c", "/test/C", "/test"));

        query = "select [jcr:path] from [nt:base] where lower(localname()) = 'B'";
        assertThat(explain(query), containsString("lucene:lowerLocalName"));
        assertQuery(query, Lists.<String>newArrayList());
    }
    
    @Test
    public void lengthName() throws Exception {
        Tree luceneIndex = createIndex("lengthName", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths", 
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lengthName");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
        func.setProperty(FulltextIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:string-length(fn:name())");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 1; idx < 1000; idx *= 10) {
            Tree testNode = test.addChild("test" + idx);
            testNode.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:base] where length(name()) = 6";
        assertThat(explain(query), containsString("lucene:lengthName"));
        assertQuery(query, Lists.newArrayList("/test/test10"));
        
        String queryXPath = "/jcr:root//*[fn:string-length(fn:name()) = 7]";
        assertThat(explainXpath(queryXPath), containsString("lucene:lengthName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/test100"));

        queryXPath = "/jcr:root//* order by fn:string-length(fn:name())";
        assertThat(explainXpath(queryXPath), containsString("lucene:lengthName"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList(
                "/test", "/test/test1", "/test/test10", "/test/test100"));
    }
    
    @Test
    public void length() throws Exception {
        Tree luceneIndex = createIndex("length", Collections.<String>emptySet());
        luceneIndex.setProperty("excludedPaths", 
                Lists.newArrayList("/jcr:system", "/oak:index"), Type.STRINGS);
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lengthName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:string-length(@value)");

        Tree test = root.getTree("/").addChild("test");
        for (int idx = 1; idx <= 1000; idx *= 10) {
            Tree testNode = test.addChild("test" + idx);
            testNode.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            testNode.setProperty("value", new byte[idx]);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:base] where length([value]) = 100";
        assertThat(explain(query), containsString("lucene:length"));
        assertQuery(query, Lists.newArrayList("/test/test100"));
        
        String queryXPath = "/jcr:root//*[fn:string-length(@value) = 10]";
        assertThat(explainXpath(queryXPath), containsString("lucene:length"));
        assertQuery(queryXPath, "xpath", Lists.newArrayList("/test/test10"));
    }
    
    @Test
    public void upperCase() throws Exception {
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("upperName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(@name)");

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n"+idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("name", "10% foo");
            paths.add("/test/n" + idx);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:unstructured] where upper([name]) = '10% FOO'";
        assertThat(explain(query), containsString("lucene:upper"));
        assertQuery(query, paths);
        
        query = "select [jcr:path] from [nt:unstructured] where upper([name]) like '10\\% FOO'";
        assertThat(explain(query), containsString("lucene:upper"));
        assertQuery(query, paths);        
    }

    @Test
    public void upperCaseRelative() throws Exception {
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("upperName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "upper([data/name])");

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n"+idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree b = a.addChild("data");
            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("name", "foo");
            paths.add("/test/n" + idx);
        }
        root.commit();

        String query = "select [jcr:path] from [nt:unstructured] where upper([data/name]) = 'FOO'";
        assertThat(explain(query), containsString("lucene:upper"));
        assertQuery(query, paths);
        
        String queryXPath = "/jcr:root//element(*, nt:unstructured)[fn:upper-case(data/@name) = 'FOO']";
        assertThat(explainXpath(queryXPath), containsString("lucene:upper"));
        assertQuery(queryXPath, "xpath", paths);
        
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.getChild("n"+idx);
            Tree b = a.getChild("data");
            b.setProperty("name", "bar");
        }
        root.commit();
        
        query = "select [jcr:path] from [nt:unstructured] where upper([data/name]) = 'BAR'";
        assertThat(explain(query), containsString("lucene:upper"));
        assertQuery(query, paths);
        
        queryXPath = "/jcr:root//element(*, nt:unstructured)[fn:upper-case(data/@name) = 'BAR']";
        assertThat(explainXpath(queryXPath), containsString("lucene:upper"));
        assertQuery(queryXPath, "xpath", paths);
    }

    @Test
    public void coalesce() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo", null).function(
                "lower(coalesce([jcr:content/foo2], coalesce([jcr:content/foo], localname())))"
        );

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("jcr:content").setProperty("foo2", "BAR");
        rootTree.addChild("b").addChild("jcr:content").setProperty("foo", "bAr");
        Tree child = rootTree.addChild("c").addChild("jcr:content");
        child.setProperty("foo", "bar");
        child.setProperty("foo2", "bar1");
        rootTree.addChild("bar");

        root.commit();

        assertPlanAndQuery(
                "select * from [nt:base] where lower(coalesce([jcr:content/foo2], coalesce([jcr:content/foo], localname()))) = 'bar'",
                "lucene:test1(/oak:index/test1)", asList("/a", "/b", "/bar"));
    }

    @Test
    public void coalesceOrdering() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync();
        idxb.indexRule("nt:base").property("foo", null).function(
                "coalesce([jcr:content/foo2], [jcr:content/foo])"
        ).ordered();

        Tree idx = root.getTree("/").getChild("oak:index").addChild("test1");
        idxb.build(idx);
        root.commit();

        Tree rootTree = root.getTree("/");
        rootTree.addChild("a").addChild("jcr:content").setProperty("foo2", "a");
        rootTree.addChild("b").addChild("jcr:content").setProperty("foo", "b");
        Tree child = rootTree.addChild("c").addChild("jcr:content");
        child.setProperty("foo", "c");
        child.setProperty("foo2", "a1");

        root.commit();

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by coalesce([jcr:content/foo2], [jcr:content/foo])",
                "lucene:test1(/oak:index/test1)", asList("/a", "/c", "/b"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by coalesce([jcr:content/foo2], [jcr:content/foo]) DESC",
                "lucene:test1(/oak:index/test1)", asList("/b", "/c", "/a"));
    }

    /*
    <OAK-8166>
    Given an index def with 2 orderable property definitions(Relative) for same property - one with function and one without
    Indexer should not fail to index the nodes covered by this index
     */
    @Test
    public void sameOrderableRelativePropertyWithAndWithoutFunction() throws Exception {

        LogCustomizer customLogs = LogCustomizer.forLogger(LuceneIndexEditor.class.getName()).enable(Level.WARN).create();
        // Create nodes that will be served by the index definition that follows
        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree a = test.addChild("jcr:content");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree b = a.addChild("n");

        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("foo", "bar");

        root.commit();

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:unstructured")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        nonFunc.setProperty("name", "jcr:content/n/foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:unstructured")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        // Now do some change in the node that are covered by above index definition
        try {
            customLogs.starting();
            root.getTree("/").getChild("test").getChild("jcr:content").getChild("n").setProperty("foo","bar2");
            root.commit();
            Assert.assertFalse(customLogs.getLogs().contains("Failed to index the node [/test]"));
            Assert.assertTrue(customLogs.getLogs().size() == 0);
        } finally {
            customLogs.finished();
        }

    }
    /*
    Given an index def with 2 orderable property definitions(Relative) for same property - one with function and one without
    Order by should give correct results
     */
    @Test
    public void sameOrderableRelPropWithAndWithoutFunc_checkOrdering() throws Exception {

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX,true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        nonFunc.setProperty("name", "jcr:content/n/foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(jcr:content/n/@foo)");

        root.commit();


        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b","e","d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree a = test.addChild("jcr:content");
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree b = a.addChild("n");

            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty("foo", "bar"+i);
            i++;
        }

        root.commit();


        // Check ordering works for func and non func properties
        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));


        // Now we change the value of foo on already indexed nodes and see if changes get indexed properly.

        i = 5;
        for (String node : asList("a", "c", "b","e","d")) {

            Tree test = root.getTree("/").getChild(node).getChild("jcr:content").getChild("n");

            test.setProperty("foo", "bar" + i);
            i--;
        }
        root.commit();

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo])",
                "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo]",
                "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by upper([jcr:content/n/foo]) DESC",
                "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

        assertOrderedPlanAndQuery(
                "select * from [nt:base] order by [jcr:content/n/foo] DESC",
                "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

    }

    /*
    Given an index def with 2 orderable property definitions(non-relative) for same property - one with function and one without
    Indexer should index any changes properly and ordering should work as expected.
     */
    @Test
    public void sameOrderablePropertyWithandWithoutFunction() throws Exception {
        LogCustomizer customLogs = LogCustomizer.forLogger(LuceneIndexEditor.class.getName()).enable(Level.WARN).create();
        // Create nodes that will be served by the index definition that follows
        int i = 1;
        // Create nodes that will be served by the index definition that follows
        for (String node : asList("a", "c", "b","e","d")) {

            Tree test = root.getTree("/").addChild(node);
            test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            test.setProperty("foo", "bar"+i);
            i++;
        }

        root.commit();

        // Index def with same property - ordered - one with function and one without
        Tree luceneIndex = createIndex("upper", Collections.<String>emptySet());
        Tree nonFunc = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("foo");
        nonFunc.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        nonFunc.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX,true);
        nonFunc.setProperty("name", "foo");

        Tree func = luceneIndex.getChild(FulltextIndexConstants.INDEX_RULES)
                .getChild("nt:base")
                .getChild(FulltextIndexConstants.PROP_NODE)
                .addChild("testOak");
        func.setProperty(FulltextIndexConstants.PROP_ORDERED,true);
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "fn:upper-case(@foo)");

        // Now do some change in the node that are covered by above index definition
        try {
            customLogs.starting();
             i = 5;
            for (String node : asList("a", "c", "b","e","d")) {

                Tree test = root.getTree("/").addChild(node);
                test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

                test.setProperty("foo", "bar"+i);
                i--;
            }

            root.commit();
            Assert.assertFalse(customLogs.getLogs().contains("Failed to index the node [/test]"));
            Assert.assertTrue(customLogs.getLogs().size() == 0);

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by upper([foo])",
                    "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by [foo]",
                    "lucene:upper(/oak:index/upper)", asList("/d","/e","/b","/c","/a"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by upper([foo]) DESC",
                    "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

            assertOrderedPlanAndQuery(
                    "select * from [nt:base] order by [foo] DESC",
                    "lucene:upper(/oak:index/upper)", asList("/a","/c","/b","/e","/d"));

        } finally {
            customLogs.finished();
        }

    }


    protected String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    protected String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        String plan = row.getValue("plan").getValue(Type.STRING);
        return plan;
    }

    private void assertOrderedPlanAndQuery(String query, String planExpectation, List<String> paths) {
        List<String> result = assertPlanAndQuery(query, planExpectation, paths);
        assertEquals("Ordering doesn't match", paths, result);
    }

    private List<String> assertPlanAndQuery(String query, String planExpectation, List<String> paths){
        assertThat(explain(query), containsString(planExpectation));
        return assertQuery(query, paths);
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }
    
    static Tree createIndex(Tree index, String name, Set<String> propNames) {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }    

}
