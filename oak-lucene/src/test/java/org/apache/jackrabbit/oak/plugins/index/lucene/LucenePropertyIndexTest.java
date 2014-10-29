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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.jcr.PropertyType;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorTest.createCal;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

public class LucenePropertyIndexTest extends AbstractQueryTest {
    /**
     * Set the size to twice the batch size to test the pagination with sorting
     */
    static final int NUMBER_OF_NODES = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2;

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexProvider provider = new LuceneIndexProvider();
        return new Oak()
                .with(new InitialContent())
                .with(new LuceneInitializerHelper("luceneGlobal", (Set<String>) null))
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Test
    public void indexSelection() throws Exception {
        createIndex("test1", of("propa", "propb"));
        createIndex("test2", of("propc"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
        assertThat(explain(propaQuery), containsString("lucene:test1"));
        assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"), containsString("lucene:test2"));

        assertQuery(propaQuery, asList("/test/a", "/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", asList("/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", asList("/test/d"));
    }

    @Test
    public void indexSelectionVsNodeType() throws Exception {
        Tree luceneIndex = createIndex("test1", of("propa"));
        // decrease cost of lucene property index
        luceneIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 5L, Type.LONG);

        // Decrease cost of node type index
        Tree nodeTypeIndex = root.getTree("/").getChild("oak:index").getChild("nodetype");
        nodeTypeIndex.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, 50L, Type.LONG);
        nodeTypeIndex.setProperty(IndexConstants.KEY_COUNT_PROPERTY_NAME, 10L, Type.LONG);

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        List<String> paths = Lists.newArrayList();
        for (int idx = 0; idx < 15; idx++) {
            Tree a = test.addChild("n"+idx);
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            a.setProperty("propa", "foo");
            paths.add("/test/n" + idx);
        }
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:unstructured] where [propa] = 'foo'";
        assertThat(explain(propaQuery), containsString("lucene:test1"));

        assertQuery(propaQuery, paths);
    }

    @Test
    public void declaringNodeTypeSameProp() throws Exception {
        createIndex("test1", of("propa"));

        Tree indexWithType = createIndex("test2", of("propa"));
        indexWithType.setProperty(PropertyStates
            .createProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:unstructured"),
                Type.STRINGS));

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        Tree a = test.addChild("a");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("propa", "foo");
        Tree b = test.addChild("b");
        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("propa", "foo");

        test.addChild("c").setProperty("propa", "foo");
        test.addChild("d").setProperty("propa", "foo");

        root.commit();

        String propabQuery = "select [jcr:path] from [nt:unstructured] where [propa] = 'foo'";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b"));

        String propcdQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
        assertThat(explain(propcdQuery), containsString("lucene:test1"));
        assertQuery(propcdQuery, asList("/test/a", "/test/b", "/test/c", "/test/d"));
    }

    @Test
    public void declaringNodeTypeSingleIndex() throws Exception {
        Tree indexWithType = createIndex("test2", of("propa", "propb"));
        indexWithType.setProperty(PropertyStates
            .createProperty(IndexConstants.DECLARING_NODE_TYPES, of("nt:unstructured"),
                Type.STRINGS));

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        Tree a = test.addChild("a");
        a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        a.setProperty("propa", "foo");
        a.setProperty("propb", "baz");

        Tree b = test.addChild("b");
        b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        b.setProperty("propa", "foo");
        b.setProperty("propb", "baz");

        root.commit();

        String propabQuery = "select [jcr:path] from [nt:unstructured] where [propb] = 'baz' and " +
            "[propa] = 'foo'";
        assertThat(explain(propabQuery), containsString("lucene:test2"));
        assertQuery(propabQuery, asList("/test/a", "/test/b"));

        String propNoIdxQuery = "select [jcr:path] from [nt:base] where [propb] = 'baz'";
        assertThat(explain(propNoIdxQuery), containsString("no-index"));
        assertQuery(propNoIdxQuery, ImmutableList.<String>of());
    }

    @Test
    public void emptyIndex() throws Exception{
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"), containsString("lucene:test1"));
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b"));
    }

    @Test
    public void rangeQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("b").setProperty("propa", 20);
        test.addChild("c").setProperty("propa", 30);
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] where [propa] >= 20"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <= 20", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < 20", asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20 or [propa] = 10 ", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > 10 and [propa] < 30", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] in (10,20)", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void rangeQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10.1);
        test.addChild("b").setProperty("propa", 20.4);
        test.addChild("c").setProperty("propa", 30.7);
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= 20.3", asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 20.4", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <= 20.5", asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < 20.4", asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > 10.5 and [propa] < 30", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Test
    public void rangeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "b is b");
        test.addChild("c").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        test.addChild("d").setProperty("propb", "f");
        test.addChild("e").setProperty("propb", "g");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa = 'a'", asList("/test/a"));
        //Check that string props are not tokenized
        assertQuery("select [jcr:path] from [nt:base] where propa = 'b is b'", asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa in ('a', 'c')", asList("/test/a", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] >= 'f'", asList("/test/d", "/test/e"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] <= 'f'", asList("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] > 'e'", asList("/test/d", "/test/e"));
        assertQuery("select [jcr:path] from [nt:base] where [propb] < 'g'", asList("/test/c", "/test/d"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }


    @Test
    public void rangeQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("propa");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", createCal("14/02/2014"));
        test.addChild("b").setProperty("propa", createCal("14/03/2014"));
        test.addChild("c").setProperty("propa", createCal("14/04/2014"));
        test.addChild("c").setProperty("propb", "foo");
        test.addChild("d").setProperty("propb", "foo");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where [propa] >= " + dt("15/02/2014"), asList("/test/b", "/test/c"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] <=" + dt("15/03/2014"), asList("/test/b", "/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] < " + dt("14/03/2014"), asList("/test/a"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] > "+ dt("15/02/2014") + " and [propa] < " + dt("13/04/2014"), asList("/test/b"));
        assertQuery("select [jcr:path] from [nt:base] where propa is not null", asList("/test/a", "/test/b", "/test/c"));
    }

    @Ignore("OAK-2190")
    @Test
    public void likeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "humpty");
        test.addChild("b").setProperty("propa", "dumpty");
        test.addChild("c").setProperty("propa", "humpy");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa like 'hum%'", asList("/test/a", "test/c"));
        assertQuery("select [jcr:path] from [nt:base] where propa like '%ty'", asList("/test/a", "/test/b"));
    }

    @Test
    public void nativeQueries() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        idx.setProperty(LuceneIndexConstants.FUNC_NAME, "foo");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "humpty");
        test.addChild("b").setProperty("propa", "dumpty");
        test.addChild("c").setProperty("propa", "humpy");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where native('foo', 'propa:(humpty OR dumpty)')",
                asList("/test/a", "/test/b"));
    }

    @Test
    public void indexDefinitionBelowRoot() throws Exception {
        Tree parent = root.getTree("/").addChild("test");
        Tree idx = createIndex(parent, "test1", of("propa", "propb"));
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = parent.addChild("test2");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] as s where ISDESCENDANTNODE(s, '/test') and propa = 'a'", asList("/test/test2/a"));
    }


    @Test
    public void sortQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_NotIndexed() throws Exception {
        Tree idx = createIndex("test1", Collections.<String>emptySet());
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] order by [foo]"), containsString("lucene:test1"));

        List<Tuple> tuples = createDataForLongProp();
        assertOrderedQuery("select [jcr:path] from [nt:base] order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base]  order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    void assertSortedLong() throws CommitFailedException {
        List<Tuple> tuples = createDataForLongProp();
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    private List<Tuple> createDataForLongProp() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Long> values = createLongs(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();
        return tuples;
    }

    @Test
    public void sortQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    @Test
    public void sortQueriesWithDouble_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

        assertSortedDouble();
    }

    void assertSortedDouble() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Double> values = createDoubles(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
            "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
            getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    @Test
    public void sortQueriesWithString_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        idx.addChild(PROP_NODE).addChild("foo");
        root.commit();

        assertSortedString();
    }

    void assertSortedString() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<String> values = createStrings(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
            "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
            getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    @Test
    public void sortQueriesWithDate_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        assertSortedDate();
    }

    void assertSortedDate() throws ParseException, CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", values.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple(values.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]",
            getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery(
            "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
            getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDateStringMixed_OrderedProps() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("bar"), STRINGS));
        idx.setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        List<Calendar> values = createDates(NUMBER_OF_NODES);
        List<Tuple> tuples = Lists.newArrayListWithCapacity(values.size());
        for(int i = 0; i < values.size(); i++){
            Tree child = test.addChild("n"+i);
            child.setProperty("bar", "baz");
            if (i != 0) {
                child.setProperty("foo", values.get(i));
                tuples.add(new Tuple(values.get(i), child.getPath()));
            } else {
                child.setProperty("foo", String.valueOf(values.get(i).getTimeInMillis()));
            }
        }
        root.commit();

        // Add the path of property added as timestamp string in the sorted list
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]",
            Lists.newArrayList(Iterables.concat(Lists.newArrayList("/test/n0"),
                getSortedPaths(tuples, OrderDirection.ASC))));
        // Append the path of property added as timestamp string to the sorted list
        assertOrderedQuery(
            "select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", Lists
            .newArrayList(Iterables.concat(getSortedPaths(tuples, OrderDirection.DESC),
                Lists.newArrayList("/test/n0"))));
    }

    @Test
    public void sortQueriesWithStringAndLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar", "baz"));
        Tree propIdx = idx.addChild(PROP_NODE).addChild("baz");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        int firstPropSize = 5;
        List<String> values = createStrings(firstPropSize);
        List<Long> longValues = createLongs(NUMBER_OF_NODES);
        List<Tuple2> tuples = Lists.newArrayListWithCapacity(values.size());
        Random r = new Random();
        for(int i = 0; i < values.size(); i++){
            String val = values.get(r.nextInt(firstPropSize));
            Tree child = test.addChild("n"+i);
            child.setProperty("foo", val);
            child.setProperty("baz", longValues.get(i));
            child.setProperty("bar", "baz");
            tuples.add(new Tuple2(val, longValues.get(i), child.getPath()));
        }
        root.commit();

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] asc, [baz] desc", getSortedPaths(tuples));
    }

    private void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, SQL2, true);
        assertEquals(paths, result);
    }

    //TODO Test for range with Date. Check for precision

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    private Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(LuceneIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        root.commit();
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    private static String dt(String date) throws ParseException {
        return String.format("CAST ('%s' AS DATE)",ISO8601.format(createCal(date)));
    }

    private static List<String> getSortedPaths(List<Tuple> tuples, OrderDirection dir) {
        if (OrderDirection.DESC == dir) {
            Collections.sort(tuples, Collections.reverseOrder());
        } else {
            Collections.sort(tuples);
        }
        List<String> paths = Lists.newArrayListWithCapacity(tuples.size());
        for (Tuple t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    private static List<String> getSortedPaths(List<Tuple2> tuples) {
        Collections.sort(tuples);
        List<String> paths = Lists.newArrayListWithCapacity(tuples.size());
        for (Tuple2 t : tuples) {
            paths.add(t.path);
        }
        return paths;
    }

    private static List<Long> createLongs(int n){
        List<Long> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(i);
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Double> createDoubles(int n){
        Random rnd = new Random();
        List<Double> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(rnd.nextDouble());
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<String> createStrings(int n){
        List<String> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(String.format("value%04d",i));
        }
        Collections.shuffle(values);
        return values;
    }

    private static List<Calendar> createDates(int n) throws ParseException {
        Random rnd = new Random();
        List<Calendar> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(createCal(String.format("%02d/%02d/2%03d", rnd.nextInt(26) + 1, rnd.nextInt(10) + 1,i)));
        }
        Collections.shuffle(values);
        return values;
    }

    private static class Tuple implements Comparable<Tuple>{
        final Comparable value;
        final String path;

        private Tuple(Comparable value, String path) {
            this.value = value;
            this.path = path;
        }

        @Override
        public int compareTo(Tuple o) {
            return value.compareTo(o.value);
        }

        @Override
        public String toString() {
            return "Tuple{" +
                    "value=" + value +
                    ", path='" + path + '\'' +
                    '}';
        }
    }

    private static class Tuple2 implements Comparable<Tuple2>{
        final Comparable value;
        final Comparable value2;
        final String path;

        private Tuple2(Comparable value, Comparable value2, String path) {
            this.value = value;
            this.value2 = value2;
            this.path = path;
        }

        @Override
        public int compareTo(Tuple2 o) {
            return ComparisonChain.start()
                    .compare(value, o.value)
                    .compare(value2, o.value2, Collections.reverseOrder())
                    .result();
        }

        @Override
        public String toString() {
            return "Tuple2{" +
                    "value=" + value +
                    ", value2=" + value2 +
                    ", path='" + path + '\'' +
                    '}';
        }
    }
}
