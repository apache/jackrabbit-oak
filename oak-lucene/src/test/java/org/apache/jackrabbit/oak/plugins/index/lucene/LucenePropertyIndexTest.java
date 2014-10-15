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
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorTest.createCal;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
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
    public void rangeQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild("propa");
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
    }

    @Test
    public void rangeQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild("propa");
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
    }

    @Test
    public void rangeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild("propa");
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
    }


    @Test
    public void rangeQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        Tree propIdx = idx.addChild("propa");
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
    }

    @Ignore("OAK-2190")
    @Test
    public void likeQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.addChild("propa");
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
    public void sortQueriesWithLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        root.commit();

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

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDouble() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DOUBLE);
        root.commit();

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
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithString() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        idx.addChild("foo");
        root.commit();

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
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithDate() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar"));
        Tree propIdx = idx.addChild("foo");
        propIdx.setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        root.commit();

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

        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]", getSortedPaths(tuples, OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC", getSortedPaths(tuples, OrderDirection.DESC));
    }

    @Test
    public void sortQueriesWithStringAndLong() throws Exception {
        Tree idx = createIndex("test1", of("foo", "bar", "baz"));
        Tree propIdx = idx.addChild("baz");
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
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(LuceneIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        root.commit();
        return root.getTree("/").getChild(INDEX_DEFINITIONS_NAME).getChild(name);
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
            values.add(createCal(String.format("%02d/%02d/2014", rnd.nextInt(29) + 1, rnd.nextInt(11) + 1)));
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
