package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import javax.jcr.PropertyType;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public abstract class OrderByCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void orderByScore() throws Exception {
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false, "propa"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        root.commit();

        assertQuery("select [jcr:path] from [nt:base] where propa is not null order by [jcr:score]", singletonList("/test/a"));
    }

    @Test
    public void sortOnNodeName() throws Exception {
        // setup function index on "fn:name()"
        IndexDefinitionBuilder idbFnName = indexOptions.createIndexDefinitionBuilder();
        idbFnName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("fn:name()");
        idbFnName.getBuilderTree().setProperty("tags", of("fnName"), STRINGS);
        indexOptions.setIndex(root, "fnName", indexOptions.createIndex(idbFnName, false));

        // same index as above except for function - "name()"
        IndexDefinitionBuilder idbName = indexOptions.createIndexDefinitionBuilder();
        idbName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("name()");
        idbName.getBuilderTree().setProperty("tags", of("name"), STRINGS);
        indexOptions.setIndex(root, "name", indexOptions.createIndex(idbName, false));

        Tree testTree = root.getTree("/").addChild("test");
        List<String> expected = IntStream.range(0, 3).mapToObj(i -> {
            String nodeName = "a" + i;
            testTree.addChild(nodeName);
            return "/test/" + nodeName;
        }).sorted().collect(Collectors.toList());
        root.commit();

        String query = "/jcr:root/test/* order by fn:name() option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() ascending option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() descending option(index tag fnName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnName(/oak:index/fnName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));

        // order by fn:name() although function index is on "name()"
        query = "/jcr:root/test/* order by fn:name() option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() ascending option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:name() descending option(index tag name)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":name(/oak:index/name)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));
    }

    @Test
    public void sortOnLocalName() throws Exception {
        // setup function index on "fn:local-name()"
        IndexDefinitionBuilder idbFnLocalName = indexOptions.createIndexDefinitionBuilder();
        idbFnLocalName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("fn:local-name()");
        idbFnLocalName.getBuilderTree().setProperty("tags", of("fnLocalName"), STRINGS);
        indexOptions.setIndex(root, "fnLocalName", indexOptions.createIndex(idbFnLocalName, false));

        // same index as above except for function - "localName()"
        IndexDefinitionBuilder idbLocalName = indexOptions.createIndexDefinitionBuilder();
        idbLocalName.indexRule("nt:base")
                .property("nodeName", null)
                .propertyIndex()
                .ordered()
                .function("localname()");
        idbLocalName.getBuilderTree().setProperty("tags", of("localName"), STRINGS);
        indexOptions.setIndex(root, "localName", indexOptions.createIndex(idbLocalName, false));

        Tree testTree = root.getTree("/").addChild("test");
        List<String> expected = IntStream.range(0, 3).mapToObj(i -> {
            String nodeName = "ja" + i; //'j*' should come after (asc) 'index' in sort order
            testTree.addChild(nodeName);
            return "/test/" + nodeName;
        }).sorted((s1, s2) -> { //sort expectation based on local name
            final StringBuffer sb1 = new StringBuffer();
            PathUtils.elements(s1).forEach(elem -> {
                String[] split = elem.split(":", 2);
                sb1.append(split[split.length - 1]);
            });
            s1 = sb1.toString();

            final StringBuffer sb2 = new StringBuffer();
            PathUtils.elements(s2).forEach(elem -> {
                String[] split = elem.split(":", 2);
                sb2.append(split[split.length - 1]);
            });
            s2 = sb2.toString();

            return s1.compareTo(s2);
        }).collect(Collectors.toList());
        root.commit();

        String query = "/jcr:root/test/* order by fn:local-name() option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() ascending option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() descending option(index tag fnLocalName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":fnLocalName(/oak:index/fnLocalName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));

        // order by fn:name() although function index is on "name()"
        query = "/jcr:root/test/* order by fn:local-name() option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() ascending option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(expected, executeQuery(query, XPATH));

        query = "/jcr:root/test/* order by fn:local-name() descending option(index tag localName)";
        assertXpathPlan(query, indexOptions.getIndexType() + ":localName(/oak:index/localName)");
        assertEquals(Lists.reverse(expected), executeQuery(query, XPATH));
    }

    @Test
    public void orderByScoreAcrossUnion() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder()
                .evaluatePathRestrictions().noAsync();
        builder.indexRule("nt:base")
                .property("foo").analyzed().propertyIndex().nodeScopeIndex()
                .property("p1").propertyIndex()
                .property("p2").propertyIndex();
        indexOptions.setIndex(root, "test-index", indexOptions.createIndex(builder, false));

        Tree testRoot = root.getTree("/").addChild("test");
        Tree path1 = testRoot.addChild("path1");
        Tree path2 = testRoot.addChild("path2");

        Tree c1 = path1.addChild("c1");
        c1.setProperty("foo", "bar. some extra stuff");
        c1.setProperty("p1", "d");

        Tree c2 = path2.addChild("c2");
        c2.setProperty("foo", "bar");
        c2.setProperty("p2", "d");

        Tree c3 = path2.addChild("c3");
        c3.setProperty("foo", "bar. some extra stuff... and then some to make it worse than c1");
        c3.setProperty("p2", "d");

        // create more stuff to get num_docs in index large and hence force union optimization
        for (int i = 0; i < 10; i++) {
            testRoot.addChild("extra" + i).setProperty("foo", "stuff");
        }

        root.commit();

        List<String> expected = asList(c2.getPath(), c1.getPath(), c3.getPath());

        // manual union
        String query =
                "select [jcr:path] from [nt:base] where contains(*, 'bar') and isdescendantnode('" + path1.getPath() + "')" +
                        " union " +
                        "select [jcr:path] from [nt:base] where contains(*, 'bar') and isdescendantnode('" + path2.getPath() + "')" +
                        " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));

        // no union (estimated fulltext without union would be same as sub-queries and it won't be optimized
        query = "select [jcr:path] from [nt:base] where contains(*, 'bar')" +
                " and (isdescendantnode('" + path1.getPath() + "') or" +
                " isdescendantnode('" + path2.getPath() + "'))" +
                " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));

        // optimization UNION as we're adding constraints to sub-queries that would improve cost of optimized union
        query = "select [jcr:path] from [nt:base] where contains(*, 'bar')" +
                " and ( (p1 = 'd' and isdescendantnode('" + path1.getPath() + "')) or" +
                " (p2 = 'd' and isdescendantnode('" + path2.getPath() + "')))" +
                " order by [jcr:score] desc";

        assertEquals(expected, executeQuery(query, SQL2));
    }

    @Test
    public void sortQueriesWithLong() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder().noAsync();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        indexRule.property("foo").propertyIndex().type(PropertyType.TYPENAME_LONG);
        indexRule.property("bar").propertyIndex();
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(builder, false));
        root.commit();

        assertSortedLong();
    }

    @Test
    public void sortQueriesWithLong_NotIndexed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder().noAsync();
        builder.getBuilderTree().setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));
        builder.getBuilderTree().setProperty(createProperty(ORDERED_PROP_NAMES, of("foo"), STRINGS));
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        indexRule.property("foo").propertyIndex().type(PropertyType.TYPENAME_LONG);
        indexOptions.setIndex(root, "test1", indexOptions.createIndex(builder, false));
        root.commit();

        assertThat(explain("select [jcr:path] from [nt:base] order by [jcr:score], [foo]"),
                containsString(indexOptions.getIndexType() + ":test1"));
        assertThat(explain("select [jcr:path] from [nt:base] order by [foo]"),
                containsString(indexOptions.getIndexType() + ":test1"));

        List<Tuple> tuples = createDataForLongProp();
        assertOrderedQuery("select [jcr:path] from [nt:base] order by [foo]", getSortedPaths(tuples, OrderedIndex.OrderDirection.ASC));
        assertOrderedQuery("select [jcr:path] from [nt:base] order by [foo] DESC", getSortedPaths(tuples, OrderedIndex.OrderDirection.DESC));
    }

    protected void assertSortedLong() throws CommitFailedException {
        List<Tuple> tuples = createDataForLongProp();
        assertEventually(() -> {
            assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo]",
                    getSortedPaths(tuples, OrderedIndex.OrderDirection.ASC));
            assertOrderedQuery("select [jcr:path] from [nt:base] where [bar] = 'baz' order by [foo] DESC",
                    getSortedPaths(tuples, OrderedIndex.OrderDirection.DESC));
        });
    }

    private List<Tuple> createDataForLongProp() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        List<Long> values = createLongs(100);
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

    static List<Long> createLongs(int n){
        List<Long> values = Lists.newArrayListWithCapacity(n);
        for (long i = 0; i < n; i++){
            values.add(i);
        }
        Collections.shuffle(values);
        return values;
    }

    private void assertOrderedQuery(String sql, List<String> paths) {
        assertOrderedQuery(sql, paths, SQL2, false);
    }

    private void assertOrderedQuery(String sql, List<String> paths, String language, boolean skipSort) {
        List<String> result = executeQuery(sql, language, true, skipSort);
        assertEquals(paths, result);
    }

    private void assertXpathPlan(String query, String planExpectation) throws ParseException {
        assertThat(explainXpath(query), containsString(planExpectation));
    }

    private String explainXpath(String query) throws ParseException {
        String explain = "explain " + query;
        Result result = executeQuery(explain, "xpath", NO_BINDINGS);
        ResultRow row = Iterables.getOnlyElement(result.getRows());
        return row.getValue("plan").getValue(Type.STRING);
    }

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private static List<String> getSortedPaths(List<Tuple> tuples, OrderedIndex.OrderDirection dir) {
        if (OrderedIndex.OrderDirection.DESC == dir) {
            tuples.sort(Collections.reverseOrder());
        } else {
            Collections.sort(tuples);
        }
        List<String> paths = Lists.newArrayListWithCapacity(tuples.size());
        for (Tuple t : tuples) {
            paths.add(t.path);
        }
        return paths;
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
}
