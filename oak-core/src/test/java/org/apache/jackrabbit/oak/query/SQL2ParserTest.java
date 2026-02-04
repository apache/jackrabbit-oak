/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.text.ParseException;
import java.util.List;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the SQL-2 parser.
 */
public class SQL2ParserTest {

    private static final NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(INITIAL_CONTENT);

    private static final SQL2Parser p = createTestSQL2Parser();

    public static SQL2Parser createTestSQL2Parser() {
        return createTestSQL2Parser(NamePathMapper.DEFAULT, nodeTypes);
    }

    public static SQL2Parser createTestSQL2Parser(NamePathMapper mappings, NodeTypeInfoProvider nodeTypes2) {
        QueryStatsData data = new QueryStatsData("", "");
        return new SQL2Parser(mappings, nodeTypes2, new QueryEngineSettings(), data.new QueryExecutionStats());
    }


    @Test
    public void testIgnoreSqlComment() throws ParseException {
        p.parse("select * from [nt:unstructured] /* sql comment */");
        p.parse("select [jcr:path], [jcr:score], * from [nt:base] as a /* xpath: //* */");
        p.parse("/* begin query */ select [jcr:path] /* this is the path */, " +
                "[jcr:score] /* the score */, * /* everything*/ " +
                "from [nt:base] /* all node types */ as a /* an identifier */");
    }

    @Test(expected = ParseException.class)
    public void testUnfinishedSqlComment() throws ParseException {
        p.parse("select [jcr:path], [jcr:score], * from [nt:base] as a /* xpath: //* ");
    }

    @Test
    public void testTransformAndParse() throws ParseException {
        p.parse(new XPathToSQL2Converter()
                .convert("/jcr:root/test/*/nt:resource[@jcr:encoding]"));
        p.parse(new XPathToSQL2Converter()
                .convert("/jcr:root/test/*/*/nt:resource[@jcr:encoding]"));
        String xpath = "/jcr:root/etc/commerce/products//*[@cq:commerceType = 'product' " +
                "and ((@size = 'M' or */@size= 'M' or */*/@size = 'M' " +
                "or */*/*/@size = 'M' or */*/*/*/@size = 'M' or */*/*/*/*/@size = 'M'))]";
        p.parse(new XPathToSQL2Converter()
                .convert(xpath));
    }

    // see OAK-OAK-830: XPathToSQL2Converter fails to wrap or clauses
    @Test
    public void testUnwrappedOr() throws ParseException {
        String q = new XPathToSQL2Converter()
                .convert("/jcr:root/home//test/* [@type='t1' or @type='t2' or @type='t3']");
        String token = "and b.[type] in('t1', 't2', 't3')";
        assertTrue(q.contains(token));
    }

    /*
     * When a query with LIMIT option, it should still select the index with the least entries
     * as those might require being traversed during post-filtering.
     *
     * See OAK-12057
     */
    @Test
    public void testPlanningWithLimit() throws ParseException {
        // Given
        var query = "SELECT * \n" +
                "FROM [nt:base] AS s \n" +
                "WHERE ISDESCENDANTNODE(s, '/content') AND s.[j:c]='/conf/wknd'\n" +
                "OPTION (LIMIT 2)";

        // - the first available option
        var indexA = mock(DummyQueryIndex.class);
        var planA = mock(QueryIndex.IndexPlan.class);
        given(indexA.getPlans(any(), any(), any())).willReturn(List.of(planA));
        given(planA.getPlanName()).willReturn("planA");
        given(planA.getEstimatedEntryCount()).willReturn(10000L);   // more entries
        given(planA.getCostPerEntry()).willReturn(1.0);
        given(planA.getCostPerExecution()).willReturn(100.0);

        // - the better option
        var indexB = mock(DummyQueryIndex.class);
        var planB = mock(QueryIndex.IndexPlan.class);
        given(indexB.getPlans(any(), any(), any())).willReturn(List.of(planB));
        given(planB.getPlanName()).willReturn("planB");
        given(planB.getEstimatedEntryCount()).willReturn(100L);     // less entries
        given(planB.getCostPerEntry()).willReturn(1.0);
        given(planB.getCostPerExecution()).willReturn(100.0);
        given(indexB.getPlanDescription(eq(planB), any())).willReturn("planB");

        var indexProvider = new QueryIndexProvider() {
            @Override
            public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
                return List.of(indexA, indexB);
            }
        };

        var context = mock(ExecutionContext.class);
        given(context.getIndexProvider()).willReturn(indexProvider);

        // When
        var parsedQuery = p.parse(query,false);
        parsedQuery.init();
        parsedQuery.setExecutionContext(context);
        parsedQuery.setTraversalEnabled(false);
        parsedQuery.prepare();

        // Then
        assertEquals("[nt:base] as [s] /* planB */", parsedQuery.getPlan());
    }

    @Test
    public void testCoalesce() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE([j:c/m/d:t], [j:c/j:t])='a'");

        p.parse("SELECT * FROM [nt:base] WHERE COALESCE(COALESCE([j:c/m/d:t], name()), [j:c/j:t])='a'");

        p.parse("SELECT * FROM [nt:base] WHERE COALESCE(COALESCE([j:c/m/d:t], name()), [j:c/j:t]) in ('a', 'b')");

        p.parse("SELECT * FROM [nt:base] WHERE COALESCE(COALESCE([j:c/a], [b]), COALESCE([c], [c:d])) = 'a'");

        p.parse(new XPathToSQL2Converter()
                .convert("//*[fn:coalesce(j:c/m/@d:t, j:c/@j:t) = 'a']"));

        p.parse(new XPathToSQL2Converter()
                .convert("//*[fn:coalesce(fn:coalesce(j:c/m/@d:t, fn:name()), j:c/@j:t) = 'a']"));

        p.parse(new XPathToSQL2Converter()
                .convert("//*[fn:coalesce(fn:coalesce(j:c/@a, b), fn:coalesce(c, c:d)) = 'a']"));
    }

    @Test
    public void testFirst() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE FIRST([d:t])='a'");

        p.parse("SELECT * FROM [nt:base] WHERE FIRST([jcr:mixinTypes])='a'");
    }

    @Test
    public void testLimitOffSet() throws ParseException {
        QueryImpl parsed = (QueryImpl) p.parse("SELECT * FROM [nt:base] WHERE b=2 OPTION(OFFSET 10, LIMIT 100)");
        assertEquals(10L, parsed.getOffset().get().longValue());
        assertEquals(100L, parsed.getLimit().get().longValue());

        QueryImpl parsedXPath = (QueryImpl) p.parse(new XPathToSQL2Converter()
                .convert("/jcr:root/test/*/nt:resource[@jcr:encoding] option(offset 2, limit 75)"));
        assertEquals(2L, parsedXPath.getOffset().get().longValue());
        assertEquals(75L, parsedXPath.getLimit().get().longValue());

        QueryImpl parsedLimitOnly = (QueryImpl) p.parse(new XPathToSQL2Converter()
                .convert("/jcr:root/test/*/nt:resource[@jcr:encoding] option(limit 98)"));
        assertFalse(parsedLimitOnly.getOffset().isPresent());
        assertEquals(98L, parsedLimitOnly.getLimit().get().longValue());

        QueryImpl parsedOffsetOnly = (QueryImpl) p.parse("SELECT * FROM [nt:base] WHERE b=2 OPTION(OFFSET 23)");
        assertEquals(23L, parsedOffsetOnly.getOffset().get().longValue());
        assertFalse(parsedOffsetOnly.getLimit().isPresent());
    }

    @Test(expected = ParseException.class)
    public void testOffsetNotDouble() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE b=2 OPTION(OFFSET 10.0, LIMIT 100)");
    }

    @Test(expected = ParseException.class)
    public void testLimitNotDouble() throws ParseException {
        p.parse(new XPathToSQL2Converter()
                .convert("/jcr:root/test/*/nt:resource[@jcr:encoding] option(offset 2, limit 75.0)"));
    }

    @Test(expected = ParseException.class)
    public void coalesceFailsWithNoParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE()='a'");
    }

    @Test(expected = ParseException.class)
    public void firstFailsWithNoParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE FIRST()='a'");
    }

    @Test(expected = ParseException.class)
    public void coalesceFailsWithOneParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE([a])='a'");
    }

    @Test(expected = ParseException.class)
    public void coalesceFailsWithMoreThanTwoParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE([a], [b], [c])='a'");
    }

    @Ignore("OAK-7131")
    @Test
    public void orderingWithUnionOfNodetype() throws Exception {
        XPathToSQL2Converter c = new XPathToSQL2Converter();

        String xpath;

        xpath = "//(element(*, type1) | element(*, type2)) order by @foo";
        assertTrue("Converted xpath " + xpath + "doesn't end with 'order by [foo]'", c.convert(xpath).endsWith("order by [foo]"));

        xpath = "//(element(*, type1) | element(*, type2) | element(*, type3)) order by @foo";
        assertTrue("Converted xpath " + xpath + "doesn't end with 'order by [foo]'", c.convert(xpath).endsWith("order by [foo]"));

        xpath = "//(element(*, type1) | element(*, type2) | element(*, type3) | element(*, type4)) order by @foo";
        assertTrue("Converted xpath " + xpath + "doesn't end with 'order by [foo]'", c.convert(xpath).endsWith("order by [foo]"));

        xpath = "//(element(*, type1) | element(*, type2))[@a='b'] order by @foo";
        assertTrue("Converted xpath " + xpath + "doesn't end with 'order by [foo]'", c.convert(xpath).endsWith("order by [foo]"));

        xpath = "//(element(*, type1) | element(*, type2))[@a='b' or @c='d'] order by @foo";
        assertTrue("Converted xpath " + xpath + "doesn't end with 'order by [foo]'", c.convert(xpath).endsWith("order by [foo]"));
    }

    interface DummyQueryIndex extends QueryIndex, QueryIndex.AdvancedQueryIndex {
        /*
         * This is needed as AdvancedQueryIndex is what we what to mock but the QueryIndexProvider
         * returns QueryIndex instances and, in reality implementation of AdvancedQueryIndex also implement
         * QueryIndex, but AdvancedQueryIndex does not explicitly extend QueryIndex.
         *
         * Making this link explicit would break the spi versioning.
         */
    }
}
