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

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.junit.Test;

/**
 * Tests the SQL-2 parser.
 */
public class SQL2ParserTest {

    private static final NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(INITIAL_CONTENT);

    private static final SQL2Parser p = createTestSQL2Parser();
    
    public static SQL2Parser createTestSQL2Parser() {
        return createTestSQL2Parser(NamePathMapper.DEFAULT, nodeTypes, new QueryEngineSettings());
    }
    
    public static SQL2Parser createTestSQL2Parser(NamePathMapper mappings, NodeTypeInfoProvider nodeTypes2,
            QueryEngineSettings qeSettings) {
        QueryStatsData data = new QueryStatsData("", "");
        return new SQL2Parser(mappings, nodeTypes2, new QueryEngineSettings(), 
                data.new QueryExecutionStats());
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

    @Test(expected = ParseException.class)
    public void coalesceFailsWithNoParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE()='a'");
    }

    @Test(expected = ParseException.class)
    public void coalesceFailsWithOneParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE([a])='a'");
    }

    @Test(expected = ParseException.class)
    public void coalesceFailsWithMoreThanTwoParam() throws ParseException {
        p.parse("SELECT * FROM [nt:base] WHERE COALESCE([a], [b], [c])='a'");
    }

}
