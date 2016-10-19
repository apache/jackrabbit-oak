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

import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.junit.Test;

/**
 * Tests the SQL-2 parser.
 */
public class SQL2ParserTest {

    private final NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(INITIAL_CONTENT);

    private final SQL2Parser p = new SQL2Parser(null, nodeTypes, new QueryEngineSettings());

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
    
}
