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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test filter conditions.
 */
public class FilterTest {
    
    private final SQL2Parser p = SQL2ParserTest.createTestSQL2Parser();

    private Filter createFilter(String xpath) throws ParseException {
        String sql = new XPathToSQL2Converter().convert(xpath);
        QueryImpl q = (QueryImpl) p.parse(sql);
        return q.createFilter(true);
    }
    
    private Filter createFilterSQL(String sql) throws ParseException {
        QueryImpl q = (QueryImpl) p.parse(sql);
        return q.createFilter(true);
    }
    
    @Test
    public void functionBasedIndex() throws Exception {
        String sql2 = "select [jcr:path] from [nt:base] where lower([test]) = 'hello'";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where lower([test]) = 'hello', " + 
                "path=*, property=[" + 
                "function*lower*@test=[hello], " +
                "test=[is not null]])", createFilterSQL(sql2).toString());
        
        sql2 = "select [jcr:path] from [nt:base] where upper([test]) = 'HELLO'";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where upper([test]) = 'HELLO', " + 
                "path=*, property=[" + 
                "function*upper*@test=[HELLO], " +
                "test=[is not null]])", createFilterSQL(sql2).toString());
        
        sql2 = "select [jcr:path] from [nt:base] where upper(name()) = 'ACME:TEST'";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where upper(name()) = 'ACME:TEST', " + 
                "path=*, property=[" + 
                "function*upper*@:name=[ACME:TEST]])", createFilterSQL(sql2).toString());
        
        sql2 = "select [jcr:path] from [nt:base] where lower(localname()) > 'test'";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where lower(localname()) > 'test', " + 
                "path=*, property=[" + 
                "function*lower*@:localname=[(test..]])", createFilterSQL(sql2).toString());

        sql2 = "select [jcr:path] from [nt:base] where length([test]) <= 10";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where length([test]) <= 10, " + 
                "path=*, property=[function*length*@test=[..10]], " + 
                "test=[is not null]])", createFilterSQL(sql2).toString());
        
        sql2 = "select [jcr:path] from [nt:base] where length([data/test]) > 2";
        assertEquals("Filter(query=select [jcr:path] from [nt:base] " + 
                "where length([data/test]) > 2, " + 
                "path=*, property=[data/test=[is not null], " + 
                "function*length*@data/test=[(2..]])", createFilterSQL(sql2).toString());
    }
    
    @Test
    public void oak4170() throws ParseException {
        String sql2 = "select * from [nt:unstructured] where CONTAINS([jcr:content/metadata/comment], 'december')";
        Filter f = createFilterSQL(sql2);
        String plan = f.toString();
        // with the "property is not null" restriction, it would be:
        // assertEquals("Filter(query=select * from [nt:unstructured] " + 
        //         "where CONTAINS([jcr:content/metadata/comment], 'december') " + 
        //         "fullText=jcr:content/metadata/comment:\"december\", " + 
        //         "path=*, property=[jcr:content/metadata/comment=[is not null]])", plan);
        assertEquals("Filter(query=select * from [nt:unstructured] " + 
                "where CONTAINS([jcr:content/metadata/comment], 'december') " + 
                "fullText=jcr:content/metadata/comment:\"december\", " + 
                "path=*)", plan);
        assertEquals(f.getPropertyRestrictions().toString(), 0, f.getPropertyRestrictions().size());
        f.getPropertyRestriction("jcr:content/metadata/comment");
    }
    
    @Test
    public void localName() throws Exception {
        Filter f = createFilterSQL("select * from [nt:base] where localname() = 'resource'");
        assertEquals("[resource]", f.getPropertyRestrictions(":localname").toString());
    }
    
    @Test
    public void name() throws Exception {
        Filter f = createFilter("//*[fn:name() = 'nt:resource']");
        assertEquals("[resource]", f.getPropertyRestrictions(":localname").toString());
    }

    @Test
    public void mvp() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[(@prop = 'aaa' and @prop = 'bbb' and @prop = 'ccc')]");
        assertFalse(f.isAlwaysFalse());
    }
    
    @Test
    public void isNull() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[not(@c)]");
        assertEquals("[is null]", f.getPropertyRestrictions("c").toString());
    }

    @Test
    public void isNotNull() throws Exception {
        // this can refer to a multi-valued property
        Filter f = createFilter("//*[@c]");
        assertEquals("[is not null]", f.getPropertyRestrictions("c").toString());
    }

    @Ignore("OAK-4170")
    @Test
    public void fulltext() throws Exception {
        Filter f = createFilterSQL("select * from [nt:unstructured] where CONTAINS([jcr:content/metadata/comment], 'december')");
        assertNotNull(f.getPropertyRestriction("jcr:content/metadata/comment"));
    }
}
