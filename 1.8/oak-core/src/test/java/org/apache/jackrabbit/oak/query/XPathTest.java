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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.junit.Test;

/**
 * Tests the XPathToSQL2Converter
 */
public class XPathTest {
    
    private final SQL2Parser parser = SQL2ParserTest.createTestSQL2Parser();
    
    @Test
    public void complexQuery() throws ParseException {
        for(int n = 1; n < 15; n++) {
            for(int m = 1; m < 15; m++) {
                complexQuery(n, m);
            }            
        }
    }
    
    private void complexQuery(int n, int m) throws ParseException {
        StringBuilder buff = new StringBuilder();
        buff.append("/jcr:root//*[");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                buff.append("and ");
            }
            buff.append("(");
            for (int j = 0; j < m; j++) {
                if (j > 0) {
                    buff.append("or ");
                }
                buff.append("@x" + j + " = " + j);
            }
            buff.append(")\n");
        }
        buff.append("]");
        String xpath = buff.toString();
        String sql2 = new XPathToSQL2Converter().convert(xpath);
        assertTrue("Length: " + sql2.length(), sql2.length() < 200000);
        Query q = parser.parse(sql2, false);
        q.buildAlternativeQuery();
    }
    
    @Test
    public void queryOptions() throws ParseException {
        verify("(/jcr:root/a//* | /jcr:root/b//*) order by @jcr:score", 
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/a') " +
                "/* xpath: /jcr:root/a//* \n" +
                "order by @jcr:score */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/b') " +
                "/* xpath: /jcr:root/b//* " +
                "order by @jcr:score */ " +
                "order by [jcr:score]");         
        verify("(/jcr:root/a//* | /jcr:root/b//* | /jcr:root/c//*) order by @jcr:score", 
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/a') " +
                "/* xpath: /jcr:root/a//* \n" +
                "order by @jcr:score */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/b') " +
                "/* xpath: /jcr:root/b//* \n" +
                "order by @jcr:score */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/c') " +
                "/* xpath: /jcr:root/c//* " +
                "order by @jcr:score */ " +
                "order by [jcr:score]"); 
        verify("//(element(*, nt:address))", 
                "select [jcr:path], [jcr:score], * " +
                "from [nt:address] as a " +
                "/* xpath: //element(*, nt:address) */"); 
        verify("//(element(*, nt:address) | element(*, nt:folder))", 
                "select [jcr:path], [jcr:score], * " +
                "from [nt:address] as a " +
                "/* xpath: //element(*, nt:address) */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:folder] as a " +
                "/* xpath: // element(*, nt:folder) */");
        verify("(//element(*, nt:address) | //element(*, nt:folder))", 
                "select [jcr:path], [jcr:score], * " +
                "from [nt:address] as a " +
                "/* xpath: //element(*, nt:address) */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:folder] as a " +
                "/* xpath: //element(*, nt:folder) */");
        verify("/jcr:root/content//*[@a] order by @c option(traversal fail)",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and isdescendantnode(a, '/content') " +
                "order by [c] option(traversal FAIL) " +
                "/* xpath: /jcr:root/content//*[@a] " +
                "order by @c " + 
                "option(traversal fail) */");            
        verify("//*[@a or @b] order by @c option(traversal warn)",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [b] is not null " +
                "order by [c] option(traversal WARN) " +
                "/* xpath: //*[@a or @b] " + 
                "order by @c " + 
                "option(traversal warn) */");
        verify("/jcr:root/(content|libs)//*[@a] order by @c option(traversal ok)",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and isdescendantnode(a, '/content') " +
                "/* xpath: /jcr:root/content//*[@a] " +
                "order by @c option(traversal ok) */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and isdescendantnode(a, '/libs') " +
                "/* xpath: /jcr:root/libs//*[@a] " +
                "order by @c option(traversal ok) */ " +
                "order by [c] " + 
                "option(traversal OK)");            
    }

    @Test
    public void chainedConditions() throws ParseException {
        verify("/jcr:root/x[@a][@b][@c]",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and [b] is not null " +
                "and [c] is not null " +
                "and issamenode(a, '/x') " +
                "/* xpath: /jcr:root/x[@a][@b][@c] */");
    }
    
    @Test
    public void union() throws ParseException {
        verify("(//*[@a=1 or @b=1] | //*[@c=1])",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] = 1 " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [b] = 1 " +
                "/* xpath: //*[@a=1 or @b=1] */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [c] = 1 " +
                "/* xpath: //*[@c=1] */");
        verify("//(a|(b|c))",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where name(a) = 'a' " +
                "/* xpath: //a */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where name(a) = 'b' " +
                "/* xpath: //b */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where name(a) = 'c' " +
                "/* xpath: //c */");
        verify("(//*[jcr:contains(., 'some')])",
                "select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where contains(*, 'some') " + 
                "/* xpath: //*[jcr:contains(., 'some')] */");
        verify("(//*[jcr:contains(., 'x')] | //*[jcr:contains(., 'y')])",
                "select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where contains(*, 'x') " + 
                "/* xpath: //*[jcr:contains(., 'x')] */ " + 
                "union select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where contains(*, 'y') " + 
                "/* xpath: //*[jcr:contains(., 'y')] */");
        try {
            verify("(/jcr:root/x[@a][@b][@c]","");
            fail();
        } catch (ParseException e) {
            // expected
        }
        verify("(/jcr:root/x[@a] | /jcr:root/y[@b])[@c]",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and [c] is not null " +
                "and issamenode(a, '/x') " +
                "/* xpath: /jcr:root/x[@a] [@c] */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [b] is not null " +
                "and [c] is not null " +
                "and issamenode(a, '/y') " +
                "/* xpath: /jcr:root/y[@b][@c] */");
        verify("(/jcr:root/x | /jcr:root/y)",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/x') " +
                "/* xpath: /jcr:root/x */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/y') " +
                "/* xpath: /jcr:root/y */");
        verify("(/jcr:root/x | /jcr:root/y )",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/x') " +
                "/* xpath: /jcr:root/x */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/y') " +
                "/* xpath: /jcr:root/y */");
        verify("(/jcr:root/content//*[@a] | /jcr:root/lib//*[@b]) order by @c",
                "select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where [a] is not null " + 
                "and isdescendantnode(a, '/content') " + 
                "/* xpath: /jcr:root/content//*[@a]  " + 
                "order by @c */ " + 
                "union select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where [b] is not null " + 
                "and isdescendantnode(a, '/lib') " + 
                "/* xpath: /jcr:root/lib//*[@b] " + 
                "order by @c */ " + 
                "order by [c]");       
        verify("/jcr:root/(content|lib)/element(*, nt:base) order by @title",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where ischildnode(a, '/content') " +
                "/* xpath: /jcr:root/content/element(*, nt:base) " +
                "order by @title */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where ischildnode(a, '/lib') " +
                "/* xpath: /jcr:root/lib/element(*, nt:base) " +
                "order by @title */ " +
                "order by [title]");        
        verify("/jcr:root/(content|lib)",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/content') " +
                "/* xpath: /jcr:root/content */ " + 
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where issamenode(a, '/lib') " +
                "/* xpath: /jcr:root/lib */");
        verify("(/jcr:root/content|/jcr:root/lib)//*",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/content') " +
                "/* xpath: /jcr:root/content//* */ " + 
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where isdescendantnode(a, '/lib') " +
                "/* xpath: /jcr:root/lib//* */");
        verify("/jcr:root/content/(a|b|c)/thumbnails/*",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where ischildnode(a, '/content/a/thumbnails') " +
                "/* xpath: /jcr:root/content/a/thumbnails/* */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where ischildnode(a, '/content/b/thumbnails') " +
                "/* xpath: /jcr:root/content/b/thumbnails/* */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where ischildnode(a, '/content/c/thumbnails') " +
                "/* xpath: /jcr:root/content/c/thumbnails/* */");        
        verify("/jcr:root/(content|lib)//*[@a]",
                "select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and isdescendantnode(a, '/content') " +
                "/* xpath: /jcr:root/content//*[@a] */ " +
                "union select [jcr:path], [jcr:score], * " +
                "from [nt:base] as a " +
                "where [a] is not null " +
                "and isdescendantnode(a, '/lib') " +
                "/* xpath: /jcr:root/lib//*[@a] */");        
    }

    private void verify(String xpath, String expectedSql2) throws ParseException {
        String sql2 = new XPathToSQL2Converter().convert(xpath);
        sql2 = formatSQL(sql2);
        expectedSql2 = formatSQL(expectedSql2);
        assertEquals(expectedSql2, sql2);
        parser.parse(sql2);
    }
    
    static String formatSQL(String sql) {
        sql = sql.replace('\n', ' ');
        sql = sql.replaceAll(" from ", "\nfrom ");
        sql = sql.replaceAll(" where ", "\nwhere ");
        sql = sql.replaceAll(" and ", "\nand ");
        sql = sql.replaceAll(" union ", "\nunion ");
        sql = sql.replaceAll(" order by ", "\norder by ");
        sql = sql.replaceAll(" option\\(", "\noption\\(");
        sql = sql.replaceAll(" \\/\\* ", "\n/* ");
        return sql;
    }

}
