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
import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.junit.Test;

/**
 * Tests the XPathToSQL2Converter
 */
public class XPathTest {
    
    private final NodeTypeInfoProvider nodeTypes =
            new NodeStateNodeTypeInfoProvider(INITIAL_CONTENT);
    
    @Test
    public void queryOptions() throws ParseException {
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
    public void test() throws ParseException {
        verify("(/jcr:root/content//*[@a] | /jcr:root/lib//*[@b]) order by @c",
                "select [jcr:path], [jcr:score], * " + 
                "from [nt:base] as a " + 
                "where [a] is not null " + 
                "and isdescendantnode(a, '/content') " + 
                "/* xpath: /jcr:root/content//*[@a] " + 
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
        SQL2Parser p = new SQL2Parser(null, nodeTypes, new QueryEngineSettings());
        p.parse(sql2);
    }
    
    static String formatSQL(String sql) {
        sql = sql.replace("\n", " ");
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
