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
package org.apache.jackrabbit.oak.jcr.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.query.QueryFormatter;
import org.junit.Test;

public class QueryFormatterTest {

    @Test
    public void detectLanguage() {
        assertFalse(QueryFormatter.isXPath("SELECT * FROM [rep:Authorizable]", null));
        assertFalse(QueryFormatter.isXPath("  select * from [nt:base]", null));
        assertFalse(QueryFormatter.isXPath("EXPLAIN SELECT ...", null));
        assertFalse(QueryFormatter.isXPath("explain measure  SELECT ...", null));
        
        // common xpath
        assertTrue(QueryFormatter.isXPath("/jcr:root//*", null));
        assertTrue(QueryFormatter.isXPath(" /jcr:root//*", null));
        assertTrue(QueryFormatter.isXPath("\nexplain  /jcr:root//element(*,rep:ACE)", null));
        
        // xpath union
        assertTrue(QueryFormatter.isXPath("( ( /jcr:root//a | /jcr:root//b ) )", null));
        
        // language is set explicitly
        assertTrue(QueryFormatter.isXPath("select", "xpath"));
    }
    
    @Test
    public void format() {
        assertEquals("/jcr:root//*[\n"
                + "  @a=1\n"
                + "  and @b=2]\n"
                + "  order by @c\n"
                + "  option(traversal ok)", 
                QueryFormatter.format(
                        "/jcr:root//*[@a=1 and @b=2] order by @c option(traversal ok)", null));
        assertEquals(
                "sElEct *\n"
                + "  FROM nt:base\n"
                + "  WHERE x=1\n"
                + "  and y=2",
                QueryFormatter.format(
                        "sElEct * FROM nt:base WHERE x=1 and y=2",
                        null));
        assertEquals(
                "select ...\n"
                + "  union select ...",
                QueryFormatter.format(
                        "select ... union select ...",
                        null));
        // formatting is also done inside string literals
        assertEquals(
                "/jcr:root//*[\n"
                + "  @x='\n"
                + "  and ']",
                QueryFormatter.format("/jcr:root//*[@x=' and ']", null));
    }
}
