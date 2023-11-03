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
import static org.junit.Assert.assertTrue;

import java.util.Random;

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
    public void formatRandomized() {
        Random r = new Random(1);
        for (int i = 0; i < 100000; i++) {
            int len = r.nextInt(30);
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < len; j++) {
                switch (r.nextInt(15)) {
                case 0:
                    buff.append('\'');
                    break;
                case 1:
                    buff.append('\"');
                    break;
                case 2:
                    buff.append('\n');
                    break;
                case 3:
                    buff.append("or");
                    break;
                case 4:
                    buff.append("and");
                    break;
                case 5:
                    buff.append("from");
                    break;
                case 6:
                    buff.append("order by");
                    break;
                case 7:
                    buff.append("option");
                    break;
                case 8:
                    buff.append('(');
                    break;
                case 9:
                    buff.append('[');
                    break;
                default:
                    buff.append(' ');
                    break;
                }
            }
            String query = buff.toString();
            QueryFormatter.format(query, "xpath");
            QueryFormatter.format(query, "sql");
        }
    }

    @Test
    public void format() {
        assertEquals("/jcr:root//*[\n"
                + "  @a=1\n"
                + "  and @b=2\n"
                + "  or @c=3]\n"
                + "  order by @d\n"
                + "  option(traversal ok)",
                QueryFormatter.format(
                        "/jcr:root//*[@a=1 and @b=2 or @c=3] order by @d option(traversal ok)", null));
        assertEquals(
                "sElEct *\n"
                + "  FROM nt:base\n"
                + "  WHERE x=1\n"
                + "  and y=2\n"
                + "  Or z=3",
                QueryFormatter.format(
                        "sElEct * FROM nt:base WHERE x=1 and y=2 Or z=3",
                        null));
        assertEquals(
                "select ...\n"
                + "  union select ...\n"
                + "  order by '...",
                QueryFormatter.format(
                        "select ... union select ... order by '...",
                        null));
        assertEquals(
                "select ' from  '' union '\n"
                + "  from ...\n"
                + "  option(...)",
                QueryFormatter.format(
                        "select ' from  '' union ' from ... option(...)",
                        null));
        assertEquals(
                "select \" from  \"\" union \"\n"
                + "  from ...\n"
                + "  option(...)",
                QueryFormatter.format(
                        "select \" from  \"\" union \" from ... option(...)",
                        null));
        assertEquals(
                "/jcr:root//*[\n"
                + "  @x=' and '' and '\n"
                + "  or @y=\" or \"]\n"
                + "  order by @z",
                QueryFormatter.format("/jcr:root//*[@x=' and '' and ' or @y=\" or \"] order by @z", null));
        assertEquals(
                "/jcr:root//*[",
                QueryFormatter.format("/jcr:root//*[", null));
        assertEquals(
                "/jcr:root//*[\n"
                + "  @a='",
                QueryFormatter.format("/jcr:root//*[@a='", null));
    }

    @Test
    public void alreadyFormatted() {
        assertEquals("/jcr:root//*[\n"
                + "  @a=1\n"
                + "  and @b=2\n"
                + "  or @c=3]\n"
                + "  order by @d\n"
                + "  option(traversal ok)",
                QueryFormatter.format(
                "/jcr:root//*[\n"
                + "  @a=1\n"
                + "  and @b=2\n"
                + "  or @c=3]\n"
                + "  order by @d\n"
                + "  option(traversal ok)", null));
        assertEquals(
                "select \" from  \"\" union \"\n"
                + "  from ...\n"
                + "  option(...)",
                QueryFormatter.format(
                "select \" from  \"\" union \"\n"
                        + "  from ...\n"
                        + "  option(...)",
                null));
    }
}
