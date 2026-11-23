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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;

import org.apache.jackrabbit.oak.query.regex.BudgetedCharSequence;
import org.apache.jackrabbit.oak.query.regex.RegexBudget;
import org.apache.jackrabbit.oak.query.regex.RegexBudgetExceededException;
import org.junit.Test;

public class QueryTemplateTest {

    @Test
    public void quotedStringsAndLiteralReplacement() {
        QueryTemplate template = new QueryTemplate("contains(., '#1')", "contains(@title, '#1') $value $1 \\");
        for (String value : new String[] {"", "hello", "John''s car", "a\nb", "$2\\value", "\"quoted\"", "#1"}) {
            assertEquals("contains(@title, '" + value + "') $value $1 \\",
                    rewrite(template, "contains(., '" + value + "')"));
        }
        assertFalse(template.find.matcher("contains(., 'unescaped'quote')").matches());
        assertFalse(template.find.matcher("contains(., 'unterminated)").matches());
    }

    @Test
    public void doubleQuotedStrings() {
        QueryTemplate template = new QueryTemplate("@title=\"#2\"", "@name=\"#2\"");
        for (String value : new String[] {"", "hello", "say \"\"hello\"\"", "John's car"}) {
            assertEquals("@name=\"" + value + "\"", rewrite(template, "@title=\"" + value + "\""));
        }
        assertFalse(template.find.matcher("@title=\"unescaped\"quote\"").matches());
    }

    @Test
    public void numbers() {
        QueryTemplate template = new QueryTemplate("@price >= #12", "@cost >= #12");
        for (String value : new String[] {"0", "100", "-30.3", "+5", ".5", "-.5", "1.50", "1.5E3", "2e-10"}) {
            assertEquals("@cost >= " + value, rewrite(template, "@price >= " + value));
        }
        for (String value : new String[] {"", ".", "1.", "NaN", "1e", "1e+", "1foo", "1 + 2"}) {
            assertFalse(value, template.find.matcher("@price >= " + value).matches());
        }
        assertFalse(new QueryTemplate("x#1", "#1").find.matcher("x12").matches());
        assertFalse(new QueryTemplate("#1x", "#1").find.matcher("12x").matches());
        assertFalse(new QueryTemplate("#1.0", "#1").find.matcher("12.0").matches());
    }

    @Test
    public void pathsAndMultipleCaptures() {
        QueryTemplate template = new QueryTemplate(
                "/jcr:root/#1//element(*, cq:Page)[jcr:contains(., '#2') and @price >= #3]",
                "/jcr:root/#1//element(*, cq:Page)[jcr:contains(., '#2') and @price >= #3] option(index tag pages)");
        for (String path : new String[] {"content", "content/site/en", "content/jcr:content", "content/_x0020_",
                "content/café"}) {
            String query = "/jcr:root/" + path
                    + "//element(*, cq:Page)[jcr:contains(., 'John''s car') and @price >= -30.3]";
            assertEquals(query + " option(index tag pages)", rewrite(template, query));
        }
        QueryTemplate subtree = new QueryTemplate("/jcr:root/content/#1//*[@x]", "/jcr:root/archive/#1//*[@x]");
        assertEquals("/jcr:root/archive/site/en//*[@x]", rewrite(subtree, "/jcr:root/content/site/en//*[@x]"));
        QueryTemplate suffix = new QueryTemplate("/jcr:root/#1/fixed/*", "/jcr:root/#1/other/*");
        assertEquals("/jcr:root/content/site/other/*", rewrite(suffix, "/jcr:root/content/site/fixed/*"));
        assertEquals("/jcr:root/content", rewrite(
                new QueryTemplate("/jcr:root/#1", "/jcr:root/#1"), "/jcr:root/content"));
        assertEquals("/jcr:root/content[@x]", rewrite(
                new QueryTemplate("/jcr:root/#1[@x]", "/jcr:root/#1[@x]"), "/jcr:root/content[@x]"));
        for (String path : new String[] {"", "content//site", "content/*", "content[@x]", "(content|libs)",
                "content/..", "content/.", "content/"}) {
            assertFalse(path, subtree.find.matcher("/jcr:root/content/" + path + "//*[@x]").matches());
        }
    }

    @Test
    public void sqlPathsDatesAndBindVariables() {
        String find = "select * from [nt:base] as a where isdescendantnode(a, '#1') "
                + "and [created] >= cast('#2' as date) and [id] = $uuid";
        String query = find.replace("#1", "/content/site/en").replace("#2", "2020-12-01T20:00:00.000");
        assertEquals(query + " option(index tag pages)",
                rewrite(new QueryTemplate(find, find + " option(index tag pages)"), query));
        assertEquals("xs:dateTime('2020-12-01T20:00:00.000')", rewrite(
                new QueryTemplate("xs:dateTime('#1')", "xs:dateTime('#1')"),
                "xs:dateTime('2020-12-01T20:00:00.000')"));
    }

    @Test
    public void repeatedCapturesRequireIdenticalText() {
        QueryTemplate template = new QueryTemplate("@x='#2' or @y='#2'", "@z='#2'");
        assertEquals("@z='John''s'", rewrite(template, "@x='John''s' or @y='John''s'"));
        assertFalse(template.find.matcher("@x='a' or @y='b'").matches());
        QueryTemplate number = new QueryTemplate("@x=#1 or @y=#1", "@z=#1");
        assertEquals("@z=1.50", rewrite(number, "@x=1.50 or @y=1.50"));
        assertFalse(number.find.matcher("@x=1.50 or @y=1.5").matches());
        assertFalse(new QueryTemplate("#1 #1x", "#1").find.matcher("12 12x").matches());
        QueryTemplate path = new QueryTemplate("/#1//#1", "/#1");
        assertEquals("/content/site", rewrite(path, "/content/site//content/site"));
        assertFalse(path.find.matcher("/content/site//content/other").matches());
    }

    @Test
    public void anchorsPrefixesAndExactWhitespace() {
        QueryTemplate template = new QueryTemplate("a b", "c");
        for (String prefix : new String[] {"", "  ", "\n", "explain ", "measure ", "ExPlAiN \tMeAsUrE\n"}) {
            assertEquals(prefix + "c", rewrite(template, prefix + "a b"));
        }
        for (String query : new String[] {"a  b", "a b ", "a b\n", "a b a b", "measure explain a b"}) {
            assertFalse(query, template.find.matcher(query).matches());
        }
        assertEquals("b", rewrite(new QueryTemplate("a", "b"), "a"));
        assertFalse(new QueryTemplate("a", "b").find.matcher("aa").find());
    }

    @Test
    public void literalHashesQuotesAndRegexMetacharacters() {
        String text = "[a].* (b) $uuid \\E 'John''s' \"say \"\"hi\"\"\" #tag #";
        assertEquals(text, rewrite(new QueryTemplate(text, text), text));
        assertEquals("'#1' #2 #", rewrite(new QueryTemplate("'##1' ##2 ##", "'##1' ##2 ##"), "'#1' #2 #"));
        assertEquals("'#1' 'value'", rewrite(new QueryTemplate("'##1' '#1'", "'##1' '#1'"), "'#1' 'value'"));
        assertEquals("", rewrite(new QueryTemplate("", ""), ""));
    }

    @Test
    public void invalidCapturesAreRejected() {
        for (String find : new String[] {"#0", "#01", "'prefix #1'", "'#1 suffix'", "'#1", "'#1''suffix'",
                "'#1' #1", "/#1// #1", "\"#1\" '#1'"}) {
            assertThrows(find, IllegalArgumentException.class, () -> new QueryTemplate(find, ""));
        }
        for (String replacement : new String[] {"#1", "'#2'", "#0", "#01", "\"#1\"", "'prefix #1'"}) {
            assertThrows(replacement, IllegalArgumentException.class,
                    () -> new QueryTemplate("'#1'", replacement));
        }
    }

    @Test
    public void generatedPatternsUseRegexBudget() {
        QueryTemplate template = new QueryTemplate("contains('#1')", "'#1'");
        CharSequence input = new BudgetedCharSequence("contains('" + "a".repeat(100) + "')", new RegexBudget(20));
        assertThrows(RegexBudgetExceededException.class, () -> template.find.matcher(input).find());
    }

    private static String rewrite(QueryTemplate template, String query) {
        Matcher matcher = template.find.matcher(query);
        assertTrue(query, matcher.matches());
        return matcher.replaceFirst(template.replace);
    }
}
