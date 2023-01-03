/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public abstract class FullTextIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void defaultAnalyzer() throws Exception {
        Tree test = setup();

        test.addChild("a").setProperty("analyzed_field", "sun.jpg");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, 'SUN.JPG')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'Sun')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'jpg')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(., 'SUN.jpg')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(., 'sun')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(., 'jpg')] ", XPATH, Collections.singletonList("/test/a"));
        });
    }

    @Test
    public void defaultAnalyzerHonourSplitOptions() throws Exception {
        Tree test = setup();

        test.addChild("a").setProperty("analyzed_field", "1234abCd5678");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, '1234')] ", XPATH, Collections.emptyList());
            assertQuery("//*[jcr:contains(@analyzed_field, 'abcd')] ", XPATH, Collections.emptyList());
            assertQuery("//*[jcr:contains(@analyzed_field, '5678')] ", XPATH, Collections.emptyList());
            assertQuery("//*[jcr:contains(@analyzed_field, '1234abCd5678')] ", XPATH, Collections.singletonList("/test/a"));
        });
    }


    @Test
    public void testWithSpecialCharsInSearchTerm() throws Exception {
        Tree test = setup();
        test.addChild("a").setProperty("analyzed_field", "foo");
        root.commit();

        assertEventually(() -> {
            // Special characters {':' , '/', '!', '&', '|', '='} are escaped before creating lucene/elastic queries using
            // {@see org.apache.jackrabbit.oak.plugins.index.search.spi.query.FullTextIndex#rewriteQueryText}
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo:')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '|foo/')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '&=!foo')] ", XPATH, Collections.singletonList("/test/a"));

            // Braces are not escaped in the above rewriteQueryText method - we do not change that to maintain backward compatibility
            // So these need explicit escaping or filtering on client side while creating the jcr query
            assertQuery("//*[jcr:contains(@analyzed_field, '\\{foo\\}')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '\\[foo\\]')] ", XPATH, Collections.singletonList("/test/a"));
        });

    }

    @Test()
    public void testFullTextTermWithUnescapedBraces() throws Exception {
        LogCustomizer customLogs = setupLogCustomizer();
        Tree test = setup();

        test.addChild("a").setProperty("analyzed_field", "foo");
        root.commit();

        // Below queries would fail silently (return 0 results with an entry in logs for the query that failed)
        // due to unescaped special character (which is not handled in backend)
        try {
            customLogs.starting();
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo}')] ", XPATH, Collections.emptyList());
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo]')] ", XPATH, Collections.emptyList());

            Assert.assertTrue(customLogs.getLogs().containsAll(getExpectedLogMessage()));
        } finally {
            customLogs.finished();
        }
    }

    @Test
    public void pathTransformationsWithNoPathRestrictions() throws Exception {
        Tree test = setup();
        test.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("b").setProperty("analyzed_field", "bar");
        test.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");

        root.commit();

        assertEventually(() -> {
            assertQuery("//*[j:c/@analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a", "/test/c/d"));
            assertQuery("//*[d/*/@analyzed_field = 'bar']", XPATH, Collections.singletonList("/test/c"));
        });
    }

    @Test
    public void pathTransformationsWithPathRestrictions() throws Exception {
        Tree test = setup();

        test.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("b").setProperty("analyzed_field", "bar");
        test.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");
        test.addChild("f").addChild("d").addChild("temp:c").setProperty("analyzed_field", "bar");
        test.addChild("g").addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");


        Tree temp = root.getTree("/").addChild("tmp");

        temp.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("a").setProperty("abc", "foo");
        temp.addChild("b").setProperty("analyzed_field", "bar");
        temp.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("c").getChild("d").setProperty("abc", "foo");
        root.commit();

        assertEventually(() -> {
            // ALL CHILDREN
            assertQuery("/jcr:root/test//*[j:c/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a", "/test/c/d"));
            assertQuery("/jcr:root/test//*[*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a", "/test/c/d", "/test/e", "/test/f/d", "/test/g/e"));
            assertQuery("/jcr:root/test//*[d/*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/c", "/test/f"));
            assertQuery("/jcr:root/test//*[analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a/j:c","/test/b","/test/c/d/j:c",
                    "/test/e/temp:c", "/test/f/d/temp:c","/test/g/e/temp:c"));

            // DIRECT CHILDREN
            assertQuery("/jcr:root/test/*[j:c/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a"));
            assertQuery("/jcr:root/test/*[*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a", "/test/e"));
            assertQuery("/jcr:root/test/*[d/*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/c", "/test/f"));
            assertQuery("/jcr:root/test/*[analyzed_field = 'bar']", XPATH, Arrays.asList("/test/b"));

            // EXACT
            assertQuery("/jcr:root/test/a[j:c/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a"));
            assertQuery("/jcr:root/test/a[*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a"));
            assertQuery("/jcr:root/test/c[d/*/analyzed_field = 'bar']", XPATH, Arrays.asList("/test/c"));
            assertQuery("/jcr:root/test/a/j:c[analyzed_field = 'bar']", XPATH, Arrays.asList("/test/a/j:c"));

            // PARENT

            assertQuery("select a.[jcr:path] as [jcr:path] from [nt:base] as a \n" +
                    "  inner join [nt:base] as b on ischildnode(b, a)\n" +
                    "  where isdescendantnode(a, '/tmp') \n" +
                    "  and b.[analyzed_field] = 'bar'\n" +
                    "  and a.[abc] is not null ", SQL2, Arrays.asList("/tmp/a", "/tmp/c/d"));
        });
    }


    protected Tree setup() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, "analyzed_field");
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed().nodeScopeIndex();
        builder.evaluatePathRestrictions();

        indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        return test;
    }

    // TODO : Below two method are only used for testFullTextTermWithUnescapedBraces
    // TODO : If needed in future, we can possibly use test metadata to change the
    // TODO : returned values from these based on which test is being executed
    protected abstract LogCustomizer setupLogCustomizer();

    protected abstract List<String> getExpectedLogMessage();

}
