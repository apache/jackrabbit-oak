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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public class MongotIndexQueryCommonTest extends IndexQueryCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotIndexQueryCommonTest() {
        indexOptions = new MongotIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new MongotCommonTestRepositoryBuilder(mongo).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @After
    public void resetMongoDatabase() {
        mongo.useFreshDatabase();
    }

    @Override
    protected void assertEventually(Runnable assertion) {
        TestUtil.assertEventually(assertion, 30_000);
    }

    @Override
    @Test
    public void sql2FullText() throws Exception {
        root.getTree("/").addChild("test").setProperty("name", "hello world");
        root.commit();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [nt:base] where contains(name, 'hello')",
                    List.of("/test"));
            assertQuery("select [jcr:path] from [nt:base] where contains(*, 'hello')",
                    List.of("/test"));
        });

        root.getTree("/test").remove();
        root.commit();
    }

    @Override
    @Test
    public void containsDash() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("name", "hello-wor");
        test.addChild("b").setProperty("name", "hello-world");
        test.addChild("c").setProperty("name", "hello");
        root.commit();

        assertEventually(() -> {
            assertQuery("/jcr:root//*[jcr:contains(., 'hello-wor*')]", "xpath",
                    List.of("/test/a", "/test/b"));
            assertQuery("/jcr:root//*[jcr:contains(., '*hello-wor*')]", "xpath",
                    List.of("/test/a", "/test/b"));
        });
    }

    @Override
    @Test
    public void multiPhraseQuery() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("dc:format", "type:application/pdf");
        test.addChild("b").setProperty("dc:format", "progress");
        root.commit();

        assertEventually(() -> {
            assertQuery("/jcr:root//*[jcr:contains(@dc:format, 'pro*')]", "xpath",
                    List.of("/test/b"));
            assertQuery("/jcr:root//*[jcr:contains(@dc:format, 'type:appli*')]", "xpath",
                    List.of("/test/a"));
        });
    }

    @Override
    @Test
    public void testNativeLuceneQuery() throws Exception {
        super.testNativeLuceneQuery();
    }

    @Override
    @Test
    public void repSimilarAsNativeQuery() throws Exception {
        super.repSimilarAsNativeQuery();
    }

    @Override
    @Test
    public void repSimilarQuery() throws Exception {
        super.repSimilarQuery();
    }

    @Override
    @Test
    public void repSimilarXPathQuery() throws Exception {
        super.repSimilarXPathQuery();
    }

    @Test
    public void combinesFullTextAndSimilarity() throws Exception {
        Tree test = root.getTree("/").addChild("hybrid");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "Bye Bye");
        root.commit();

        String query = "select [jcr:path] from [nt:base] where contains(*, 'Hello') "
                + "and similar(., '/hybrid/a')";
        assertEventually(() -> assertQuery(query,
                List.of("/hybrid/a", "/hybrid/b", "/hybrid/d")));
    }

    @Override
    public String getContainsValueForEqualityQuery_native() {
        return typed("propa") + "=bar";
    }

    @Override
    public String getContainsValueForInequalityQuery_native() {
        return "$ne=bar";
    }

    @Override
    public String getContainsValueForInequalityQueryWithoutAncestorFilter_native() {
        return "$ne=bar";
    }

    @Override
    public String getContainsValueForEqualityInequalityCombined_native() {
        return typed("propb") + "=world";
    }

    @Override
    public String getContainsValueForNotNullQuery_native() {
        return typed("propa") + "=Document{{$exists=true}}";
    }

    @Override
    public String getExplainValueForDescendantTestWithIndexTagExplain() {
        return MongoFieldNames.ANCESTORS + "=/test";
    }

    private static String typed(String propertyName) {
        return MongoFieldNames.TYPED + "." + MongoFieldNames.encodeProperty(propertyName);
    }
}
