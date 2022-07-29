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
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public abstract class FullTextIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void defaultAnalyzer() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, "analyzed_field");
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed().nodeScopeIndex();

        indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

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
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, "analyzed_field");
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed().nodeScopeIndex();

        indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

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
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, "analyzed_field");
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed().nodeScopeIndex();

        indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("analyzed_field", "foo");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, '{foo}')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '\\{foo}')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo:')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '[foo]')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '|foo/')] ", XPATH, Collections.singletonList("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '(&=!foo')] ", XPATH, Collections.singletonList("/test/a"));
        });

    }

}
