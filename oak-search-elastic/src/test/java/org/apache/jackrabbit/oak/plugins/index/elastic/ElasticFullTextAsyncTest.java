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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticFullTextAsyncTest extends ElasticAbstractQueryTest {

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    @Test
    public void testFullTextQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa").analyzed();

        String indexId = UUID.randomUUID().toString();
        setIndex(indexId, builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "Simple test");
        test.addChild("c").setProperty("propa", "Hello everyone. This is an elastic test");
        test.addChild("d").setProperty("propa", "howdy! hello again");
        root.commit();

        String query = "//*[jcr:contains(@propa, 'Hello')] ";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString("elasticsearch:" + indexId));
            assertQuery(query, XPATH, Arrays.asList("/test/a", "/test/c", "/test/d"));
        });
    }

    @Test
    public void testNodeScopeIndexedQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a", "b").async("async");
        builder.indexRule("nt:base").property("a").analyzed().nodeScopeIndex();
        builder.indexRule("nt:base").property("b").analyzed().nodeScopeIndex();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("a", "hello");
        test.addChild("b").setProperty("a", "world");
        test.addChild("c").setProperty("a", "hello world");
        Tree d = test.addChild("d");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(., 'Hello')] ", XPATH, Arrays.asList("/test/a", "/test/c", "/test/d"));
            assertQuery("//*[jcr:contains(., 'hello world')] ", XPATH, Arrays.asList("/test/c", "/test/d"));
        });
    }

    @Test
    public void testFullTextMultiTermQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("analyzed_field");
        builder.async("async");
        builder.indexRule("nt:base").property("analyzed_field").analyzed();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "test123");
        test.addChild("b").setProperty("analyzed_field", "test456");
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(@analyzed_field, 'test123')] ", XPATH, Collections.singletonList("/test/a"))
        );
    }

    @Test
    public void testDefaultAnalyzer() throws Exception {
        IndexDefinitionBuilder builder = createIndex("analyzed_field");
        builder.async("async");
        builder.indexRule("nt:base")
                .property("analyzed_field")
                .analyzed().nodeScopeIndex();

        setIndex(UUID.randomUUID().toString(), builder);
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

}
