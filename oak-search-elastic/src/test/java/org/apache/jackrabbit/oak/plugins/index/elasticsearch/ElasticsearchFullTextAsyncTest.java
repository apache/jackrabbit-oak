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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;


import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticsearchFullTextAsyncTest extends ElasticsearchAbstractQueryTest {

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    @Test
    public void testFullTextQuery() throws Exception {
        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa").analyzed();

        setIndex("test1", builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("c").setProperty("propa", "Hello everyone. This is an elastic test");
        test.addChild("b").setProperty("propa", "Simple test");
        root.commit();
		// Wait for DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS
		// This is needed in addition to assertEventually to make the 
		// test reliable, otherwise they seem to fail sometimes even
		// with assertEventually wait in place, due to minor delay in async 
		// cycle exec.
        Thread.sleep(DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS * 1000);
		
        String query = "//*[jcr:contains(@propa, 'Hello')] ";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString("elasticsearch:test1"));
            assertQuery(query, XPATH, Arrays.asList("/test/a", "/test/c"));
        });

    }

}
